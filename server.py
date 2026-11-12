#!/usr/bin/env python3
"""Solana Priority Fee API: an HTTP service backed by NoLimitNodes RPC and streams.

Examples:
    # one-shot estimate from NLN RPC, printed as JSON
    python server.py --once --accounts 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P

    # run the API with the live landed-transaction window
    python server.py --port 8080
    curl 'localhost:8080/v1/fees?cu_limit=150000'
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from fee_estimator import (DEFAULT_CU_LIMIT, LANDED_TOPIC, MAX_CU_LIMIT, MAX_RPC_ACCOUNTS,
                           FeeEstimator, LandedFeeWindow, NlnRpc, RpcError, is_pubkey)

log = logging.getLogger("fee_api")


class BadRequest(ValueError):
    pass


def parse_accounts(raw: str):
    accounts = [a.strip() for a in raw.split(",") if a.strip()]
    if len(accounts) > MAX_RPC_ACCOUNTS:
        raise BadRequest(f"at most {MAX_RPC_ACCOUNTS} accounts")
    bad = [a for a in accounts if not is_pubkey(a)]
    if bad:
        raise BadRequest(f"not a base58 public key: {bad[0][:60]}")
    return accounts


def parse_int(qs, name, default, lo, hi):
    raw = qs.get(name, [None])[0]
    if raw is None:
        return default
    try:
        val = int(raw)
    except ValueError:
        raise BadRequest(f"{name} must be an integer")
    if not lo <= val <= hi:
        raise BadRequest(f"{name} must be between {lo} and {hi}")
    return val


def make_handler(estimator: FeeEstimator, window: LandedFeeWindow, state: dict,
                 default_accounts, cors: bool):
    class Handler(BaseHTTPRequestHandler):
        server_version = "solana-priority-fee-api/1.0"

        def _send(self, code: int, payload: dict):
            body = json.dumps(payload, indent=2).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            if cors:
                self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            url = urlparse(self.path)
            qs = parse_qs(url.query)
            try:
                if url.path in ("/", ""):
                    return self._send(200, {
                        "service": "Solana Priority Fee API (NoLimitNodes)",
                        "endpoints": {
                            "/v1/fees": "query: cu_limit, signatures, accounts (comma-separated), source=auto|rpc|landed",
                            "/health": "stream and RPC status",
                        },
                    })
                if url.path == "/health":
                    age = (time.monotonic() - window.last_event_at) if window.last_event_at else None
                    return self._send(200, {
                        "ok": True,
                        "stream_enabled": state["stream_enabled"],
                        "stream_error": state.get("stream_error"),
                        "landed_events_total": window.total,
                        "last_landed_event_age_s": round(age, 2) if age is not None else None,
                        "last_landed_slot": window.last_slot or None,
                    })
                if url.path == "/v1/fees":
                    cu_limit = parse_int(qs, "cu_limit", DEFAULT_CU_LIMIT, 1, MAX_CU_LIMIT)
                    signatures = parse_int(qs, "signatures", 1, 1, 64)
                    accounts = parse_accounts(qs.get("accounts", [""])[0]) if "accounts" in qs \
                        else default_accounts
                    source = qs.get("source", ["auto"])[0]
                    if source not in ("auto", "rpc", "landed"):
                        raise BadRequest("source must be auto, rpc or landed")
                    result = estimator.estimate(cu_limit, signatures, accounts, source)
                    if result["recommended"] is None:
                        return self._send(503, {"error": "no fee data yet", **result})
                    return self._send(200, result)
                return self._send(404, {"error": "not found"})
            except BadRequest as exc:
                return self._send(400, {"error": str(exc)})
            except (RpcError, OSError) as exc:
                log.warning("RPC failure: %s", exc)
                return self._send(502, {"error": f"upstream RPC error: {exc}"})

        def log_message(self, fmt, *args):
            log.debug("%s %s", self.address_string(), fmt % args)

    return Handler


def start_stream(window: LandedFeeWindow, state: dict, stop: threading.Event):
    from nln_stream import StreamAuthError, subscribe

    def run():
        try:
            for event in subscribe(LANDED_TOPIC, os.environ["NLN_API_KEY"], stop=stop):
                window.add(event)
        except StreamAuthError as exc:
            state["stream_error"] = str(exc)
            log.error("NLN stream rejected the API key; continuing in RPC-only mode: %s", exc)
        except Exception as exc:  # keep the API alive on unexpected stream failures
            state["stream_error"] = repr(exc)
            log.exception("stream thread stopped")

    threading.Thread(target=run, daemon=True, name="nln-landed-stream").start()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Solana priority fee API on NoLimitNodes RPC + streams.")
    p.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)))
    p.add_argument("--accounts", default=os.environ.get("FEE_ACCOUNTS", ""),
                   help="default comma-separated writable accounts for getRecentPrioritizationFees")
    p.add_argument("--window", type=float, default=60, help="landed-tx window in seconds")
    p.add_argument("--min-landed", type=int, default=20,
                   help="landed txs needed before they are preferred over RPC data")
    p.add_argument("--max-cu-price", type=int, default=None,
                   help="cap recommendations at this many micro-lamports per CU")
    p.add_argument("--cu-limit", type=int, default=DEFAULT_CU_LIMIT, help="CU limit for --once")
    p.add_argument("--no-stream", action="store_true", help="RPC only; do not open the NLN stream")
    p.add_argument("--cors", action="store_true", help="send Access-Control-Allow-Origin: *")
    p.add_argument("--once", action="store_true", help="print one RPC-based estimate and exit")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stderr)

    try:
        default_accounts = parse_accounts(args.accounts)
    except BadRequest as exc:
        sys.exit(f"--accounts: {exc}")

    rpc = NlnRpc()
    window = LandedFeeWindow(window_s=args.window)
    estimator = FeeEstimator(rpc, window, min_landed=args.min_landed, max_cu_price=args.max_cu_price)

    if args.once:
        print(json.dumps(estimator.estimate(args.cu_limit, 1, default_accounts, "rpc"), indent=2))
        return 0

    stop = threading.Event()
    state = {"stream_enabled": False}
    if args.no_stream:
        log.info("stream disabled; serving RPC-based estimates only")
    elif not os.environ.get("NLN_API_KEY"):
        log.warning("NLN_API_KEY not set; serving RPC-based estimates only")
    else:
        state["stream_enabled"] = True
        start_stream(window, state, stop)

    server = ThreadingHTTPServer((args.host, args.port),
                                 make_handler(estimator, window, state, default_accounts, args.cors))
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown).start())
    log.info("listening on http://%s:%d/v1/fees", args.host, server.server_port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
