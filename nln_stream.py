"""Minimal NoLimitNodes (NLN) event-stream client.

NLN delivers decoded Solana program events over gRPC/TLS. This module wraps the
``nln.stream.v1.StreamService/Subscribe`` call with:

* runtime proto compilation (no generated files to commit),
* automatic reconnect with exponential backoff,
* fan-in of several topics into one iterator.

Environment variables:
    NLN_API_KEY          API key from https://app.nolimitnodes.com (required)
    NLN_STREAM_ENDPOINT  defaults to stream-1.nln.clr3.org:443
    NLN_STREAM_INSECURE  set to 1 only for a local plaintext test server
"""

from __future__ import annotations

import json
import logging
import os
import queue
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Iterator, Optional, Sequence, Tuple

log = logging.getLogger("nln_stream")

DEFAULT_ENDPOINT = "stream-1.nln.clr3.org:443"

PROTO_SRC = """\
syntax = "proto3";
package nln.stream.v1;

service StreamService {
  rpc Subscribe(SubscribeRequest) returns (stream SubscribeResponse);
}

message SubscribeRequest { string topic = 1; OutputFormat format = 2; }
enum OutputFormat { PROTO = 0; JSON = 1; }

message SubscribeResponse {
  string topic = 1;
  int64 partition = 2;
  int64 offset = 3;
  int64 timestamp_ms = 4;
  bytes payload = 5;
}
"""

_stub_lock = threading.Lock()
_modules: Optional[tuple] = None


def load_stubs():
    """Compile the embedded proto once per process and return (pb2, pb2_grpc)."""
    global _modules
    with _stub_lock:
        if _modules is None:
            from grpc_tools import protoc

            out = Path(tempfile.mkdtemp(prefix="nln_pb_"))
            (out / "stream_service.proto").write_text(PROTO_SRC)
            rc = protoc.main([
                "protoc",
                f"-I{out}",
                f"--python_out={out}",
                f"--grpc_python_out={out}",
                str(out / "stream_service.proto"),
            ])
            if rc != 0:
                raise RuntimeError(f"protoc failed with exit code {rc}")
            sys.path.insert(0, str(out))
            import stream_service_pb2 as pb  # type: ignore
            import stream_service_pb2_grpc as pb_grpc  # type: ignore

            _modules = (pb, pb_grpc)
        return _modules


class StreamAuthError(RuntimeError):
    """Raised when NLN rejects the API key; retrying will not help."""


def _channel(endpoint: str):
    import grpc

    if os.environ.get("NLN_STREAM_INSECURE") == "1":
        return grpc.insecure_channel(endpoint)
    return grpc.secure_channel(endpoint, grpc.ssl_channel_credentials())


def subscribe(
    topic: str,
    api_key: str,
    endpoint: Optional[str] = None,
    stop: Optional[threading.Event] = None,
    max_backoff: float = 30.0,
) -> Iterator[dict]:
    """Yield decoded JSON events for ``topic`` forever, reconnecting on errors."""
    import grpc

    pb, pb_grpc = load_stubs()
    endpoint = endpoint or os.environ.get("NLN_STREAM_ENDPOINT", DEFAULT_ENDPOINT)
    backoff = 1.0
    while not (stop and stop.is_set()):
        try:
            with _channel(endpoint) as channel:
                stub = pb_grpc.StreamServiceStub(channel)
                request = pb.SubscribeRequest(topic=topic, format=pb.JSON)
                log.info("subscribed to %s via %s", topic, endpoint)
                for msg in stub.Subscribe(request, metadata=[("x-api-key", api_key)]):
                    backoff = 1.0
                    try:
                        event = json.loads(msg.payload)
                    except ValueError:
                        log.debug("skipping non-JSON payload on %s", topic)
                        continue
                    yield event
                    if stop and stop.is_set():
                        return
                log.warning("stream %s closed by server; reconnecting", topic)
        except grpc.RpcError as exc:
            code = exc.code()
            if code in (grpc.StatusCode.UNAUTHENTICATED, grpc.StatusCode.PERMISSION_DENIED):
                raise StreamAuthError(f"{topic}: {code.name}: {exc.details()}") from exc
            log.warning("stream %s error %s: %s; retrying in %.0fs",
                        topic, code.name if code else "?", exc.details(), backoff)
        if stop and stop.wait(backoff):
            return
        if not stop:
            time.sleep(backoff)
        backoff = min(backoff * 2, max_backoff)


def merge(
    topics: Sequence[str],
    api_key: str,
    endpoint: Optional[str] = None,
    stop: Optional[threading.Event] = None,
    maxsize: int = 20000,
) -> Iterator[Tuple[str, dict]]:
    """Subscribe to several topics on background threads and yield (topic, event)."""
    q: "queue.Queue[Tuple[str, object]]" = queue.Queue(maxsize=maxsize)

    def pump(topic: str) -> None:
        try:
            for event in subscribe(topic, api_key, endpoint, stop):
                q.put((topic, event))
        except BaseException as exc:  # surfaced to the consumer below
            q.put((topic, exc))

    for topic in topics:
        threading.Thread(target=pump, args=(topic,), daemon=True, name=f"nln:{topic}").start()

    while not (stop and stop.is_set()):
        try:
            topic, item = q.get(timeout=0.5)
        except queue.Empty:
            continue
        if isinstance(item, BaseException):
            raise item
        yield topic, item  # type: ignore[misc]


def require_api_key() -> str:
    key = os.environ.get("NLN_API_KEY", "").strip()
    if not key:
        raise SystemExit(
            "NLN_API_KEY is not set. Create a key at https://app.nolimitnodes.com/register "
            "and run: export NLN_API_KEY=sk_live_..."
        )
    return key
