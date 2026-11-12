"""Solana priority fee estimation from two NoLimitNodes sources.

1. NLN Solana RPC ``getRecentPrioritizationFees``: per-slot fees for the last
   (up to) 150 slots, optionally scoped to the writable accounts you will lock.
2. NLN decoded stream ``prod.rpc.solana.pumpfun.transaction``: compute-unit
   prices and tips of PumpFun transactions that actually landed, so you see
   what competitive traders are paying right now.

All fee math is in lamports and micro-lamports per compute unit (CU).
"""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
import urllib.request
from collections import OrderedDict, deque
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple

COMPUTE_BUDGET_PROGRAM_ID = "ComputeBudget111111111111111111111111111111"
BASE_FEE_LAMPORTS_PER_SIGNATURE = 5_000
DEFAULT_CU_LIMIT = 200_000
MAX_CU_LIMIT = 1_400_000
MICRO_LAMPORTS_PER_LAMPORT = 1_000_000
LAMPORTS_PER_SOL = 1_000_000_000
MAX_RPC_ACCOUNTS = 128  # getRecentPrioritizationFees accepts at most 128 addresses
LANDED_TOPIC = "prod.rpc.solana.pumpfun.transaction"

_BASE58 = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def is_pubkey(value: str) -> bool:
    return bool(_BASE58.match(value or ""))


def _int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# -- statistics ----------------------------------------------------------------
def percentile(sorted_values: Sequence[int], pct: float) -> int:
    """Nearest-rank percentile of an already sorted, non-empty sequence."""
    if not sorted_values:
        raise ValueError("percentile of empty sequence")
    rank = max(1, math.ceil(pct / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def summarize(values: Iterable[int]) -> Optional[Dict[str, int]]:
    vals = sorted(values)
    if not vals:
        return None
    out = {"count": len(vals), "min": vals[0], "max": vals[-1]}
    for p in (25, 50, 75, 90, 99):
        out[f"p{p}"] = percentile(vals, p)
    return out


# -- fee math --------------------------------------------------------------------
def priority_fee_lamports(cu_price_micro_lamports: int, cu_limit: int) -> int:
    """Priority fee = CU limit x CU price, rounded up from micro-lamports to lamports."""
    return math.ceil(cu_price_micro_lamports * cu_limit / MICRO_LAMPORTS_PER_LAMPORT)


def set_compute_unit_limit_data(units: int) -> bytes:
    """Instruction data for ComputeBudget SetComputeUnitLimit (tag 2, u32 LE)."""
    return bytes([2]) + int(units).to_bytes(4, "little")


def set_compute_unit_price_data(micro_lamports: int) -> bytes:
    """Instruction data for ComputeBudget SetComputeUnitPrice (tag 3, u64 LE)."""
    return bytes([3]) + int(micro_lamports).to_bytes(8, "little")


def fee_quote(cu_price: int, cu_limit: int, signatures: int) -> dict:
    priority = priority_fee_lamports(cu_price, cu_limit)
    total = priority + BASE_FEE_LAMPORTS_PER_SIGNATURE * signatures
    return {
        "cu_price_micro_lamports": cu_price,
        "priority_fee_lamports": priority,
        "total_fee_lamports": total,
        "total_fee_sol": round(total / LAMPORTS_PER_SOL, 9),
    }


# -- NLN RPC -------------------------------------------------------------------------
class RpcError(RuntimeError):
    pass


class NlnRpc:
    """Tiny JSON-RPC client for an NLN Solana RPC endpoint (``NLN_RPC_URL``)."""

    def __init__(self, url: Optional[str] = None, timeout: float = 10.0):
        self.url = url or os.environ.get("NLN_RPC_URL", "")
        if not self.url:
            raise SystemExit("NLN_RPC_URL is not set. Copy your RPC URL from https://app.nolimitnodes.com")
        self.timeout = timeout

    def call(self, method: str, params: Optional[list] = None):
        body = {"jsonrpc": "2.0", "id": 1, "method": method}
        if params is not None:
            body["params"] = params
        req = urllib.request.Request(self.url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "solana-priority-fee-api/1.0"})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            data = json.loads(resp.read())
        if "error" in data:
            raise RpcError(f"{method}: {data['error']}")
        return data["result"]

    def slot(self) -> int:
        return int(self.call("getSlot", [{"commitment": "confirmed"}]))

    def recent_prioritization_fees(self, accounts: Sequence[str] = ()) -> List[Tuple[int, int]]:
        """Return [(slot, micro_lamports_per_cu), ...] for recent slots."""
        result = self.call("getRecentPrioritizationFees", [list(accounts)] if accounts else [])
        return [(int(r["slot"]), int(r["prioritizationFee"])) for r in result]


# -- landed-transaction window --------------------------------------------------------
class LandedFeeWindow:
    """Thread-safe rolling window of fees paid by landed PumpFun transactions."""

    def __init__(self, window_s: float = 60.0, max_events: int = 200_000):
        self.window_s = window_s
        self._events: Deque[Tuple[float, int, int]] = deque(maxlen=max_events)
        self._seen: "OrderedDict[str, None]" = OrderedDict()
        self._lock = threading.Lock()
        self.total = 0
        self.last_event_at: Optional[float] = None
        self.last_slot = 0

    def add(self, event: dict, now: Optional[float] = None) -> bool:
        """Record one ``PumpfunTransaction`` event. Returns False for duplicates."""
        now = time.monotonic() if now is None else now
        sig = event.get("signature") or ""
        with self._lock:
            if sig:
                if sig in self._seen:
                    return False
                self._seen[sig] = None
                if len(self._seen) > 50_000:
                    self._seen.popitem(last=False)
            cu_price = _int(event.get("compute_unit_price"))
            tip = _int(event.get("tip_amount"))
            self._events.append((now, cu_price, tip))
            self.total += 1
            self.last_event_at = now
            self.last_slot = max(self.last_slot, _int(event.get("slot")))
        return True

    def snapshot(self, now: Optional[float] = None) -> dict:
        now = time.monotonic() if now is None else now
        cutoff = now - self.window_s
        with self._lock:
            while self._events and self._events[0][0] < cutoff:
                self._events.popleft()
            prices = [p for _, p, _ in self._events]
            tips = [t for _, _, t in self._events if t > 0]
        n = len(prices)
        return {
            "topic": LANDED_TOPIC,
            "window_s": self.window_s,
            "transactions": n,
            "micro_lamports_per_cu": summarize(prices),
            "share_paying_priority_fee": round(sum(1 for p in prices if p > 0) / n, 3) if n else None,
            "tip_lamports_nonzero": summarize(tips),
            "share_paying_tip": round(len(tips) / n, 3) if n else None,
        }


# -- estimator ------------------------------------------------------------------------
class FeeEstimator:
    """Combines RPC and landed-transaction data into fee recommendations."""

    TIERS = (("low", "p50"), ("medium", "p75"), ("high", "p90"))

    def __init__(self, rpc: Optional[NlnRpc], window: Optional[LandedFeeWindow],
                 min_landed: int = 20, rpc_cache_s: float = 2.0,
                 max_cu_price: Optional[int] = None):
        self.rpc = rpc
        self.window = window
        self.min_landed = min_landed
        self.rpc_cache_s = rpc_cache_s
        self.max_cu_price = max_cu_price
        self._cache: Dict[Tuple[str, ...], Tuple[float, dict]] = {}
        self._cache_lock = threading.Lock()

    def rpc_summary(self, accounts: Sequence[str] = ()) -> Optional[dict]:
        if self.rpc is None:
            return None
        key = tuple(sorted(accounts))
        now = time.monotonic()
        with self._cache_lock:
            hit = self._cache.get(key)
            if hit and now - hit[0] < self.rpc_cache_s:
                return hit[1]
        rows = self.rpc.recent_prioritization_fees(accounts)
        fees = [fee for _, fee in rows]
        summary = {
            "method": "getRecentPrioritizationFees",
            "accounts": list(accounts),
            "slots": len(rows),
            "latest_slot": max((s for s, _ in rows), default=None),
            "micro_lamports_per_cu": summarize(fees),
            "share_of_slots_with_fee": round(sum(1 for f in fees if f > 0) / len(fees), 3) if fees else None,
        }
        with self._cache_lock:
            self._cache[key] = (now, summary)
            if len(self._cache) > 256:
                self._cache.pop(next(iter(self._cache)))
        return summary

    def estimate(self, cu_limit: int = DEFAULT_CU_LIMIT, signatures: int = 1,
                 accounts: Sequence[str] = (), source: str = "auto") -> dict:
        rpc = self.rpc_summary(accounts) if source in ("auto", "rpc") else None
        landed = self.window.snapshot() if self.window and source in ("auto", "landed") else None

        chosen, stats = None, None
        if landed and landed["transactions"] >= self.min_landed and source in ("auto", "landed"):
            chosen, stats = "landed_pumpfun_transactions", landed["micro_lamports_per_cu"]
        elif rpc and rpc["micro_lamports_per_cu"]:
            chosen, stats = "rpc_recent_prioritization_fees", rpc["micro_lamports_per_cu"]

        tiers = None
        if stats:
            tiers = {}
            for name, key in self.TIERS:
                price = stats[key]
                if self.max_cu_price is not None:
                    price = min(price, self.max_cu_price)
                tiers[name] = fee_quote(price, cu_limit, signatures)

        tips = None
        if landed and landed["tip_lamports_nonzero"]:
            t = landed["tip_lamports_nonzero"]
            tips = {"low": t["p50"], "medium": t["p75"], "high": t["p90"],
                    "based_on": t["count"], "share_of_txs_tipping": landed["share_paying_tip"]}

        medium_price = tiers["medium"]["cu_price_micro_lamports"] if tiers else 0
        return {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "cu_limit": cu_limit,
            "signatures": signatures,
            "recommended": {"source": chosen, **(tiers or {})} if tiers else None,
            "tip_lamports": tips,
            "compute_budget_instructions": {
                "program_id": COMPUTE_BUDGET_PROGRAM_ID,
                "set_compute_unit_limit_data_hex": set_compute_unit_limit_data(cu_limit).hex(),
                "set_compute_unit_price_data_hex": set_compute_unit_price_data(medium_price).hex(),
                "note": "Price instruction uses the 'medium' tier.",
            },
            "sources": {"rpc": rpc, "landed": landed},
        }
