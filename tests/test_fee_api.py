"""Offline tests for the Solana Priority Fee API.

Run with:  python -m unittest discover -s tests -v
No NLN credentials are needed: RPC is faked and the stream test uses a local gRPC server.
Set LIVE_RPC_URL to also run one real getRecentPrioritizationFees call.
"""

import json
import os
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
from concurrent import futures
from http.server import ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import fee_estimator as fe  # noqa: E402
import server  # noqa: E402


class FakeRpc:
    def __init__(self, fees):
        self.fees = fees
        self.calls = []

    def recent_prioritization_fees(self, accounts=()):
        self.calls.append(tuple(accounts))
        return [(1000 + i, f) for i, f in enumerate(self.fees)]


def landed(sig, price, tip=0, slot=1):
    return {"signature": sig, "slot": str(slot), "compute_unit_price": str(price),
            "compute_unit_limit": "200000", "tip_amount": str(tip)}


class MathTests(unittest.TestCase):
    def test_percentile_nearest_rank(self):
        vals = list(range(1, 101))
        self.assertEqual(fe.percentile(vals, 50), 50)
        self.assertEqual(fe.percentile(vals, 90), 90)
        self.assertEqual(fe.percentile([7], 99), 7)

    def test_priority_fee_rounds_up(self):
        self.assertEqual(fe.priority_fee_lamports(10_000, 200_000), 2_000)
        self.assertEqual(fe.priority_fee_lamports(1, 1), 1)
        self.assertEqual(fe.priority_fee_lamports(0, 1_400_000), 0)

    def test_compute_budget_instruction_bytes(self):
        self.assertEqual(fe.set_compute_unit_limit_data(200_000).hex(), "02400d0300")
        self.assertEqual(fe.set_compute_unit_price_data(1).hex(), "030100000000000000")

    def test_pubkey_validation(self):
        self.assertTrue(fe.is_pubkey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"))
        self.assertFalse(fe.is_pubkey("0OIl-not-base58"))


class WindowTests(unittest.TestCase):
    def test_dedupes_and_expires(self):
        w = fe.LandedFeeWindow(window_s=10)
        self.assertTrue(w.add(landed("a", 100), now=0))
        self.assertFalse(w.add(landed("a", 100), now=1))
        w.add(landed("b", 0, tip=50_000), now=5)
        snap = w.snapshot(now=6)
        self.assertEqual(snap["transactions"], 2)
        self.assertEqual(snap["share_paying_priority_fee"], 0.5)
        self.assertEqual(snap["tip_lamports_nonzero"]["p50"], 50_000)
        self.assertEqual(w.snapshot(now=12)["transactions"], 1)


class EstimatorTests(unittest.TestCase):
    def test_prefers_landed_when_enough_data(self):
        w = fe.LandedFeeWindow(window_s=60)
        for i in range(30):
            w.add(landed(f"s{i}", (i + 1) * 1000, tip=10_000 if i % 2 else 0))
        est = fe.FeeEstimator(FakeRpc([0, 5, 10]), w, min_landed=20).estimate(cu_limit=100_000)
        rec = est["recommended"]
        self.assertEqual(rec["source"], "landed_pumpfun_transactions")
        self.assertEqual(rec["medium"]["cu_price_micro_lamports"], 23_000)  # p75 of 1k..30k
        self.assertEqual(rec["medium"]["priority_fee_lamports"], 2_300)
        self.assertEqual(rec["medium"]["total_fee_lamports"], 7_300)
        self.assertEqual(est["tip_lamports"]["share_of_txs_tipping"], 0.5)

    def test_falls_back_to_rpc_and_caps(self):
        est = fe.FeeEstimator(FakeRpc(list(range(0, 100_000, 1000))), fe.LandedFeeWindow(),
                              max_cu_price=50_000).estimate()
        self.assertEqual(est["recommended"]["source"], "rpc_recent_prioritization_fees")
        self.assertEqual(est["recommended"]["high"]["cu_price_micro_lamports"], 50_000)

    def test_rpc_results_are_cached(self):
        rpc = FakeRpc([1, 2, 3])
        est = fe.FeeEstimator(rpc, None, rpc_cache_s=60)
        est.estimate(accounts=["A"])
        est.estimate(accounts=["A"])
        est.estimate(accounts=["B"])
        self.assertEqual(rpc.calls, [("A",), ("B",)])


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.window = fe.LandedFeeWindow()
        estimator = fe.FeeEstimator(FakeRpc([0, 100, 200, 300]), cls.window)
        handler = server.make_handler(estimator, cls.window, {"stream_enabled": False}, [], cors=True)
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()
        cls.base = f"http://127.0.0.1:{cls.httpd.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()

    def get(self, path):
        try:
            with urllib.request.urlopen(self.base + path) as r:
                return r.status, json.loads(r.read()), r.headers
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read()), e.headers

    def test_fees_endpoint(self):
        code, body, headers = self.get("/v1/fees?cu_limit=300000&signatures=2")
        self.assertEqual(code, 200)
        self.assertEqual(body["recommended"]["high"]["cu_price_micro_lamports"], 300)
        self.assertEqual(body["recommended"]["high"]["total_fee_lamports"], 90 + 10_000)
        self.assertEqual(headers["Access-Control-Allow-Origin"], "*")

    def test_validation_errors(self):
        self.assertEqual(self.get("/v1/fees?cu_limit=9999999")[0], 400)
        self.assertEqual(self.get("/v1/fees?accounts=not-a-key")[0], 400)
        self.assertEqual(self.get("/v1/fees?source=magic")[0], 400)
        self.assertEqual(self.get("/nope")[0], 404)

    def test_landed_only_without_data_is_503(self):
        self.assertEqual(self.get("/v1/fees?source=landed")[0], 503)

    def test_health(self):
        code, body, _ = self.get("/health")
        self.assertEqual(code, 200)
        self.assertTrue(body["ok"])


class StreamTests(unittest.TestCase):
    def test_landed_events_flow_from_grpc_into_window(self):
        import grpc
        import nln_stream

        pb, pb_grpc = nln_stream.load_stubs()

        class FakeNLN(pb_grpc.StreamServiceServicer):
            def Subscribe(self, request, context):
                assert request.topic == fe.LANDED_TOPIC
                for i in range(25):
                    yield pb.SubscribeResponse(topic=request.topic, offset=i,
                                               payload=json.dumps(landed(f"g{i}", i * 10)).encode())
                time.sleep(5)

        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
        pb_grpc.add_StreamServiceServicer_to_server(FakeNLN(), grpc_server)
        port = grpc_server.add_insecure_port("127.0.0.1:0")
        grpc_server.start()
        os.environ["NLN_STREAM_INSECURE"] = "1"
        stop = threading.Event()
        window = fe.LandedFeeWindow()
        try:
            for ev in nln_stream.subscribe(fe.LANDED_TOPIC, "sk_test", f"127.0.0.1:{port}", stop):
                window.add(ev)
                if window.total == 25:
                    stop.set()
        finally:
            os.environ.pop("NLN_STREAM_INSECURE", None)
            grpc_server.stop(0)
        est = fe.FeeEstimator(None, window).estimate()
        self.assertEqual(est["recommended"]["source"], "landed_pumpfun_transactions")
        self.assertEqual(est["recommended"]["low"]["cu_price_micro_lamports"], 120)


@unittest.skipUnless(os.environ.get("LIVE_RPC_URL"), "set LIVE_RPC_URL to run")
class LiveRpcTest(unittest.TestCase):
    def test_real_rpc(self):
        rows = fe.NlnRpc(os.environ["LIVE_RPC_URL"]).recent_prioritization_fees()
        self.assertTrue(rows)


if __name__ == "__main__":
    unittest.main()
