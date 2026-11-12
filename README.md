# Solana Priority Fee API: Live Compute Unit Price Estimates

A self-hosted **Solana priority fee estimator** that answers "how much priority fee should I pay right now?" It combines two [NoLimitNodes](https://nolimitnodes.com) data sources:

1. **NLN Solana RPC `getRecentPrioritizationFees`** for the last 150 slots, scoped to the writable accounts your transaction will lock.
2. **The NLN decoded stream of landed PumpFun transactions** (`prod.rpc.solana.pumpfun.transaction`), which shows the compute-unit price and Jito-style tip that transactions which actually landed paid in the last 60 seconds.

It returns low, medium and high fee tiers in micro-lamports per CU and in total lamports, plus tip suggestions and ready-made ComputeBudget instruction data.

```bash
curl 'localhost:8080/v1/fees?cu_limit=150000&accounts=58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2'
```

```json
{
  "recommended": {
    "source": "rpc_recent_prioritization_fees",
    "low":    {"cu_price_micro_lamports": 1681,  "priority_fee_lamports": 253,  "total_fee_lamports": 5253},
    "medium": {"cu_price_micro_lamports": 10000, "priority_fee_lamports": 1500, "total_fee_lamports": 6500},
    "high":   {"cu_price_micro_lamports": 10000, "priority_fee_lamports": 1500, "total_fee_lamports": 6500}
  },
  "tip_lamports": null,
  "compute_budget_instructions": {
    "program_id": "ComputeBudget111111111111111111111111111111",
    "set_compute_unit_limit_data_hex": "02f0490200",
    "set_compute_unit_price_data_hex": "031027000000000000"
  }
}
```

*Example output is trimmed. Values change every slot.*

## What is a Solana priority fee?

A Solana priority fee is an optional fee you add to a transaction to raise its scheduling priority when blocks are busy. You set a compute-unit price in micro-lamports with the ComputeBudget `SetComputeUnitPrice` instruction. The fee is `compute unit limit × compute unit price ÷ 1,000,000` lamports, and it is paid on top of the 5,000-lamport base fee per signature.

## Why combine RPC data with landed transactions?

`getRecentPrioritizationFees` reports the **minimum** fee that landed in each recent slot for the accounts you pass. When you pass no accounts, or only read-only accounts such as a program ID, it often returns all zeros, which can lead you to underpay during a memecoin rush. The landed-transaction stream shows the fee distribution that PumpFun traders are **actually paying**, including the share that also sends a tip. The API prefers the landed data once at least 20 transactions are in the window and falls back to RPC otherwise. Every response shows which source it used.

## Quick start

```bash
git clone https://github.com/no-limit-nodes/solana-priority-fee-api.git
cd solana-priority-fee-api
python -m pip install -r requirements.txt

export NLN_RPC_URL=https://your-nln-rpc-endpoint   # from https://app.nolimitnodes.com
export NLN_API_KEY=sk_live_...                     # enables the landed-transaction stream

python server.py --port 8080
curl 'localhost:8080/v1/fees'
```

One-shot estimate without starting a server:

```bash
python server.py --once --accounts 58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2
```

## API reference

### `GET /v1/fees`

| Query param | Default | Meaning |
|---|---|---|
| `cu_limit` | `200000` | Compute-unit limit you will set (1 to 1,400,000) |
| `signatures` | `1` | Number of signatures, used for the base fee |
| `accounts` | `--accounts` | Comma-separated writable accounts for `getRecentPrioritizationFees` (max 128) |
| `source` | `auto` | `auto`, `rpc` or `landed` |

Response fields:

- `recommended.{low,medium,high}`: the p50, p75 and p90 compute-unit price, with `priority_fee_lamports`, `total_fee_lamports` and `total_fee_sol`.
- `recommended.source`: `landed_pumpfun_transactions` or `rpc_recent_prioritization_fees`.
- `tip_lamports`: the p50, p75 and p90 of non-zero tips in the landed window, plus the share of transactions that tipped.
- `compute_budget_instructions`: hex instruction data for `SetComputeUnitLimit` and for `SetComputeUnitPrice` at the medium tier.
- `sources.rpc` and `sources.landed`: full percentile breakdowns (min, p25, p50, p75, p90, p99, max) for transparency.

It returns `400` for invalid input, `502` when the RPC fails, and `503` when no fee data exists yet.

### `GET /health`

Stream status, total landed events seen, age of the last event, and the latest slot.

## Configuration

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `--host` / `--port` | `HOST` / `PORT` | `127.0.0.1` / `8080` | Bind address |
| `--accounts` | `FEE_ACCOUNTS` | none | Default writable accounts for RPC estimates |
| `--window` | | `60` | Landed-transaction window in seconds |
| `--min-landed` | | `20` | Landed transactions needed before they are preferred over RPC data |
| `--max-cu-price` | | none | Upper bound on any recommended CU price |
| `--no-stream` | | off | RPC-only mode |
| `--cors` | | off | Add `Access-Control-Allow-Origin: *` for browser apps |

## Using the result in a transaction

TypeScript (`@solana/web3.js`):

```ts
const fees = await (await fetch("http://localhost:8080/v1/fees?cu_limit=150000")).json();
tx.add(
  ComputeBudgetProgram.setComputeUnitLimit({ units: 150_000 }),
  ComputeBudgetProgram.setComputeUnitPrice({ microLamports: fees.recommended.medium.cu_price_micro_lamports }),
);
```

Python (`solders`):

```python
from solders.compute_budget import set_compute_unit_limit, set_compute_unit_price
ixs = [set_compute_unit_limit(150_000),
       set_compute_unit_price(fees["recommended"]["medium"]["cu_price_micro_lamports"])]
```

## Repository layout

- [`server.py`](server.py): HTTP API, CLI and stream thread.
- [`fee_estimator.py`](fee_estimator.py): NLN RPC client, landed-fee window, percentile math and ComputeBudget encoders. Has no third-party dependencies.
- [`nln_stream.py`](nln_stream.py): NoLimitNodes gRPC stream client with reconnect.
- [`docs/guide.md`](docs/guide.md): fee formula, choosing accounts, choosing a CU limit, and deployment.
- [`tests/test_fee_api.py`](tests/test_fee_api.py): 14 tests covering math, the window, the estimator, HTTP and gRPC (with a fake server), plus an optional live RPC check.

## FAQ

### How do I calculate a Solana priority fee?

Multiply your compute-unit limit by your compute-unit price in micro-lamports, then divide by 1,000,000 to get lamports. For example, 200,000 CU at 10,000 micro-lamports per CU is a 2,000-lamport priority fee. Add 5,000 lamports per signature for the base fee to get the total.

### Why does `getRecentPrioritizationFees` return 0?

The method reports the lowest fee that landed in each slot for transactions that write-locked the accounts you pass. Read-only accounts, such as program IDs, and calls with no accounts usually return 0. Pass the writable accounts your transaction touches, such as the pool or bonding-curve account, or use the landed-transaction data from this API.

### What compute unit limit should I set?

Simulate your transaction and set the limit slightly above its `unitsConsumed`, for example 10 to 20 percent higher. A tight limit lowers your priority fee at the same CU price, because the fee is charged on the limit you request, not on the units used.

### Is a higher priority fee a guarantee of landing?

No. Priority fees improve scheduling priority, but landing also depends on leader connectivity, account contention, blockhash freshness and, for many trading flows, block-engine tips. Use the `high` tier together with the `tip_lamports` suggestions in competitive moments.

## Related NoLimitNodes resources

- [PumpFun Telegram Alert Bot](https://github.com/no-limit-nodes/pumpfun-telegram-alert-bot): launch, graduation and whale alerts
- [Solana streaming reference (17 topics, 7 languages)](https://github.com/no-limit-nodes/solana-programs-data-streaming-reference)
- [Every Solana program decoded](https://github.com/no-limit-nodes/every-solana-program-decoded)

## License

Released under the [MIT License](LICENSE).
