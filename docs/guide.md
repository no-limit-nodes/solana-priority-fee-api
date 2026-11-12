# Solana Priority Fee Guide: Formula, Accounts and Deployment

This guide explains the math behind the Solana Priority Fee API, how to choose the accounts and compute-unit limit you pass, and how to run the service in production.

## What is the Solana priority fee formula?

```text
priority_fee_lamports = ceil(cu_limit × cu_price_micro_lamports / 1,000,000)
total_fee_lamports    = priority_fee_lamports + 5,000 × signatures
```

- `cu_limit` is the value you set with `SetComputeUnitLimit` (ComputeBudget instruction tag `2`, u32 little-endian). The maximum per transaction is 1,400,000.
- `cu_price` is the value you set with `SetComputeUnitPrice` (tag `3`, u64 little-endian), in micro-lamports per compute unit.
- The API returns both instructions as hex in `compute_budget_instructions`, for clients that build transactions by hand.

## How are the tiers chosen?

| Tier | Percentile | Typical use |
|---|---|---|
| `low` | p50 | Non-urgent transfers and housekeeping |
| `medium` | p75 | Normal swaps |
| `high` | p90 | Launches, snipes, liquidations and other contested moments |

Percentiles use the nearest-rank method over the selected source. With `--max-cu-price`, every tier is capped, which protects a bot from following a fee spike.

## Which source does the API use?

1. **Landed PumpFun transactions** (`source: landed_pumpfun_transactions`). NLN topic `prod.rpc.solana.pumpfun.transaction` carries `compute_unit_price`, `compute_unit_limit` and `tip_amount` for every PumpFun-touching transaction. The API removes duplicate signatures and keeps a rolling window, 60 seconds by default. It is used once at least `--min-landed` (20) transactions are in the window.
2. **RPC** (`source: rpc_recent_prioritization_fees`). This calls `getRecentPrioritizationFees` on your NLN RPC endpoint. It returns per-slot minimums for up to the last 150 slots, and the API caches each account set for 2 seconds.

Force a source with `?source=rpc` or `?source=landed`.

## Which accounts should I pass?

Pass the **writable** accounts your transaction will lock: the AMM pool, the bonding-curve account, the token accounts of a hot market, and so on. Fees are local to those accounts, so they reflect contention on your exact market. Program IDs are never write-locked, so they almost always return 0. You can pass up to 128 accounts.

## How do I pick a compute unit limit?

1. Simulate the transaction with `simulateTransaction` and read `unitsConsumed`.
2. Set the limit 10 to 20 percent above that value.
3. Request `/v1/fees?cu_limit=<your limit>`. The lamport amounts in the response are then exact for your transaction.

The priority fee is charged on the limit you request, so an oversized limit costs money without improving priority.

## How do I deploy it?

```ini
[Unit]
Description=Solana Priority Fee API
After=network-online.target

[Service]
WorkingDirectory=/opt/solana-priority-fee-api
EnvironmentFile=/opt/solana-priority-fee-api/.env
ExecStart=/opt/solana-priority-fee-api/.venv/bin/python server.py --host 0.0.0.0 --port 8080
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- The server binds to `127.0.0.1` by default. Put a reverse proxy with TLS and rate limiting in front of it before exposing it publicly.
- If NLN rejects the stream API key, the server logs the error, reports it in `/health` and keeps serving RPC-based estimates.
- Each request performs at most one RPC call per unique account set every 2 seconds, which keeps RPC usage predictable under load.

## FAQ

### What is the difference between a priority fee and a Jito tip?

A priority fee is paid to the network through the ComputeBudget program and raises scheduling priority with the current leader. A Jito-style tip is a separate SOL transfer to a block-engine tip account, used for bundles and ordering guarantees. The API reports both, because competitive PumpFun traders often pay both.

### Why are the landed fees much higher than the RPC fees?

RPC values are per-slot **minimums** for the accounts you pass, while landed values are the full distribution of what traders paid. During busy launches, many traders overpay to be safe, which pushes the landed p75 and p90 well above the RPC minimum. Use RPC tiers for cost-sensitive flows and landed tiers when speed matters more than cost.

### Does the API sign or send transactions?

No. It only reads public chain data and returns numbers. You add the ComputeBudget instructions to your own transaction and sign it in your own application.
