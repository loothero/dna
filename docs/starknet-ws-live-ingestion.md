# Starknet WS Live Ingestion

This change adds an opt-in push-first live ingestion plane for Starknet pre-confirmed events, transaction receipts, and transaction data.

## Problem

The existing Starknet pending path polls `getBlockWithReceipts(PRE_CONFIRMED)` and then waits for `getStateUpdate(PRE_CONFIRMED)` before a pending DNA block can be written. Event and receipt-only consumers therefore inherit state update latency even though Starknet websocket subscriptions can deliver the relevant receipt/event payload earlier.

There is also a moving-tag failure mode: by the time DNA polls the `PRE_CONFIRMED` tag, the pushed websocket data may already refer to a block that the tag has moved past. Reducing the polling interval alone does not remove that race and does not make event delivery push-based.

## Architecture

Canonical HTTP ingestion remains the source of truth for:

- Backfill.
- Accepted and finalized blocks.
- Reorg reconciliation.
- State updates, storage diffs, nonces, and contract/class changes.
- Optional traces.

The new websocket live plane handles optimistic pending data:

- `starknet_subscribeNewTransactionReceipts` with `PRE_CONFIRMED`.
- `starknet_subscribeNewTransactions` with `PRE_CONFIRMED`.
- Optionally, `starknet_subscribeEvents` with `PRE_CONFIRMED` and a server-level event filter.

Receipt and transaction notifications are correlated by `transaction_hash` in `StarknetLiveAssembler`. As soon as receipt/event data is available for the next pending block, DNA can write a pending block fragment without waiting for `getStateUpdate(PRE_CONFIRMED)`. If the transaction body has not arrived yet, receipt/event-only fragments are still emitted.

Accepted/finalized HTTP ingestion later writes canonical blocks and prunes live assembler entries through the accepted block number.

If live mode is enabled but no pushed live block is available for a pending refresh, DNA falls back to the existing HTTP pre-confirmed pending ingestion path. Pushed receipt/event data still wins whenever it is available, so event and receipt consumers do not wait for HTTP state updates in the normal live path.

When WebSocket data is ahead of the stream's current canonical head, DNA may emit that optimistic live payload immediately, but the message does not advance `end_cursor` past the canonical head. This preserves the protocol's resume semantics: reconnecting from `end_cursor` cannot skip accepted canonical blocks. Consumers may therefore see optimistic pending data replayed or later reconciled by the canonical accepted/finalized path.

## Configuration

Live ingestion is disabled by default.

Enable it with:

```bash
STARKNET_WS_URL=ws://...
STARKNET_WS_LIVE_INGESTION_ENABLED=true
```

`STARKNET_WS_URL` is required when `STARKNET_WS_LIVE_INGESTION_ENABLED=true`; startup fails if live ingestion is enabled without a WebSocket URL.

Setting `STARKNET_WS_LIVE_INGESTION_ENABLED=true` also enables the pending stream lane. The existing `STARKNET_INGEST_PRE_CONFIRMED=true` HTTP pending behavior remains available when websocket live ingestion is disabled.

Optional event subscription filters are configured at the DNA server level:

```bash
STARKNET_WS_LIVE_EVENT_ADDRESS=0x...
STARKNET_WS_LIVE_EVENT_KEY0=0x...
```

These filters are not derived from individual DNA stream requests. They should be configured for the live event surface that the server is expected to optimize.

## Benchmark

The benchmark command compares direct Starknet `subscribeEvents` first-seen time against DNA pending stream first-seen time for matching events. The DNA filter includes the matching event and receipt, but not the transaction body, so the measurement targets the receipt/event live path directly.

```bash
cargo run -p apibara-benchmark -- starknet-live-latency \
  --direct-ws-url ws://64.34.87.87:9545/ws/rpc/v0_10 \
  --stream-url http://localhost:7007 \
  --game-core-address 0x... \
  --game-event-key 0x... \
  --adventurer-id 0x...
```

It prints one CSV-style row per matched event:

```text
txHash,blockNumber,eventIndex,finality,directSubscribeEventsSeenMs,dnaStreamSeenMs,dnaMinusSubscribeEventsMs
```

Acceptance target: `p95DnaMinusSubscribeEventsMs <= 500` under normal live conditions.
