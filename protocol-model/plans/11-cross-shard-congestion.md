# Plan: Cross-shard communication & congestion control

Generation brief for `spec/cross-shard-congestion.md`. Follow `../CONVENTIONS.md`.

## Scope
How receipts move between shards and how throughput is regulated: the receipt kinds
(action, data, delayed, postponed, buffered, promise-yield), how outgoing receipts are
routed to receiver shards, congestion control (NEP-539) limiting accepted work, and the
bandwidth scheduler allocating cross-shard send capacity.

## Out of scope
- How receipts are *produced/executed* → [runtime-execution](../spec/runtime-execution.md).
- Receipt struct layout → [data-structures-serialization](../spec/data-structures-serialization.md).

## Code to read
- `runtime/runtime/src/congestion_control.rs` — `DelayedReceiptQueueWrapper`, outgoing
  buffers, `CongestionInfo` usage, admission limits.
- `runtime/runtime/src/bandwidth_scheduler/` — bandwidth requests/grants.
- `runtime/runtime/src/receipt_manager.rs` — receipt creation metadata.
- `core/primitives/src/congestion_info.rs` — per-shard congestion metrics.
- `core/store/src/trie/receipts_column_helper.rs` — persistent delayed/buffered queues.
- `core/primitives/src/receipt.rs` — receipt kinds, promise-yield.
- `docs/architecture/how/{cross-shard,receipt-congestion,tx_receipts}.md` (cross-check).

## Questions the spec must answer
- Enumerate the receipt kinds and the queue each lives in (delayed, postponed,
  buffered, incoming) and when a receipt moves between them.
- How does a promise / cross-contract call become a data dependency (input_data_ids /
  output_data_receivers) and how is it resolved (data receipts, promise results)?
- Congestion control: what does `CongestionInfo` track, how is it computed and stored
  per chunk, and how does a sender shard decide how much to forward to a congested
  receiver?
- Bandwidth scheduler: what problem it solves, how send capacity is requested and
  granted across shards.
- Promise yield/resume: how a receipt is parked and later resumed (timeout).
- Failure/backpressure modes: fully congested receiver, queue growth bounds.

## Cross-component edges
- Receipts originate in [runtime-execution](../spec/runtime-execution.md); congestion
  info is committed in chunk headers (block processing) and consulted at apply;
  persisted via [state-storage](../spec/state-storage.md).

## Relevant ProtocolFeatures
- Congestion control (NEP-539), bandwidth scheduler, `ClampOutgoingGasAdmission`,
  promise-yield. Verify against `version.rs`.
