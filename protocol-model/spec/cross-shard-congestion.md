# Cross-shard communication & congestion control

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `runtime/runtime/src/congestion_control.rs`, `runtime/runtime/src/bandwidth_scheduler/`, `runtime/runtime/src/receipt_manager.rs`, `core/primitives/src/congestion_info.rs`, `core/primitives/src/receipt.rs`, `core/primitives/src/bandwidth_scheduler.rs`, `core/store/src/trie/receipts_column_helper.rs`

## Role

Receipts are the sole mechanism for cross-shard communication: a chunk applied on
one shard produces receipts whose receiver may live on another shard. This
component covers everything between "a receipt has been produced" and "the
receiver shard accepts it into its work queue": the receipt kinds, the persistent
trie queues that hold them (delayed, outgoing buffers, postponed, promise-yield),
the per-shard **congestion control** (NEP-539) that throttles how much gas a
sender may forward to a congested receiver, and the **bandwidth scheduler** that
throttles how many *bytes* of receipts may travel on each shard-to-shard link.
Receipts are produced and executed by [runtime-execution](runtime-execution.md);
their struct layout lives in
[data-structures-serialization](data-structures-serialization.md); the queues and
`CongestionInfo` are persisted via [state-storage](state-storage.md) and the
finalized `CongestionInfo` is committed in chunk headers by
[block-chunk-production](block-chunk-production.md).

## Key data structures

- **`Receipt` / `ReceiptEnum`** — `core/primitives/src/receipt.rs:74`, `:567` —
  `Receipt` is an enum with only the `V0(ReceiptV0)` variant today
  (`receipt.rs:74`; the `ReceiptV0` struct is at `:57`). `ReceiptEnum` (`:567`) is the payload, discriminants pinned
  with `#[borsh(use_discriminant = true)]`: `Action = 0`, `Data = 1`,
  `PromiseYield = 2`, `PromiseResume = 3`, `GlobalContractDistribution = 4`,
  `ActionV2 = 5`, `PromiseYieldV2 = 6`. `ActionReceiptV2` (`:623`) adds a
  `refund_to` field over `ActionReceipt` (`:591`); both are accessed uniformly via
  `VersionedActionReceipt` (`:647`) and `VersionedReceiptEnum` (`:747`).
- **`ActionReceipt` cross-shard fields** — `receipt.rs:591` — `output_data_receivers:
  Vec<DataReceiver>` (`:599`, where to route this receipt's output data) and
  `input_data_ids: Vec<CryptoHash>` (`:605`, the data dependencies that must all
  arrive before the receipt executes). A `DataReceiver` (`:38`) pairs a `data_id`
  with the `receiver_id` (possibly on another shard) that awaits it.
- **`DataReceipt` / `ReceivedData`** — `receipt.rs:828`, `:849` — a `DataReceipt`
  carries `data_id` and `data: Option<Vec<u8>>` (`None` = failed dependency). When
  delivered it is stored as `ReceivedData` keyed by `(account_id, data_id)` until
  the awaiting action receipt is ready.
- **`CongestionInfo` / `CongestionInfoV1`** — `core/primitives/src/congestion_info.rs:187`,
  `:460` — per-shard, versioned, committed in the chunk header. `V1` tracks
  `delayed_receipts_gas: u128`, `buffered_receipts_gas: u128`, `receipt_bytes: u64`
  (borsh size of all delayed/buffered/postponed/yielded receipts), and
  `allowed_shard: u16` (the one shard permitted to forward to us when we are fully
  congested).
- **`CongestionControl`** — `congestion_info.rs:18` — bundles a
  `CongestionControlConfig`, a remote chunk's `CongestionInfo`, and its
  `missed_chunks_count`; it is the decision object (`congestion_level`,
  `outgoing_gas_limit`, `shard_accepts_transactions`). It is built from *another*
  shard's finalized info to decide forwarding; the local shard builds its own
  `CongestionInfo` directly.
- **`ExtendedCongestionInfo` / `BlockCongestionInfo`** — `congestion_info.rs:434`,
  `:390` — `ExtendedCongestionInfo` = `CongestionInfo` + `missed_chunks_count`;
  `BlockCongestionInfo` is a `BTreeMap<ShardId, ExtendedCongestionInfo>` for all
  shards in the block, deterministically ordered (ordering matters for allowed-shard
  selection). This is `apply_state.congestion_info`.
- **`ReceiptSink` / `ReceiptSinkV2`** — `runtime/runtime/src/congestion_control.rs:28`,
  `:58` — the per-chunk object that forwards or buffers each outgoing receipt. Holds
  `own_congestion_info`, the growing `outgoing_receipts: Vec<Receipt>`, per-receiver
  `outgoing_limit: HashMap<ShardId, OutgoingLimit>` (`:65`), the persistent
  `outgoing_buffers`, `outgoing_metadatas`, and the `bandwidth_scheduler_output`.
- **`OutgoingLimit`** — `congestion_control.rs:74` — `{ gas: Gas, size: u64 }`, the
  remaining gas (from congestion control) and size (from the bandwidth grant) a
  sender may still forward to one receiver this chunk.
- **`DelayedReceiptQueueWrapper`** — `congestion_control.rs:802` — wraps the
  persistent `DelayedReceiptQueue`, accumulating `new_/removed_delayed_gas` and
  `_bytes` so all `CongestionInfo` deltas can be applied at the end
  (`apply_congestion_changes`, `:931`).
- **Persistent trie queues** — `core/store/src/trie/receipts_column_helper.rs` —
  `TrieQueue` trait (`:63`) gives FIFO `push_back`/`pop_front`/`pop_n`/`iter` over
  trie-stored items keyed by `TrieQueueIndices { first_index, next_available_index }`.
  Concrete queues: `DelayedReceiptQueue` (`:30`, key `TrieKey::DelayedReceipt`) and
  `ShardsOutgoingReceiptBuffer` (`:39`) → one `OutgoingReceiptBuffer` per receiver
  shard (`:52`, key `TrieKey::BufferedReceipt { index, receiving_shard }`). Items are
  `ReceiptOrStateStoredReceipt` (`:262`, `:310`).
- **`ReceiptOrStateStoredReceipt` / `StateStoredReceipt`** — `receipt.rs:150`, `:97` —
  the on-trie form. `StateStoredReceipt` (V0/V1) wraps a receipt plus
  `StateStoredReceiptMetadata { congestion_gas, congestion_size }` (`:124`) so the
  precomputed congestion cost is read back rather than recomputed. Custom borsh
  discriminates by a two-byte `STATE_STORED_RECEIPT_TAG = u8::MAX` prefix (`:134`,
  `:235`, `:312`); a plain `Receipt::V0` always has `0` as its second byte, so the
  two are distinguishable on read.
- **`BandwidthSchedulerParams`** — `core/primitives/src/bandwidth_scheduler.rs:287` —
  `{ base_bandwidth, max_shard_bandwidth, max_receipt_size, max_allowance }`, derived
  from the runtime config and `num_shards` via `new` (`:304`).
- **`GrantedBandwidth`** — `runtime/runtime/src/bandwidth_scheduler/scheduler.rs:158` —
  `BTreeMap<(sender ShardId, receiver ShardId), Bandwidth>`; `get_granted_bandwidth`
  (`:163`) returns `0` for links with no grant.
- **`PromiseYieldTimeout` / `PromiseYieldIndices`** — `receipt.rs:1090`, `:1075` — the
  timeout-queue entry `{ account_id, data_id, expires_at: BlockHeight }` and its FIFO
  indices, ordered by `expires_at`.

## Behavior

### 1. Receipt kinds and their queues

| Kind (`ReceiptEnum`) | Meaning | Where it lives while waiting |
| --- | --- | --- |
| `Action` / `ActionV2` | a batch of actions to run on the receiver | **delayed** queue (incoming backlog) or **outgoing buffer** (waiting to be forwarded); if it has unmet `input_data_ids`, **postponed** in state |
| `Data` | delivers one output-data value (`data_id`) | stored as `ReceivedData`; may release a postponed action receipt |
| `PromiseYield` / `PromiseYieldV2` | an action receipt parked awaiting an explicit resume | **promise-yield receipt** in state, plus a timeout entry in the promise-yield queue |
| `PromiseResume` | delivers the yield's input data (or a timeout with `data: None`) | transient; resolves a parked yield |
| `GlobalContractDistribution` | broadcasts global contract code to shards | routed by `target_shard`; out of scope here |

The runtime processes receipts each chunk in a fixed order: **local → delayed →
incoming → promise-yield timeouts** (`runtime/runtime/src/lib.rs:2670-2696`,
`process_receipts`).

### 2. Admission into the delayed queue (incoming backpressure)

Incoming receipts are validated first, then either executed or deferred:
`process_incoming_receipts` (`lib.rs:2541`) executes a receipt only while
`total.compute < compute_limit` and the storage-proof size limit is not exceeded;
otherwise it calls `delayed_receipts.push(...)` (`lib.rs:2578`) to persist it for a
later chunk. `compute_limit` is the chunk gas limit (`lib.rs:2668`). Delayed
receipts are drained FIFO in `process_delayed_receipts` (`lib.rs:2441`), stopping on
the same compute / proof-size checks (`lib.rs:2462`). Each `pop`/`push` updates the
wrapper's accumulated gas/bytes (`congestion_control.rs:838` push, `:880` pop), which
feed `delayed_receipts_gas` and `receipt_bytes` in `CongestionInfo`.

`DelayedReceiptQueueWrapper::pop` (`congestion_control.rs:880`) also breaks *before*
popping if `trie.check_proof_size_limit_exceed()` (`:889`), and — for
ReshardingV3 — accounts gas/bytes for every popped receipt but returns only those
whose `receiver_shard_id` matches the current shard (`receipt_filter_fn`, `:874`),
skipping receipts that belong to a sibling shard after a split.

### 3. Data dependencies: postponed receipts and promise results

An action receipt carries `input_data_ids` (`receipt.rs:605`) that must all be
satisfied before it runs. In `process_action_receipt` (`lib.rs:1529`) the runtime
counts how many of those ids are *not* yet in state (`has_received_data`,
`lib.rs:1546`); for each missing one it records a `PostponedReceiptId` link
(`lib.rs:1550`). If `pending_data_count == 0` it executes immediately
(`lib.rs:1561`); otherwise it stores a `PendingDataCount` and the receipt itself as a
**postponed receipt** (`lib.rs:1581`, `set_postponed_receipt`).

When a `Data` receipt arrives (`process_receipt`, `lib.rs:1322`) the runtime writes
`ReceivedData` (`lib.rs:1325`), then, if a `PostponedReceiptId` link exists, decrements
`PendingDataCount`; when it reaches 1→0 it removes the postponed receipt and executes
it (`lib.rs:1357-1391`). On execution, an action receipt's outputs become `Data`
receipts routed to each `output_data_receivers` entry (`lib.rs:1059`), so a
cross-contract call is: caller creates a callback action receipt with an
`input_data_id`, records itself as an `output_data_receiver` on the callee's receipt
(`receipt_manager.rs:111`, `create_action_receipt`), and the callee's return value is
turned into the `Data` receipt that resolves the dependency.

The **congestion cost** of a receipt is defined by `compute_receipt_congestion_gas`
(`congestion_control.rs:678`): for action receipts it sums prepaid exec fees, the
`new_action_receipt` fee, prepaid send fees, and attached function-call gas
(`action_receipt_congestion_gas`, `:716`). `Data`, `PromiseYield`, `PromiseResume`,
and `GlobalContractDistribution` all count as **zero** congestion gas
(`:687-712`) — the MVP does not charge them (data/postponed costs would require extra
trie lookups). Size is the borsh length of the whole receipt (`compute_receipt_size`,
`:964`).

### 4. Forwarding vs buffering (congestion + bandwidth admission)

Every outgoing receipt goes through `ReceiptSink::forward_or_buffer_receipt`
(`congestion_control.rs:162` → `:292`). It computes the receipt's receiver shard,
size, and congestion gas, then calls `try_forward` (`:403`):

1. If `size > max_receipt_size`, size is clamped to `max_receipt_size` for the limit
   comparison (bug workaround for oversized receipts, issue #12606, `:417`).
2. The receiver's `OutgoingLimit` is looked up; a missing entry defaults to
   `{ gas: Gas::MAX, size: 0 }` (`:439`) — since the bandwidth scheduler, a shard may
   send **zero** bytes on a link with no grant.
3. Under `ClampOutgoingGasAdmission` (PV 85) the *admission* gas is clamped to
   `allowed_shard_outgoing_gas` (`:443`), so a single very-expensive receipt cannot be
   blocked forever by the gas limit; pre-85 the full receipt gas is used.
4. Forward iff `forward_limit.gas >= admission_gas && forward_limit.size >= size`
   (`:451`); then the receipt is pushed to `outgoing_receipts` and the limit is
   decremented by the *actual* gas and size (`:453`). Otherwise it is returned
   `NotForwarded` and `buffer_receipt` (`:466`) pushes it onto the outgoing buffer for
   that shard, growing `own_congestion_info` by its size and buffered gas (`:486`).

At the **start** of each chunk `ReceiptSink::new` (`congestion_control.rs:86`) builds
the per-receiver limits: gas comes from the *receiver's* congestion control
`outgoing_gas_limit(own_shard)` (`:105`) — for same-shard receipts it is `Gas::MAX`
(`:110`, backpressure would not help); size comes from the bandwidth grant for the
`(own_shard, receiver)` link (`:113`). Before processing new receipts, buffered
receipts from previous chunks are drained first: `forward_from_buffer` (`:236`) walks
parent-shard buffers (resharding leftovers) then current-layout buffers
(`:257-279`), forwarding each via `try_forward` and shrinking `own_congestion_info`
as receipts leave (`:368`).

### 5. Congestion control math (NEP-539)

`CongestionControl::congestion_level` (`congestion_info.rs:44`) is the **max** of four
fractions, each clamped to [0,1] (`clamped_f64_fraction`, `:474`):

- incoming = `delayed_receipts_gas / max_congestion_incoming_gas` (`:331`)
- outgoing = `buffered_receipts_gas / max_congestion_outgoing_gas` (`:338`)
- memory = `receipt_bytes / max_congestion_memory_consumption` (`:345`)
- missed-chunks = `missed_chunks_count / max_congestion_missed_chunks`, but 0 when
  `missed_chunks_count <= 1` (`:68`)

`outgoing_gas_limit(sender_shard)` (`:80`) drives how much a given sender may forward
to this shard next block:

- **Fully congested** (`congestion_level == 1.0`, `is_fully_congested`, `:95`): only
  the `allowed_shard` may send, and only `allowed_shard_outgoing_gas`; every other
  shard gets `Gas::ZERO` (`:83-89`). This is the "red light" that guarantees progress
  while stopping unbounded growth.
- Otherwise: linearly interpolate between `max_outgoing_gas` and `min_outgoing_gas` by
  the congestion level via `mix_gas` (`:91`, `mix` at `:484` works in integer space to
  avoid f64 precision loss at high gas).

`outgoing_size_limit` (`:103`) gives the allowed shard `outgoing_receipts_big_size_limit`
and everyone else `outgoing_receipts_usual_size_limit` (used as bandwidth-request
context, not the primary size gate — that is the bandwidth grant).

**Transaction admission**: `shard_accepts_transactions` (`:123`) rejects new
transactions targeting this shard once `congestion_level >=
reject_tx_congestion_threshold` (default 0.8, asserted in tests at `:641`), reporting
which of the four dimensions caused it (`RejectTransactionReason`, `:161`).
`process_tx_limit` (`:115`) throttles gas spent converting new transactions to
receipts using only the *incoming* congestion.

### 6. Finalizing per-shard congestion info

At the end of chunk apply (`lib.rs:2754`): the delayed-queue deltas are folded in via
`apply_congestion_changes` (`lib.rs:2758`), then `finalize_allowed_shard`
(`congestion_info.rs:360`) picks the `allowed_shard` deterministically —
`get_new_allowed_shard` (`:370`) is `all_shards[congestion_seed % num_shards]`, where
`congestion_seed = block_height + shard_index` (`lib.rs:2767`). The result becomes
`ApplyResult.congestion_info` (`lib.rs:2862`), which
[block-chunk-production](block-chunk-production.md) writes into the chunk header;
`validate_extra_and_header` (`congestion_info.rs:204`) later checks the header matches
the chunk extra.

`bootstrap_congestion_info` (`congestion_control.rs:743`) recomputes a shard's info
from scratch by iterating the delayed queue and every outgoing buffer — an
IO-intensive fallback used only when no prior `CongestionInfo` exists
(`own_congestion_info`, `lib.rs:2872`).

### 7. Bandwidth scheduler

Congestion control limits *gas*; the bandwidth scheduler limits *bytes per link*. It
runs once at the start of chunk apply (`run_bandwidth_scheduler`,
`bandwidth_scheduler/mod.rs:44`; called at `lib.rs:1767`) and produces
`GrantedBandwidth` that seeds each `OutgoingLimit.size`. Determinism is essential:
every shard runs the identical algorithm on the identical inputs (bandwidth requests
+ shard statuses) and must compute the same grants; a `sanity_check_hash` over the
shard set is chained into the persistent state to catch divergence
(`mod.rs:124-137`).

`BandwidthScheduler::run` (`scheduler.rs:200`) → `schedule_bandwidth` (`:303`) has four
stages, matching the module doc (`scheduler.rs:60-73`):

1. `init_budgets` (`:314`): every shard may send and receive at most
   `max_shard_bandwidth`.
2. `increase_allowances` (`:323`): each link gains `max_shard_bandwidth / num_shards`
   of *allowance* (fairness token bucket), capped at `max_allowance` (`:442`).
3. `grant_base_bandwidth` (`:340`): grant `base_bandwidth` on every allowed link
   unconditionally — small senders need no request.
4. `process_bandwidth_requests` (`:347`): requests are bucketed by link allowance and
   served highest-allowance-first; ties within a bucket are shuffled with a
   seeded `ChaCha20Rng` (`:362`, seed = `prev_block_hash`, passed at `mod.rs:115`). Each
   granted increment decreases the sender/receiver budgets and the link allowance
   (`try_grant_bandwidth`, `:467`); the request is re-queued at its new (lower)
   allowance so large requests are deprioritized after each grant.
5. `distribute_remaining_bandwidth` (`:393`): fairly hand out any leftover budget to
   improve utilization (`bandwidth_scheduler/distribute_remaining.rs`).

`is_link_allowed` (`calculate_is_link_allowed`, `:506`) integrates with congestion
control so the two limits never deadlock: a link is forbidden if the receiver's status
is unknown, if the **receiver's** last chunk was missing (`:516`), if the **sender's**
last chunk was missing (`:524`, it will not apply a chunk this height anyway), or if
the receiver is fully congested and the sender is not its allowed shard (`:535`). This
mirrors congestion control's red-light so that any granted bandwidth can actually be
used by at least one receipt (module doc, `scheduler.rs:75-90`).

**Generating requests**: at the end of apply, `generate_bandwidth_requests`
(`congestion_control.rs:503`, called `lib.rs:2774`) builds one `BandwidthRequest` per
receiver from the *sizes of receipt groups* still in that shard's outgoing buffer
(`get_receipt_group_sizes_for_buffer_to_shard`, `:571`), using `OutgoingMetadatas`.
Requests for a child shard also fold in the parent-shard buffer (`:545`) so
post-resharding receipts aren't stranded. Sizes are clamped to `max_receipt_size`
(`:561`). Requests are consumed by the scheduler on the following height, so the loop
is: grant → forward-up-to-grant → generate new requests → repeat.

### 8. Promise yield / resume (parking and timeout)

A `PromiseYield` receipt, when processed, is simply stored keyed by
`(receiver_id, data_id)` and awaits resume (`process_receipt`, `lib.rs:1431-1434`,
`set_promise_yield_receipt`). Yields are **instant receipts** (`is_instant_receipt`,
`receipt.rs:474`) and confined to a single account, so they never cross shard
boundaries and count as zero congestion gas (`congestion_control.rs:696`).

A `PromiseResume` receipt (`lib.rs:1436`) delivers the awaited data. If it is a
*timeout* resume (`data: None`) and the yield status is `ResumeInitiated`, it is
dropped because a real resume already exists (`lib.rs:1441-1444`). Otherwise, if the
parked yield receipt is found, the runtime removes it and its status, stores the
`ReceivedData`, and executes the yield receipt immediately (`lib.rs:1450-1498`); a
second resume for the same `data_id` finds nothing and is ignored (`lib.rs:1499`).

**Timeouts** are the last step of `process_receipts`:
`resolve_promise_yield_timeouts` (`lib.rs:2986`) walks the `PromiseYieldTimeout` queue
in `expires_at` order, stopping at the first entry with `expires_at >
block_height` (`:3019`) or once the compute/proof-size budget is hit (`:3003`). For
each expired entry whose yield still exists, it synthesizes a `PromiseResume` with
`data: None` destined for the same (local) account and forwards/buffers it
(`:3037-3074`); the timeout is then dequeued (`:3078`). The timeout resume and any
real resume are ordered to the same shard, so a late timeout after a real resume is
simply discarded.

## Interactions

- **Consumes**: `apply_state.congestion_info` (`BlockCongestionInfo` for all shards,
  from the block's chunk headers), `apply_state.bandwidth_requests` (previous height's
  requests), and the persistent trie queues/scheduler state. Receipts to forward come
  from [runtime-execution](runtime-execution.md) (execution outputs, `lib.rs:1059`) and
  from the delayed/buffer queues.
- **Produces**: `ApplyResult.congestion_info` and `ApplyResult` bandwidth requests
  (both committed to the chunk header by
  [block-chunk-production](block-chunk-production.md)), the `outgoing_receipts` vector
  (delivered to receiver shards), and mutated delayed/buffer/postponed/yield queues
  persisted via [state-storage](state-storage.md).
- **Touches**: [transactions-and-signing](transactions-and-signing.md) via
  `shard_accepts_transactions` (tx admission at chunk production);
  [resharding](resharding.md) via parent-shard buffer draining, delayed-receipt shard
  filtering, and cross-boundary bandwidth requests.
- Receipt struct layout / borsh: [data-structures-serialization](data-structures-serialization.md).

## Protocol-version-gated behavior

Verified against `core/primitives-core/src/version.rs` in this tree
(`MIN_SUPPORTED_PROTOCOL_VERSION = 83`, `STABLE_PROTOCOL_VERSION = 86`). Because the
minimum supported version is 83, congestion control (old `CongestionControl`, v68),
stateless validation (v69), the `StateStoredReceipt` format (v72), the
`BandwidthScheduler` (v74), and `CurrentEpochStateSync` are all **unconditionally
active** on 2.13.0 — their feature enum entries are now `_Deprecated*`
(`version.rs:243`, `:257`, `:273`, `:534`) and the code paths run without gating (e.g.
`ReceiptSink` has only the `V2` variant, `congestion_control.rs:28`).

Still-gated features touching this component:

| Feature | Activates | Effect |
| --- | --- | --- |
| `ClampOutgoingGasAdmission` | PV **85** (`version.rs:573`) | In `try_forward`, the admission-gas comparison is clamped to `allowed_shard_outgoing_gas` (`congestion_control.rs:443`) so a single receipt above the per-shard outgoing gas limit can still be forwarded instead of stalling the buffer. Pre-85 the full receipt gas is required. |
| `EnforcePerReceiptStorageProofLimit` | PV **86** (`version.rs:576`) | The PV-86 stable feature. Enforces a per-receipt storage-proof limit during application; interacts with the `check_proof_size_limit_exceed` breaks that bound delayed-queue draining and yield-timeout processing (`congestion_control.rs:889`, `lib.rs:2464`, `:3003`). |
| `YieldWithId` | PV **85** (`version.rs:570`) | On resume, cleans up the `yield_id ↔ data_id` mapping created by `promise_yield_create_with_id` (`lib.rs:1460-1473`). Pre-85 no such mapping exists. |
| `ExecutionMetadataV4` | PV **85** (`version.rs:571`) | Execution of a forwarded/delayed action receipt emits `ExecutionMetadata::V4` with per-action contract info (`lib.rs:1135`); pre-85 emits `V3`. Peripheral to routing/congestion. |

There is **no** `FixContractLoadingError` feature on 2.13.0; the loading-cost feature
present is `FixContractLoadingCost`, which is nightly-only (PV 129, `version.rs:579`)
and does not touch this component. The MAINNET schedule votes PV 86 on 2026-07-20
(`core/primitives/src/version.rs`).

## Invariants & failure modes

- **Bounded incoming work**: a fully congested shard grants `Gas::ZERO` to all
  senders except its `allowed_shard` (`congestion_info.rs:83`), and the bandwidth
  scheduler forbids all links into it except the allowed one (`scheduler.rs:535`);
  together these stop unbounded delayed-queue growth while guaranteeing one sender can
  always make progress. Asserted by the `test_missed_chunks_finalize`
  (`congestion_info.rs:814`) and the `test_*_congestion` tests
  (`:769` missed-chunks, `:621` memory, `:670` incoming, `:723` outgoing).
- **Empty buffers ⇒ zero buffered gas**: after draining, if all outgoing buffers are
  empty the code asserts `own_congestion_info.buffered_receipts_gas() == 0`
  (`congestion_control.rs:283`).
- **Congestion accounting cannot underflow**: `CongestionInfo` add/remove use
  `checked_add`/`checked_sub` and return `RuntimeError::UnexpectedIntegerOverflow`
  (`congestion_info.rs:245-321`); `bootstrap_congestion_info` maps overflow to
  `StorageError::StorageInconsistentState` (`congestion_control.rs:969`).
- **Deterministic cross-shard agreement**: `allowed_shard` is a pure function of
  `(block_height + shard_index) % num_shards` (`congestion_info.rs:370`,
  `lib.rs:2767`); the bandwidth scheduler's `sanity_check_hash` (`mod.rs:124`) and
  `scheduler_state_hash` (`mod.rs:137`) detect any shard running with divergent inputs.
- **Oversized-receipt workaround**: receipts above `max_receipt_size` are treated as
  exactly `max_receipt_size` for both forwarding limits (`congestion_control.rs:417`)
  and bandwidth requests (`:561`) so they cannot get permanently stuck (issue #12606).
- **Inconsistent-state failures**: a missing delayed/buffered/postponed/yield item
  referenced by an index yields `StorageError::StorageInconsistentState`
  (`receipts_column_helper.rs:111`, `lib.rs:3011`); a delayed receipt that fails
  `validate_receipt` on pop is likewise treated as inconsistent state, not a soft error
  (`lib.rs:2506`).
- **Backpressure knob**: transactions to a shard at or above
  `reject_tx_congestion_threshold` congestion are rejected at production time
  (`congestion_info.rs:137`), surfacing as a congestion rejection to the submitter
  rather than being queued forever.

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `core/primitives/src/receipt.rs:567` | `ReceiptEnum` | Enumerates the six receipt payload kinds with pinned borsh discriminants. |
| `core/primitives/src/receipt.rs:438` | `Receipt::receiver_shard_id` | Maps a receipt to its receiver shard (account id → shard, or `target_shard`). |
| `core/primitives/src/receipt.rs:474` | `Receipt::is_instant_receipt` | PromiseYield and single-`DeleteAccount` receipts run immediately, off-queue. |
| `core/primitives/src/congestion_info.rs:44` | `CongestionControl::congestion_level` | max of incoming/outgoing/memory/missed-chunks fractions. |
| `core/primitives/src/congestion_info.rs:80` | `CongestionControl::outgoing_gas_limit` | Per-sender forward gas budget; red-light for full congestion. |
| `core/primitives/src/congestion_info.rs:123` | `CongestionControl::shard_accepts_transactions` | Tx admission gate at `reject_tx_congestion_threshold`. |
| `core/primitives/src/congestion_info.rs:360` | `CongestionInfo::finalize_allowed_shard` | Deterministic allowed-shard selection. |
| `runtime/runtime/src/congestion_control.rs:86` | `ReceiptSink::new` | Builds per-receiver gas (congestion) + size (bandwidth) limits. |
| `runtime/runtime/src/congestion_control.rs:236` | `ReceiptSinkV2WithInfo::forward_from_buffer` | Drains parent then current-layout outgoing buffers. |
| `runtime/runtime/src/congestion_control.rs:403` | `ReceiptSinkV2::try_forward` | Forward-or-not decision incl. `ClampOutgoingGasAdmission` and oversize clamp. |
| `runtime/runtime/src/congestion_control.rs:466` | `ReceiptSinkV2::buffer_receipt` | Buffers a receipt, growing own congestion info. |
| `runtime/runtime/src/congestion_control.rs:678` | `compute_receipt_congestion_gas` | Congestion gas per receipt kind (data/yield/resume = 0). |
| `runtime/runtime/src/congestion_control.rs:743` | `bootstrap_congestion_info` | IO-heavy recompute of `CongestionInfo` from queues. |
| `runtime/runtime/src/congestion_control.rs:802` | `DelayedReceiptQueueWrapper` | Delayed-queue push/pop with congestion delta accumulation + resharding filter. |
| `core/store/src/trie/receipts_column_helper.rs:63` | `TrieQueue` | FIFO trie-backed queue for delayed & buffered receipts. |
| `runtime/runtime/src/bandwidth_scheduler/mod.rs:44` | `run_bandwidth_scheduler` | Per-chunk scheduler entry; reads/writes persistent state, sanity hash. |
| `runtime/runtime/src/bandwidth_scheduler/scheduler.rs:303` | `BandwidthScheduler::schedule_bandwidth` | Four-stage grant algorithm. |
| `runtime/runtime/src/bandwidth_scheduler/scheduler.rs:506` | `calculate_is_link_allowed` | Blocks links to congested / missing-chunk shards. |
| `runtime/runtime/src/congestion_control.rs:503` | `ReceiptSinkV2::generate_bandwidth_requests` | Builds next-height requests from buffered receipt groups. |
| `runtime/runtime/src/lib.rs:2541` | `Runtime::process_incoming_receipts` | Executes or delays incoming receipts by compute/proof budget. |
| `runtime/runtime/src/lib.rs:1529` | `Runtime::process_action_receipt` | Postpones action receipts with unmet input data. |
| `runtime/runtime/src/lib.rs:1322` | `Runtime::process_receipt` (Data arm) | Resolves postponed receipts as data arrives. |
| `runtime/runtime/src/lib.rs:2986` | `resolve_promise_yield_timeouts` | Times out parked yields via synthetic PromiseResume. |
| `runtime/runtime/src/lib.rs:2754-2779` | apply finalization | Applies congestion deltas, picks allowed shard, generates bandwidth requests. |
| `core/primitives-core/src/version.rs:573`,`:576` | `ClampOutgoingGasAdmission`, `EnforcePerReceiptStorageProofLimit` | PV 85 / 86 gates relevant here. |

## Open questions

- The `PromiseYieldTimeout` queue *entries* are created during action execution
  (yield host function), which lives in [runtime-execution](runtime-execution.md); the
  exact `expires_at = block_height + timeout_length` computation was not traced here
  and is out of scope for this spec.
- `validate_extra_and_header` (`congestion_info.rs:204`) explicitly does **not** yet
  validate the `allowed_shard` field (`TODO(congestion_control)` at `:203`); whether
  this is exploitable is not determined from code alone.
