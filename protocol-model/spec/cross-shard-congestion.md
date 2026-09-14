# Cross-shard communication & congestion control

> Protocol version: 87 (stable) · Release: 2.14.0-rc.1 · Derived from commit: 233252e · Generated: 2026-09-14
> Primary crates/files: `runtime/runtime/src/congestion_control.rs`, `runtime/runtime/src/bandwidth_scheduler/`, `runtime/runtime/src/receipt_manager.rs`, `core/primitives/src/congestion_info.rs`, `core/primitives/src/receipt.rs`, `core/primitives/src/bandwidth_scheduler.rs`, `core/store/src/trie/receipts_column_helper.rs`, `core/parameters/res/runtime_configs/`

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
`CongestionInfo` are persisted via [state-storage](state-storage.md); the
finalized `CongestionInfo` is committed in chunk headers, and the outgoing
receipts are delivered as receipt proofs, by
[sharding-chunks](sharding-chunks.md).

## Key data structures

- **`Receipt` / `ReceiptEnum`** — `core/primitives/src/receipt.rs:73`, `:566` —
  `Receipt` is an enum with only the `V0(ReceiptV0)` variant today
  (`receipt.rs:73`; the `ReceiptV0` struct is at `:56`). `ReceiptEnum` (`:566`) is
  the payload, with seven variants and discriminants pinned by
  `#[borsh(use_discriminant = true)]`: `Action = 0`, `Data = 1`,
  `PromiseYield = 2`, `PromiseResume = 3`, `GlobalContractDistribution = 4`,
  `ActionV2 = 5`, `PromiseYieldV2 = 6`. `ActionReceiptV2` (`:622`) adds a
  `refund_to` field over `ActionReceipt` (`:590`); both are accessed uniformly via
  `VersionedActionReceipt` (`:646`) and `VersionedReceiptEnum` (`:746`).
- **`ActionReceipt` cross-shard fields** — `receipt.rs:590` — `output_data_receivers:
  Vec<DataReceiver>` (`:598`, where to route this receipt's output data) and
  `input_data_ids: Vec<CryptoHash>` (`:604`, the data dependencies that must all
  arrive before the receipt executes). A `DataReceiver` (`:37`) pairs a `data_id`
  with the `receiver_id` (possibly on another shard) that awaits it. The
  version-agnostic accessors are `VersionedActionReceipt::output_data_receivers`
  (`:685`) and `::input_data_ids` (`:697`).
- **`DataReceipt` / `ReceivedData`** — `receipt.rs:827`, `:848` — a `DataReceipt`
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
  `ReceiptSinkV2WithInfo` (`:37`) pairs the sink with the immutable per-chunk
  `ReceiptSinkV2Info` (shard layout, parent shard ids) to work around borrow rules.
- **`OutgoingLimit`** — `congestion_control.rs:74` — `{ gas: Gas, size: u64 }`, the
  remaining gas (from congestion control) and size (from the bandwidth grant) a
  sender may still forward to one receiver this chunk.
- **`DelayedReceiptQueueWrapper`** — `congestion_control.rs:802` — wraps the
  persistent `DelayedReceiptQueue`, accumulating `new_/removed_delayed_gas` and
  `_bytes` so all `CongestionInfo` deltas can be applied at the end
  (`apply_congestion_changes`, `:931`).
- **Persistent trie queues** — `core/store/src/trie/receipts_column_helper.rs` —
  `TrieQueue` trait (`:63`) gives FIFO `push_back` (`:83`) / `pop_front` (`:100`) /
  `pop_n` (`:191`) / `iter` over trie-stored items keyed by
  `TrieQueueIndices { first_index, next_available_index }`.
  Concrete queues: `DelayedReceiptQueue` (`:30`, key `TrieKey::DelayedReceipt`) and
  `ShardsOutgoingReceiptBuffer` (`:39`) → one `OutgoingReceiptBuffer` per receiver
  shard (`:52`, key `TrieKey::BufferedReceipt { index, receiving_shard }`). Items are
  `ReceiptOrStateStoredReceipt` (`:262`, `:310`).
- **`ReceiptOrStateStoredReceipt` / `StateStoredReceipt`** — `receipt.rs:149`, `:96` —
  the on-trie form. `StateStoredReceipt` (V0/V1) wraps a receipt plus
  `StateStoredReceiptMetadata { congestion_gas, congestion_size }` (`:123`) so the
  precomputed congestion cost is read back rather than recomputed. Custom borsh
  discriminates by a two-byte `STATE_STORED_RECEIPT_TAG = u8::MAX` prefix (`:133`,
  `:242`, `:331`); a plain `Receipt::V0` always has `0` as its second byte, so the
  two are distinguishable on read.
- **`BandwidthSchedulerParams`** — `core/primitives/src/bandwidth_scheduler.rs:287` —
  `{ base_bandwidth, max_shard_bandwidth, max_single_grant, max_receipt_size,
  max_allowance }`, derived from the runtime config and `num_shards` via `new`
  (`:304`) → `calculate` (`:317`). `base_bandwidth` is *derived*, not configured:
  `(max_shard_bandwidth - max_single_grant) / max(1, num_shards - 1)`, then capped at
  the configured `max_base_bandwidth` (`:341-343`).
- **`GrantedBandwidth`** — `runtime/runtime/src/bandwidth_scheduler/scheduler.rs:158` —
  `BTreeMap<(sender ShardId, receiver ShardId), Bandwidth>`; `get_granted_bandwidth`
  (`:163`) returns `0` for links with no grant.
- **`PromiseYieldTimeout` / `PromiseYieldIndices`** — `receipt.rs:1084`, `:1069` — the
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
incoming → promise-yield timeouts** (`runtime/runtime/src/lib.rs:2787-2825`,
`process_receipts`; `process_local_receipts` at `:2486`).

### 2. Admission into the delayed queue (incoming backpressure)

Incoming receipts are validated first, then either executed or deferred:
`process_incoming_receipts` (`lib.rs:2670`) executes a receipt only while
`total.compute < compute_limit` and the storage-proof size limit is not exceeded;
otherwise it calls `delayed_receipts.push(...)` (`lib.rs:2707`) to persist it for a
later chunk. `compute_limit` is the chunk gas limit (`lib.rs:2797`). Delayed
receipts are drained FIFO in `process_delayed_receipts` (`lib.rs:2570`), stopping on
the same compute / proof-size checks (`lib.rs:2591`). Each `pop`/`push` updates the
wrapper's accumulated gas/bytes (`congestion_control.rs:838` push, `:880` pop), which
feed `delayed_receipts_gas` and `receipt_bytes` in `CongestionInfo`.

`DelayedReceiptQueueWrapper::pop` (`congestion_control.rs:880`) also breaks *before*
popping if `trie.check_proof_size_limit_exceed()` (`:889`), and — for
ReshardingV3 — accounts gas/bytes for every popped receipt but returns only those
whose `receiver_shard_id` matches the current shard (`receipt_filter_fn`, `:874`),
skipping receipts that belong to a sibling shard after a split.

### 3. Data dependencies: postponed receipts and promise results

An action receipt carries `input_data_ids` (`receipt.rs:604`) that must all be
satisfied before it runs. In `process_action_receipt` (`lib.rs:1647`) the runtime
counts how many of those ids are *not* yet in state (`has_received_data`,
`lib.rs:1664`); for each missing one it records a `PostponedReceiptId` link
(`lib.rs:1668`). If `pending_data_count == 0` it executes immediately
(`lib.rs:1679`); otherwise it stores a `PendingDataCount` and the receipt itself as a
**postponed receipt** (`lib.rs:1708`, `set_postponed_receipt`).

When a `Data` receipt arrives (`process_receipt`, `lib.rs:1439`) the runtime writes
`ReceivedData` (`lib.rs:1443`), then, if a `PostponedReceiptId` link exists, decrements
`PendingDataCount`; when it reaches 1→0 it removes the postponed receipt and executes
it (`lib.rs:1465-1509`). On execution, an action receipt's outputs become `Data`
receipts routed to each `output_data_receivers` entry (`lib.rs:1152-1190`), so a
cross-contract call is: caller creates a callback action receipt with an
`input_data_id`, records itself as an `output_data_receiver` on the callee's receipt
(`receipt_manager.rs:112`, `create_action_receipt`), and the callee's return value is
turned into the `Data` receipt that resolves the dependency.

Since PV 87 the *combined* size of a receipt's resolved promise inputs is bounded:
`apply_action_receipt` sums `get_received_data_size` over `input_data_ids`
(`lib.rs:847-859`) and, if the total exceeds
`max_receipt_total_input_size` (4_194_944 at PV 87), drops all the input data and
fails the receipt with `ActionErrorKind::TotalPromiseInputSizeExceeded`
(`lib.rs:861-868`, `:916-923`). See
[ReceiptPromiseInputSizeLimit](#protocol-version-gated-behavior) below.

The **congestion cost** of a receipt is defined by `compute_receipt_congestion_gas`
(`congestion_control.rs:678`): for action receipts it sums prepaid exec fees, the
`new_action_receipt` fee, prepaid send fees, and attached function-call gas
(`action_receipt_congestion_gas`, `:716`). `Data`, `PromiseYield`, `PromiseResume`,
and `GlobalContractDistribution` all count as **zero** congestion gas
(`:687-712`) — the MVP does not charge them (data/postponed costs would require extra
trie lookups). Size is the borsh length of the whole receipt (`compute_receipt_size`,
`:964`); for a receipt read back out of state the precomputed
`StateStoredReceiptMetadata.congestion_size` is used instead (`receipt_size`, `:946`).

### 4. Forwarding vs buffering (congestion + bandwidth admission)

Every outgoing receipt goes through `ReceiptSink::forward_or_buffer_receipt`
(`congestion_control.rs:162` → `:292`). It computes the receipt's receiver shard,
size, and congestion gas, then calls `try_forward` (`:403`):

1. If `size > max_receipt_size`, size is clamped to `max_receipt_size` for the limit
   comparison (bug workaround for oversized receipts, issue #12606, `:417`).
2. The receiver's `OutgoingLimit` is looked up; a missing entry defaults to
   `{ gas: Gas::MAX, size: 0 }` (`:433-441`) — since the bandwidth scheduler, a shard
   may send **zero** bytes on a link with no grant.
3. Under `ClampOutgoingGasAdmission` (PV 85) the *admission* gas is clamped to
   `allowed_shard_outgoing_gas` (`:443-449`), so a single very-expensive receipt cannot
   be blocked forever by the gas limit; pre-85 the full receipt gas is used.
4. Forward iff `forward_limit.gas >= admission_gas && forward_limit.size >= size`
   (`:451`); then the receipt is pushed to `outgoing_receipts` and the limit is
   decremented by the *actual* gas (saturating) and size (`:453-455`). Otherwise it is
   returned `NotForwarded` and `buffer_receipt` (`:466`) pushes it onto the outgoing
   buffer for that shard, growing `own_congestion_info` by its size and buffered gas.

At the **start** of each chunk `ReceiptSink::new` (`congestion_control.rs:86`) builds
the per-receiver limits: gas comes from the *receiver's* congestion control
`outgoing_gas_limit(own_shard)` (`:105`) — for same-shard receipts it is `Gas::MAX`
(`:110`, backpressure would not help); size comes from the bandwidth grant for the
`(own_shard, receiver)` link (`:113-115`). Before processing new receipts, buffered
receipts from previous chunks are drained first: `forward_from_buffer` (`:236`) walks
parent-shard buffers (resharding leftovers, `:257`) then current-layout buffers
(`:269`), forwarding each via `forward_from_buffer_to_shard` (`:338`) → `try_forward`
and shrinking `own_congestion_info` as receipts leave.

### 5. Congestion control math (NEP-539)

`CongestionControl::congestion_level` (`congestion_info.rs:44`) is the **max** of four
fractions, each clamped to [0,1] (`clamped_f64_fraction`, `:474`):

| Dimension | Formula | Parameter value at PV 87 |
| --- | --- | --- |
| incoming | `delayed_receipts_gas / max_congestion_incoming_gas` (`:331`) | 400 PGas (raised from 20 PGas at PV 73) |
| outgoing | `buffered_receipts_gas / max_congestion_outgoing_gas` (`:338`) | 10 PGas |
| memory | `receipt_bytes / max_congestion_memory_consumption` (`:345`) | 1_000_000_000 B |
| missed chunks | `missed_chunks_count / max_congestion_missed_chunks`, forced to 0 when `missed_chunks_count <= 1` (`:68`) | 125 (raised from 5 at PV 79) |

Values verified against `core/parameters/res/runtime_configs/parameters.snap:277-291`
(the snapshot of the stable config), with the deltas in `68.yaml`, `73.yaml`,
`74.yaml` and `79.yaml` (`68.yaml:13-16` sets incoming to 20 PGas, `73.yaml:6-9` raises it
to 400 PGas, `79.yaml` raises missed-chunks 5 → 125). `87.yaml` changes **no**
congestion-control or bandwidth-scheduler parameter. `localized_congestion_level` (`:324`) is the same max without
the missed-chunks term.

`outgoing_gas_limit(sender_shard)` (`:80`) drives how much a given sender may forward
to this shard next block:

- **Fully congested** (`congestion_level == 1.0`, `is_fully_congested`, `:95`): only
  the `allowed_shard` may send, and only `allowed_shard_outgoing_gas` (1 PGas); every
  other shard gets `Gas::ZERO` (`:83-89`). This is the "red light" that guarantees
  progress while stopping unbounded growth.
- Otherwise: linearly interpolate between `max_outgoing_gas` (300 PGas) and
  `min_outgoing_gas` (1 PGas) by the congestion level via `mix_gas` (`:91`, `:504`;
  `mix` at `:484` works in integer space to avoid f64 precision loss at high gas).

`outgoing_size_limit` (`:103`) gives the allowed shard `outgoing_receipts_big_size_limit`
(4_718_592 B) and everyone else `outgoing_receipts_usual_size_limit` (102_400 B). These
are advisory context, not the primary size gate — that is the bandwidth grant.

**Transaction admission**: `shard_accepts_transactions` (`:123`) rejects new
transactions targeting this shard once `congestion_level >=
reject_tx_congestion_threshold` (80/100 = 0.8 at PV 87, `parameters.snap:286`; the
check is the negated `<` at `:137`), reporting which of the four dimensions caused it
(`RejectTransactionReason`, `:161`). `process_tx_limit` (`:115`) throttles gas spent
converting new transactions to receipts by mixing `max_tx_gas` (500 TGas) and
`min_tx_gas` (20 TGas) with only the *incoming* congestion. Both are consulted from
`chain/chain/src/runtime/mod.rs:741`, `:1770` and `:1795`.

### 6. Finalizing per-shard congestion info

At the end of chunk apply (`lib.rs:2884-2901`): the delayed-queue deltas are folded in
via `apply_congestion_changes` (`lib.rs:2887`), then `finalize_allowed_shard`
(`congestion_info.rs:360`) picks the `allowed_shard` deterministically —
`get_new_allowed_shard` (`:370`) is `all_shards[congestion_seed % num_shards]`, where
`congestion_seed = block_height.wrapping_add(shard_index)` (`lib.rs:2896`). The result
becomes `ApplyResult.congestion_info` (`lib.rs:2991`), which
[sharding-chunks](sharding-chunks.md) writes into the chunk header;
`validate_extra_and_header` (`congestion_info.rs:204`) later checks the header matches
the chunk extra, comparing all four `V1` fields including `allowed_shard` (`:210`).
(The `// TODO(congestion_control) validate allowed shard` comment immediately above the
function, `:203`, is **stale** — the `allowed_shard` comparison it asks for is already on
line `:210`. Verified by reading the function body at HEAD; this settles the open question
carried by the 2.13.0 spec.)

`bootstrap_congestion_info` (`congestion_control.rs:743`) recomputes a shard's info
from scratch by iterating the delayed queue and every outgoing buffer — an
IO-intensive fallback used only when no prior `CongestionInfo` exists
(`own_congestion_info`, `lib.rs:3001`). It seeds `allowed_shard` with the shard's own
id (`congestion_control.rs:787`) so no knowledge of other shards is needed.

### 7. Bandwidth scheduler

Congestion control limits *gas*; the bandwidth scheduler limits *bytes per link*. It
runs once at the start of chunk apply — for **every** chunk, including missing ones
(`run_bandwidth_scheduler`, `bandwidth_scheduler/mod.rs:44`; called at `lib.rs:1910`,
before the missing-chunk early return at `:1918`) — and produces `GrantedBandwidth`
that seeds each `OutgoingLimit.size`. Determinism is essential: every shard runs the
identical algorithm on the identical inputs (bandwidth requests + shard statuses) and
must compute the same grants; a `sanity_check_hash` over the shard set is chained into
the persistent state to catch divergence (`mod.rs:127-129`), and the whole state is
hashed into `scheduler_state_hash` (`mod.rs:137`) which is carried in `ApplyResult`.

`BandwidthScheduler::run` (`scheduler.rs:200`) → `schedule_bandwidth` (`:303`) runs five
steps (the module doc, `scheduler.rs:62-73`, describes four — it does not count
`init_budgets`):

1. `init_budgets` (`:314`): every shard may send and receive at most
   `max_shard_bandwidth` (4_500_000 B).
2. `increase_allowances` (`:323`): each link gains `max_shard_bandwidth / num_shards`
   of *allowance* (fairness token bucket), capped at `max_allowance` (4_500_000 B,
   `:445`).
3. `grant_base_bandwidth` (`:340`): grant `base_bandwidth` on every allowed link
   unconditionally — small senders need no request. `base_bandwidth` is derived from
   `max_shard_bandwidth`, `max_single_grant` (4_194_304 B) and the shard count, capped
   at `max_base_bandwidth` (100_000 B) — see `BandwidthSchedulerParams::calculate`
   (`core/primitives/src/bandwidth_scheduler.rs:317`).
4. `process_bandwidth_requests` (`:347`): requests are bucketed by link allowance and
   served highest-allowance-first; ties within a bucket are shuffled with a
   seeded `ChaCha20Rng` (`:291`, seed = `prev_block_hash`, passed at `mod.rs:115`). Each
   granted increment decreases the sender/receiver budgets and the link allowance
   (`try_grant_bandwidth`, `:467`); the request is re-queued at its new (lower)
   allowance so large requests are deprioritized after each grant.
5. `distribute_remaining_bandwidth` (`:393`): fairly hand out any leftover budget to
   improve utilization (`bandwidth_scheduler/distribute_remaining.rs`).

`is_link_allowed` (`calculate_is_link_allowed`, `:506`) integrates with congestion
control so the two limits never deadlock: a link is forbidden if the receiver's status
is unknown (`:511-514`), if the **receiver's** last chunk was missing (`:516`), if the
**sender's** last chunk was missing (`:523`, it will not apply a chunk this height
anyway), or if the receiver is fully congested and the sender is not its allowed shard
(`:534`). This mirrors congestion control's red-light so that any granted bandwidth can
actually be used by at least one receipt (module doc, `scheduler.rs:75-90`).

**Generating requests**: at the end of apply, `generate_bandwidth_requests`
(`congestion_control.rs:503`, called `lib.rs:2903`) builds one `BandwidthRequest` per
receiver (`generate_bandwidth_request`, `:526`) from the *sizes of receipt groups*
still in that shard's outgoing buffer (`get_receipt_group_sizes_for_buffer_to_shard`,
`:571`), using `OutgoingMetadatas`. Requests for a child shard also fold in the
parent-shard buffer (`:545`) so post-resharding receipts aren't stranded. Sizes are
clamped to `max_receipt_size` (`:561-562`). Requests are consumed by the scheduler on the
following height (they reach apply as `apply_state.bandwidth_requests`, assembled from the
previous block's chunk headers at `lib.rs:3084-3106`), so the loop is: grant → forward-up-to-grant → generate new requests
→ repeat.

### 8. Promise yield / resume (parking and timeout)

A `PromiseYield` receipt, when processed, is simply stored keyed by
`(receiver_id, data_id)` and awaits resume (`process_receipt`, `lib.rs:1549-1552`,
`set_promise_yield_receipt`). Yields are **instant receipts** (`is_instant_receipt`,
`receipt.rs:473`) and confined to a single account, so they never cross shard
boundaries and count as zero congestion gas (`congestion_control.rs:696`).

A `PromiseResume` receipt (`lib.rs:1554`) delivers the awaited data. If it is a
*timeout* resume (`data: None`) and the yield status is `ResumeInitiated`, it is
dropped because a real resume already exists (`lib.rs:1555-1561`). Otherwise, if the
parked yield receipt is found, the runtime removes it and its status, cleans up the
`yield_id ↔ data_id` mapping when `YieldWithId` is active (`lib.rs:1577-1589`), stores
the `ReceivedData`, and executes the yield receipt immediately (`lib.rs:1591-1616`); a
second resume for the same `data_id` finds nothing and is ignored (`lib.rs:1617-1621`).

**Timeouts** are the last step of `process_receipts`:
`resolve_promise_yield_timeouts` (`lib.rs:3113`) walks the `PromiseYieldTimeout` queue
in `expires_at` order, stopping at the first entry with `expires_at >
block_height` (`:3146`) or once the compute/proof-size budget is hit (`:3130`). For
each expired entry whose yield still exists, it synthesizes a `PromiseResume` with
`data: None` destined for the same (local) account and forwards/buffers it
(`:3155-3201`); the timeout is then dequeued (`:3204-3208`). The timeout resume and any
real resume are ordered to the same shard, so a late timeout after a real resume is
simply discarded.

## Interactions

- **Consumes**: `apply_state.congestion_info` (`BlockCongestionInfo` for all shards,
  from the block's chunk headers), `apply_state.bandwidth_requests` (previous height's
  requests, assembled at `lib.rs:3084-3106`), and the persistent trie
  queues/scheduler state. Receipts to forward come from
  [runtime-execution](runtime-execution.md) (execution outputs, `lib.rs:1152`) and
  from the delayed/buffer queues.
- **Produces**: `ApplyResult.congestion_info`, `ApplyResult.bandwidth_requests` and
  `ApplyResult.bandwidth_scheduler_state_hash` (all committed to / checked against the
  chunk header by [sharding-chunks](sharding-chunks.md)), the
  `outgoing_receipts` vector (delivered to receiver shards), and mutated
  delayed/buffer/postponed/yield queues persisted via [state-storage](state-storage.md).
- **Touches**: [sharding-chunks](sharding-chunks.md) via `shard_accepts_transactions`
  and `process_tx_limit` (tx admission at chunk production,
  `chain/chain/src/runtime/mod.rs:741`, `:1770`, `:1795`), and via resharding —
  parent-shard buffer draining, delayed-receipt shard filtering, and cross-boundary
  bandwidth requests.
- **Delivery of `outgoing_receipts`** is [sharding-chunks](sharding-chunks.md)'s
  receipt-proof machinery (see that spec), not this component. Note that `from_shard_id` is *not*
  covered by `prev_outgoing_receipts_root`, so it is checked explicitly against the
  chunk header's shard when a partial encoded chunk is processed
  (`chain/chunks/src/shards_manager_actor.rs:1802`, added in #16136; the stateless- and
  SPICE-validation paths already did this).
- Receipt struct layout / borsh: [data-structures-serialization](data-structures-serialization.md).

## Protocol-version-gated behavior

Verified against `core/primitives-core/src/version.rs` in this tree
(`MIN_SUPPORTED_PROTOCOL_VERSION = 84` at `:652`, `STABLE_PROTOCOL_VERSION = 87` at
`:680`). Because the minimum supported version is 84, congestion control (old
`CongestionControl`, v68), stateless validation (v69), the `StateStoredReceipt` format
(v72), the `BandwidthScheduler` (v74), `IncreaseMaxCongestionMissedChunks` (v79, which
raised `max_congestion_missed_chunks` 5 → 125), `InstantPromiseYield` /
`YieldResumeImprovements` / `InstantDeleteAccount` (v83) and `Wasmtime` (v84) are all
**unconditionally active** at 2.14.0-rc.1 — their feature enum entries are now
`_Deprecated*` (`version.rs:244`, `:259`, `:274`, `:328`, and the `=> 83` / `=> 84`
arms at `:590-600`) and the code paths run without gating (e.g. `ReceiptSink` has only
the `V2` variant, `congestion_control.rs:28`).

Still-gated features touching this component:

| Feature | Activates | Effect |
| --- | --- | --- |
| `ClampOutgoingGasAdmission` | PV **85** (`version.rs:444`, `:615`) | In `try_forward`, the admission-gas comparison is clamped to `allowed_shard_outgoing_gas` (`congestion_control.rs:443`) so a single receipt above the per-shard outgoing gas limit can still be forwarded instead of stalling the buffer. Pre-85 the full receipt gas is required. Unchanged since 2.13.0. |
| `YieldWithId` | PV **85** (`version.rs:432`, `:612`) | On resume, cleans up the `yield_id ↔ data_id` mapping created by `promise_yield_create_with_id` (`lib.rs:1577-1589`). Pre-85 no such mapping exists. |
| `ExecutionMetadataV4` | PV **85** (`version.rs:429`, `:613`) | Execution of a forwarded/delayed action receipt emits `ExecutionMetadata::V4` with per-action contract info (`lib.rs:1253-1256`); pre-85 emits `V3`. Peripheral to routing/congestion. |
| `EnforcePerReceiptStorageProofLimit` | PV **86** (`version.rs:466`, `:618`) | Snapshots `recorded_storage_size_upper_bound()` at receipt start (`lib.rs:925-932`) and bounds the proof a single receipt records. Interacts with the `check_proof_size_limit_exceed` breaks that bound delayed-queue draining and yield-timeout processing (`congestion_control.rs:889`, `lib.rs:2592-2594`, `:3130`). |
| **`ReceiptPromiseInputSizeLimit`** | PV **87** (`version.rs:450`, `:625`) | **New at 87.** Bounds the combined borsh size of a receipt's resolved promise inputs by `max_receipt_total_input_size` (`lib.rs:847-869`). Over the limit, all input data is removed from state and the receipt fails with `ActionErrorKind::TotalPromiseInputSizeExceeded` instead of materializing the promise results. The parameter goes 4_294_967_295 → 4_194_944 at 87 (`core/parameters/res/runtime_configs/87.yaml`, `parameters.snap:249`) = `max_length_returned_data` (4 MiB) + `max_number_input_data_dependencies` (128) × 5 bytes of framing. |
| **`EnforceStorageProofLimitForAllActions`** | PV **87** (`version.rs:473`, `:624`) | **New at 87.** Extends the per-receipt storage-proof limit (`per_receipt_storage_proof_size_limit` = 4_000_000, `parameters.snap:13`) from `FunctionCall` to every action kind, checked after each action (`lib.rs:934-942`); failure is `ActionErrorKind::ReceiptStorageProofSizeExceeded`. Tightens the same proof budget that gates how many delayed receipts a chunk drains. |
| `max_state_init_entries` cap (**parameter-only**, no `ProtocolFeature`) | PV **87** | **New at 87.** `87.yaml` caps `max_state_init_entries` at 1_500 (old value 4_294_967_295; `parameters.snap:251`) *specifically* because each state-init entry costs 200 Ggas of `..._state_init_per_entry` execution fee, which lands in the receipt's congestion gas whether or not it is burnt; an uncapped count let one receipt reserve several times `max_congestion_outgoing_gas` (10 PGas) and pin its own shard at full outgoing congestion — stated verbatim in the `87.yaml` comment block. At 1_500 entries the receipt carries ~0.3 PGas, 3% of the cap. Enforced in `validate_number_of_state_init_entries` (`runtime/runtime/src/action_validation.rs:45`, error `ActionsValidationError::TotalNumberOfStateInitEntriesExceeded`), applied to `ValidateReceiptMode::NewReceipt` only (`:117-122`) so receipts already in flight at the upgrade keep executing. The cap is *not* gated on `UniversalAccounts` (PV 87, `version.rs:491`, `:629`): it is a plain runtime-config delta, and it tightens `DeterministicStateInit`, live since PV 82, as well. |

No *new* congestion-control or bandwidth-scheduler `ProtocolFeature` was added between
2.13.0 (v86) and 2.14.0-rc.1 (v87); `runtime/runtime/src/congestion_control.rs`,
`runtime/runtime/src/bandwidth_scheduler/`, `core/primitives/src/congestion_info.rs`
and `core/store/src/trie/receipts_column_helper.rs` are byte-identical to the 2.13.0
baseline — confirmed at verification time:
`git diff 499283a5..HEAD --stat -- runtime/runtime/src/congestion_control.rs
runtime/runtime/src/bandwidth_scheduler core/primitives/src/congestion_info.rs
core/store/src/trie/receipts_column_helper.rs` produces no output. All 87-related
movement in this component is parameter-side (`87.yaml`) or in `runtime/runtime/src/lib.rs`
and `action_validation.rs`.

**Not stable — nightly/SPICE only.** Under SPICE (`ProtocolFeature::Spice`, version
**180**, `version.rs:638`; not part of nightly's 157 either) chunk execution is
decoupled from block production, so the congestion inputs are *not* in the chunk
header. `spice_block_congestion_info` (`chain/chain/src/spice/chunk_application.rs:253`)
reconstructs a `BlockCongestionInfo` from the last certified block's executed
`ChunkExtra`s, and the chunk producer gates tx admission on it
(`chain/client/src/chunk_producer.rs:529`). Shards that are neither tracked nor yet
certified are omitted from the map. None of this affects mainnet/testnet at PV 87
(#15956, #15960).

**Deleted parameter configs.** `core/parameters/res/runtime_configs/{46,48,49,50,52}.yaml`
were removed (#15952, #15993, #16245) as a consequence of `MIN_SUPPORTED_PROTOCOL_VERSION`
moving to 84. None of them carried congestion-control or bandwidth-scheduler values —
those first appear in `68.yaml` (congestion control) and `74.yaml` (bandwidth
scheduler), both still present.

## Invariants & failure modes

- **Bounded incoming work**: a fully congested shard grants `Gas::ZERO` to all
  senders except its `allowed_shard` (`congestion_info.rs:83-89`), and the bandwidth
  scheduler forbids all links into it except the allowed one (`scheduler.rs:534`);
  together these stop unbounded delayed-queue growth while guaranteeing one sender can
  always make progress. Asserted by `test_missed_chunks_finalize`
  (`congestion_info.rs:814`) and the `test_*_congestion` tests
  (`:770` missed-chunks, `:622` memory, `:671` incoming, `:724` outgoing), and
  end-to-end by `test-loop-tests/src/tests/bandwidth_scheduler.rs` (which since #16386
  uses a dedicated contract rather than `rs_contract`).
- **Empty buffers ⇒ zero buffered gas**: after draining, if all outgoing buffers are
  empty the code asserts `own_congestion_info.buffered_receipts_gas() == 0`
  (`congestion_control.rs:282-283`).
- **Congestion accounting cannot underflow**: `CongestionInfo` add/remove use
  `checked_add`/`checked_sub` and return `RuntimeError::UnexpectedIntegerOverflow`
  (`congestion_info.rs:245-321`); `bootstrap_congestion_info` maps overflow to
  `StorageError::StorageInconsistentState` (`congestion_control.rs:973`,
  `overflow_storage_err`).
- **Deterministic cross-shard agreement**: `allowed_shard` is a pure function of
  `(block_height + shard_index) % num_shards` (`congestion_info.rs:370`,
  `lib.rs:2896`) and *is* validated header-vs-extra
  (`congestion_info.rs:210`); the bandwidth scheduler's `sanity_check_hash`
  (`mod.rs:127`) and `scheduler_state_hash` (`mod.rs:137`) detect any shard running
  with divergent inputs.
- **Oversized-receipt workaround**: receipts above `max_receipt_size` (4_194_304 B,
  `parameters.snap:244`) are treated as exactly `max_receipt_size` for both forwarding
  limits (`congestion_control.rs:417`) and bandwidth requests (`:561-562`) so they cannot
  get permanently stuck (issue #12606). `max_single_grant >= max_receipt_size` is
  asserted at parameter-derivation time
  (`core/primitives/src/bandwidth_scheduler.rs:325-332`), so a maximum-size receipt can
  always be sent in principle.
- **Congestion gas is bounded per receipt**: at PV 87 `max_state_init_entries` = 1_500
  (`parameters.snap:251`) caps the `..._state_init_per_entry` execution fees (200 Ggas
  each) a single receipt can reserve, enforced for newly created receipts in
  `validate_number_of_state_init_entries` (`action_validation.rs:45`, `:117-122`), keeping
  one receipt's contribution to `buffered_receipts_gas` at ~0.3 PGas = 3% of
  `max_congestion_outgoing_gas`.
- **Inconsistent-state failures**: a missing delayed/buffered/postponed/yield item
  referenced by an index yields `StorageError::StorageInconsistentState`
  (`receipts_column_helper.rs:112`, `lib.rs:3139`); a delayed receipt that fails
  `validate_receipt` on pop is likewise treated as inconsistent state, not a soft error
  (`lib.rs:2636`).
- **Backpressure knob**: transactions to a shard at or above
  `reject_tx_congestion_threshold` (0.8) congestion are rejected at production time
  (`congestion_info.rs:137`), surfacing as a congestion rejection to the submitter
  rather than being queued forever.

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `core/primitives/src/receipt.rs:566` | `ReceiptEnum` | Enumerates the seven receipt payload kinds with pinned borsh discriminants. |
| `core/primitives/src/receipt.rs:437` | `Receipt::receiver_shard_id` | Maps a receipt to its receiver shard (account id → shard, or `target_shard` resolved through split history). |
| `core/primitives/src/receipt.rs:473` | `Receipt::is_instant_receipt` | PromiseYield receipts, and action receipts whose only action is `DeleteAccount` *and* whose `input_data_ids` is empty, run immediately, off-queue. |
| `core/primitives/src/receipt.rs:133` | `STATE_STORED_RECEIPT_TAG` | Two-byte tag distinguishing `StateStoredReceipt` from plain `Receipt` on the trie. |
| `core/primitives/src/congestion_info.rs:44` | `CongestionControl::congestion_level` | max of incoming/outgoing/memory/missed-chunks fractions. |
| `core/primitives/src/congestion_info.rs:80` | `CongestionControl::outgoing_gas_limit` | Per-sender forward gas budget; red-light for full congestion. |
| `core/primitives/src/congestion_info.rs:123` | `CongestionControl::shard_accepts_transactions` | Tx admission gate at `reject_tx_congestion_threshold` (0.8). |
| `core/primitives/src/congestion_info.rs:204` | `CongestionInfo::validate_extra_and_header` | Header-vs-chunk-extra check; compares all four V1 fields incl. `allowed_shard` (`:210`). The `TODO(congestion_control)` at `:203` is stale. |
| `core/primitives/src/congestion_info.rs:360` | `CongestionInfo::finalize_allowed_shard` | Deterministic allowed-shard selection. |
| `core/primitives/src/bandwidth_scheduler.rs:317` | `BandwidthSchedulerParams::calculate` | Derives `base_bandwidth`; asserts `max_receipt_size <= max_single_grant <= max_shard_bandwidth`. |
| `runtime/runtime/src/congestion_control.rs:86` | `ReceiptSink::new` | Builds per-receiver gas (congestion) + size (bandwidth) limits. |
| `runtime/runtime/src/congestion_control.rs:236` | `ReceiptSinkV2WithInfo::forward_from_buffer` | Drains parent then current-layout outgoing buffers; asserts empty-buffer invariant. |
| `runtime/runtime/src/congestion_control.rs:403` | `ReceiptSinkV2::try_forward` | Forward-or-not decision incl. `ClampOutgoingGasAdmission` and oversize clamp. |
| `runtime/runtime/src/congestion_control.rs:466` | `ReceiptSinkV2::buffer_receipt` | Buffers a receipt, growing own congestion info. |
| `runtime/runtime/src/congestion_control.rs:503` | `ReceiptSinkV2::generate_bandwidth_requests` | Builds next-height requests from buffered receipt groups. |
| `runtime/runtime/src/congestion_control.rs:678` | `compute_receipt_congestion_gas` | Congestion gas per receipt kind (data/yield/resume/global-distribution = 0). |
| `runtime/runtime/src/congestion_control.rs:743` | `bootstrap_congestion_info` | IO-heavy recompute of `CongestionInfo` from queues. |
| `runtime/runtime/src/congestion_control.rs:802` | `DelayedReceiptQueueWrapper` | Delayed-queue push/pop with congestion delta accumulation + resharding filter. |
| `runtime/runtime/src/congestion_control.rs:964` | `compute_receipt_size` | Borsh length of a receipt; protocol-relevant, only for writes into state. |
| `core/store/src/trie/receipts_column_helper.rs:63` | `TrieQueue` | FIFO trie-backed queue for delayed & buffered receipts. |
| `runtime/runtime/src/bandwidth_scheduler/mod.rs:44` | `run_bandwidth_scheduler` | Per-chunk scheduler entry (also for missing chunks); reads/writes persistent state, sanity hash. |
| `runtime/runtime/src/bandwidth_scheduler/scheduler.rs:303` | `BandwidthScheduler::schedule_bandwidth` | Five-stage grant algorithm. |
| `runtime/runtime/src/bandwidth_scheduler/scheduler.rs:506` | `calculate_is_link_allowed` | Blocks links to congested / missing-chunk shards. |
| `runtime/runtime/src/lib.rs:2670` | `Runtime::process_incoming_receipts` | Executes or delays incoming receipts by compute/proof budget. |
| `runtime/runtime/src/lib.rs:2570` | `Runtime::process_delayed_receipts` | Drains the delayed queue FIFO under the same budget. |
| `runtime/runtime/src/lib.rs:1647` | `Runtime::process_action_receipt` | Postpones action receipts with unmet input data. |
| `runtime/runtime/src/lib.rs:1439` | `Runtime::process_receipt` (Data arm) | Resolves postponed receipts as data arrives. |
| `runtime/runtime/src/lib.rs:847` | `Runtime::apply_action_receipt` (input-size gate) | PV-87 `max_receipt_total_input_size` check on resolved promise inputs. |
| `runtime/runtime/src/lib.rs:3113` | `resolve_promise_yield_timeouts` | Times out parked yields via synthetic PromiseResume. |
| `runtime/runtime/src/lib.rs:2884-2903` | apply finalization | Applies congestion deltas, picks allowed shard, generates bandwidth requests. |
| `runtime/runtime/src/action_validation.rs:45` | `validate_number_of_state_init_entries` | PV-87 cap keeping one receipt's congestion gas bounded. |
| `core/primitives-core/src/version.rs:444`,`:450`,`:466`,`:473` | `ClampOutgoingGasAdmission`, `ReceiptPromiseInputSizeLimit`, `EnforcePerReceiptStorageProofLimit`, `EnforceStorageProofLimitForAllActions` | PV 85 / 87 / 86 / 87 gates relevant here (match arms at `:615`, `:625`, `:618`, `:624`). |
| `core/parameters/res/runtime_configs/parameters.snap:277-291` | congestion + bandwidth params | Authoritative stable values quoted throughout this spec. |

## Open questions

- The `PromiseYieldTimeout` queue *entries* are created during action execution
  (yield host function), which lives in [runtime-execution](runtime-execution.md); the
  exact `expires_at = block_height + timeout_length` computation was not traced here
  and is out of scope for this spec.
- `max_congestion_incoming_gas` is 400 PGas while `max_outgoing_gas` is 300 PGas, so
  the incoming dimension alone can essentially never reach 1.0 from a single block's
  forwarding; whether the 20 → 400 PGas raise at PV 73 was intended to make incoming
  congestion effectively advisory (leaving memory and missed-chunks as the binding
  dimensions) is not determinable from code alone.
- `mix`/`mix_gas` (`congestion_info.rs:484`, `:504`) interpolate in integer space using
  an `f64` ratio derived from `clamped_f64_fraction`. The rounding is deterministic for
  a given input, but no test pins the exact result at the boundaries across platforms;
  cross-platform f64 determinism here is assumed rather than verified.
