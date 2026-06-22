# Cross-shard communication & congestion control

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `runtime/runtime/src/congestion_control.rs`, `runtime/runtime/src/bandwidth_scheduler/`, `core/primitives/src/congestion_info.rs`, `core/primitives/src/bandwidth_scheduler.rs`, `core/store/src/trie/receipts_column_helper.rs`, `core/primitives/src/receipt.rs`

## Role

This component governs how *work crosses shard boundaries* and how that flow is throttled. Receipts are the unit of cross-shard communication: when [runtime-execution](runtime-execution.md) executes a chunk it produces outgoing receipts addressed to accounts that may live on other shards. This component decides, for each outgoing receipt, whether to **forward** it now (into the chunk's `outgoing_receipts`, which become incoming receipts of the receiver shard next height) or **buffer** it in the trie for a later chunk, and it maintains the persistent receipt queues (delayed, buffered, postponed, promise-yield) that decouple production from consumption. Two regulators sit on top: **congestion control** (NEP-539) caps how much *gas* a sender shard may forward to a given receiver based on that receiver's published `CongestionInfo`, and the **bandwidth scheduler** caps how many *bytes* of receipts each shard-to-shard link may carry per height. Congestion info is computed during `Runtime::apply`, committed in the chunk header by [sharding-chunks](sharding-chunks.md) / [chain-block-processing](chain-block-processing.md), and read back at the next apply. Receipt struct layout is owned by [data-structures-serialization](data-structures-serialization.md); receipt *production/execution* is owned by [runtime-execution](runtime-execution.md) — this spec covers only routing, queuing, and the two throttles.

## Key data structures

- **`Receipt` / `ReceiptEnum`** — `core/primitives/src/receipt.rs:73` / `:566` — the cross-shard message. Only `Receipt::V0` (`ReceiptV0`, `:56`) exists. `ReceiptEnum` variants (discriminants are protocol-stable): `Action(ActionReceipt)=0`, `Data(DataReceipt)=1`, `PromiseYield(ActionReceipt)=2`, `PromiseResume(DataReceipt)=3`, `GlobalContractDistribution=4`, `ActionV2(ActionReceiptV2)=5`, `PromiseYieldV2(ActionReceiptV2)=6` (`:566`). The receipt kinds and their queues are enumerated in *Behavior*.
- **`ActionReceipt` / `ActionReceiptV2`** — `core/primitives/src/receipt.rs:590` / `:622` — carries `signer_id`, `signer_public_key`, `gas_price`, `output_data_receivers: Vec<DataReceiver>`, `input_data_ids: Vec<CryptoHash>`, `actions`. `input_data_ids` are the data dependencies that must arrive before the receipt can execute; `output_data_receivers` say where to send this receipt's return value as `Data` receipts. `ActionReceiptV2` adds `refund_to` (`:626`).
- **`DataReceipt`** — `core/primitives/src/receipt.rs:827` — `{ data_id, data: Option<Vec<u8>> }`. `data == None` encodes a failed promise result. Carried by `Data` and `PromiseResume` variants.
- **`DataReceiver`** — `core/primitives/src/receipt.rs:37` — `{ data_id, receiver_id }`: the address+id a data receipt is routed to.
- **`ReceivedData`** — `core/primitives/src/receipt.rs:848` — `{ data: Option<Vec<u8>> }` stored in the trie under `TrieKey::ReceivedData` until all of a postponed receipt's `input_data_ids` are satisfied.
- **`PromiseYieldTimeout` / `PromiseYieldIndices`** — `core/primitives/src/receipt.rs:1084` / `:1069` — FIFO queue entry `{ account_id, data_id, expires_at: BlockHeight }` and its `{first_index, next_available_index}` indices; orders parked yields by expiry.
- **`CongestionInfo` (enum, only `V1`)** — `core/primitives/src/congestion_info.rs:187` — the per-shard congestion summary that lives in the chunk header. `CongestionInfoV1` (`:460`) fields: `delayed_receipts_gas: u128`, `buffered_receipts_gas: u128`, `receipt_bytes: u64` (borsh size of all receipts held in state — delayed, buffered, postponed, yielded), `allowed_shard: u16` (the single shard permitted to forward when fully congested). Versioned and immutable per version by design (`:168` doc).
- **`CongestionControl`** — `core/primitives/src/congestion_info.rs:18` — wraps `(config, CongestionInfo, missed_chunks_count)` and is the decision engine a *sender* uses against a *receiver's* published info. Not needed to build one's own info (`:13` doc).
- **`BlockCongestionInfo` / `ExtendedCongestionInfo`** — `core/primitives/src/congestion_info.rs:390` / `:434` — `BTreeMap<ShardId, ExtendedCongestionInfo>` for all shards in the block; `ExtendedCongestionInfo = { congestion_info, missed_chunks_count }`. The `BTreeMap` ordering is consensus-relevant because `allowed_shard` selection depends on shard ordering (`:392` doc). Passed into apply via `ApplyState.congestion_info`.
- **`ReceiptSink` / `ReceiptSinkV2`** — `runtime/runtime/src/congestion_control.rs:28` / `:58` — the per-apply object that forwards or buffers outgoing receipts. Holds `own_congestion_info` (this shard's *new* info being built — not used for this chunk's forwarding decisions), `outgoing_receipts: Vec<Receipt>`, `outgoing_limit: HashMap<ShardId, OutgoingLimit>`, `outgoing_buffers: ShardsOutgoingReceiptBuffer`, `outgoing_metadatas`, `bandwidth_scheduler_output`, `stats`.
- **`OutgoingLimit`** — `runtime/runtime/src/congestion_control.rs:74` — `{ gas: Gas, size: u64 }`: per-receiver-shard remaining budget. `gas` from congestion control, `size` from the bandwidth scheduler grant.
- **`DelayedReceiptQueueWrapper`** — `runtime/runtime/src/congestion_control.rs:802` — wraps `DelayedReceiptQueue` and accumulates `new_delayed_gas/bytes` and `removed_delayed_gas/bytes` so `CongestionInfo` is updated once at finalize via `apply_congestion_changes` (`:931`). Also filters receipts not belonging to the current shard after resharding (`receipt_filter_fn`, `:874`).
- **`DelayedReceiptQueue` / `ShardsOutgoingReceiptBuffer` / `OutgoingReceiptBuffer`** — `core/store/src/trie/receipts_column_helper.rs:30` / `:39` / `:52` — type-safe trie FIFO queues. All implement the `TrieQueue` trait (`:63`) giving `push_back`/`pop_front`/`pop_back`/`pop_n`/`iter`, with in-memory cached indices written back on every mutation. Delayed queue is keyed by `TrieKey::DelayedReceipt{index}` (`:281`); outgoing buffers by `TrieKey::BufferedReceipt{index, receiving_shard}` (`:332`), one logical sub-queue per receiver shard.
- **`ReceiptOrStateStoredReceipt` / `StateStoredReceipt`** — `core/primitives/src/receipt.rs` (`StateStoredReceipt` at `:96`) — the in-state representation. When `config.use_state_stored_receipt` is set, queued receipts carry `StateStoredReceiptMetadata { congestion_gas, congestion_size }` so gas/size need not be recomputed when popped (`congestion_control.rs:475`, `:851`).
- **`BandwidthSchedulerParams` / `BandwidthRequest` / `GrantedBandwidth`** — `core/primitives/src/bandwidth_scheduler.rs:287` / `:74` / `scheduler.rs:158` — scheduler config (`base_bandwidth`, `max_shard_bandwidth`, `max_single_grant`, `max_receipt_size`, `max_allowance`), a per-link byte request (a bitmap over `BANDWIDTH_REQUEST_VALUES_NUM = 40` predefined values, `:131`), and the resulting `BTreeMap<(sender,receiver), Bandwidth>` of granted bytes.

## Behavior

### 1. Receipt kinds and the queue each lives in

| Kind | Created by | Where it can rest | Resolved/executed when |
|---|---|---|---|
| **Action** (`Action`/`ActionV2`) | tx conversion or a contract creating a promise | delayed queue (over gas limit) or postponed (waiting on data) | all `input_data_ids` present and gas budget allows |
| **Data** (`Data`) | an action receipt finishing, sending its return to `output_data_receivers` | (transient) — stored as `ReceivedData` in trie keyed by `data_id` | delivered immediately on arrival; may unblock a postponed receipt |
| **PromiseYield** | a contract calling `promise_yield_create` | stored under `TrieKey::PromiseYieldReceipt`; an entry is added to the promise-yield timeout queue | a matching `PromiseResume` arrives, or timeout fires |
| **PromiseResume** | `promise_yield_resume`, or a timeout (data `None`) | delayed queue possible | matched against the stored yield receipt |
| **GlobalContractDistribution** | global-contract deploy | n/a (handled by global-contract code) | on receipt |
| **(buffered)** — any outgoing receipt | the `ReceiptSink` when over the receiver's gas/size limit | outgoing buffer for the receiver shard | forwarded from the buffer in a later chunk |
| **(delayed)** — any incoming/local receipt | runtime when over the chunk gas/compute limit | delayed queue | popped FIFO next chunk |

The persistent queues live in the trie: the **delayed** queue (`DelayedReceiptQueue`) holds incoming/local receipts that did not fit under this chunk's gas limit; the **outgoing buffers** (`ShardsOutgoingReceiptBuffer`, one per receiver shard) hold receipts congestion/bandwidth would not let us forward yet; **postponed** action receipts and their `ReceivedData` live under `TrieKey::PostponedReceipt*` / `ReceivedData`; **yielded** promises live under `TrieKey::PromiseYieldReceipt` plus the timeout queue. All four feed `CongestionInfo.receipt_bytes`.

**Receipt movement.** A newly produced outgoing receipt is either *forwarded* or *buffered* (step 3). A *local/incoming* receipt that cannot run under the gas/compute limit is *pushed onto the delayed queue* by [runtime-execution](runtime-execution.md). An *action receipt missing data* is stored *postponed*; a *data receipt* that completes the last missing dependency moves the postponed receipt back to *execution* (step 2). A *buffered* receipt moves to *forwarded* once limits permit (step 4). A *delayed* receipt is popped FIFO and executed.

### 2. Data dependencies (promises) and their resolution

A cross-contract call is modeled as an action receipt with `input_data_ids` it waits on, plus `output_data_receivers` telling where its own result goes. The wiring is built in `receipt_manager.rs` ([runtime-execution](runtime-execution.md)); resolution happens in `process_receipt` / `process_action_receipt`:

1. When an **action receipt** arrives, `process_action_receipt` counts how many `input_data_ids` are not yet present in state (`runtime/runtime/src/lib.rs:1529`). If `pending_data_count == 0` it executes now; otherwise it stores the receipt as a **postponed** receipt (`set_postponed_receipt`, `:1575`), writes a `PostponedReceiptId` link for each missing `data_id` (`:1537`), and stores the `PendingDataCount` (`:1568`).
2. When a **data receipt** arrives, `process_receipt` stores `ReceivedData{data}` keyed by `data_id` (`set_received_data`, `:1310`), then looks for a postponed receipt awaiting that `data_id` via `PostponedReceiptId` (`:1321`). If found and its `PendingDataCount == 1` (this was the last dependency), it removes the count + link, loads the postponed receipt, and executes it (`:1342`–`:1360`); otherwise it decrements `PendingDataCount` (`:1386`).
3. At execution, `apply_action_receipt` collects each input's `ReceivedData`, removing it from state; `data: Some` becomes `PromiseResult::Successful`, `data: None` becomes `Failed` ([runtime-execution](runtime-execution.md)). A missing entry is a fatal `StorageInconsistentState`.

### 3. Forwarding vs buffering an outgoing receipt — `forward_or_buffer_receipt`

`ReceiptSink::forward_or_buffer_receipt` (`runtime/runtime/src/congestion_control.rs:162`, impl at `:292`) is called for every outgoing receipt produced this chunk:

1. Compute the receiver shard from `receipt.receiver_shard_id(shard_layout)`, the receipt's borsh `size` (`compute_receipt_size`, `:964`), and its **congestion gas** (`compute_receipt_congestion_gas`, `:678`).
2. Call `try_forward` (`:403`). It looks up `outgoing_limit[shard]` (default `OutgoingLimit { gas: Gas::MAX, size: 0 }` if unknown — unknown gas means "no limit", but **unknown size means 0**: since the bandwidth scheduler, a shard may send nothing without a grant, `:434`–`:441`). It clamps oversized receipts to `max_receipt_size` to avoid stuck receipts (bug #12606, `:417`). Under `ClampOutgoingGasAdmission` (enabled at 86) the *admission* gas is `gas.min(config.allowed_shard_outgoing_gas)` — so a single huge receipt can always be admitted if the gas budget is at least one allowed-shard quantum (`:443`); pre-feature it used the raw `gas`.
3. **Forward** iff `forward_limit.gas >= admission_gas && forward_limit.size >= size`: push to `outgoing_receipts`, decrement both budgets (`gas` by the full `gas`, `size` by `size`) (`:451`). **Otherwise buffer**: `buffer_receipt` (`:466`) wraps it (optionally as a `StateStoredReceipt` with cached gas/size), bumps `own_congestion_info` by `add_receipt_bytes(size)` + `add_buffered_receipt_gas(gas)` (`:486`), updates outgoing metadata, and `push_back`s it onto the receiver's outgoing buffer (`:498`).

**Congestion-gas accounting** (`compute_receipt_congestion_gas`, `:678`): an action receipt counts `total_prepaid_exec_fees + new_action_receipt exec fee + total_prepaid_send_fees + total_prepaid_gas` (`action_receipt_congestion_gas`, `:716`) — i.e. all gas guaranteed plus attached function-call gas. **Data, PromiseYield, PromiseResume, and GlobalContractDistribution receipts contribute `Gas::ZERO`** (`:687`–`:712`): the congestion MVP does not account for them (data/yield/resume gas is hard to find without extra trie lookups; yields never cross shards).

### 4. Draining the outgoing buffers — `forward_from_buffer`

Run **first**, before processing transactions, so leftover receipts from previous chunks get priority on this chunk's freshly granted budget (`forward_from_buffer`, `runtime/runtime/src/congestion_control.rs:236`). For each shard — first any **parent**-shard buffers (post-resharding leftovers), then current-layout shards (`:257`, `:270`) — `forward_from_buffer_to_shard` (`:338`) iterates the buffer in FIFO order calling `try_forward`; on each success it `remove_receipt_bytes`/`remove_buffered_receipt_gas` from `own_congestion_info` and counts the receipt; on the **first** `NotForwarded` it `break`s (FIFO, no skipping). It then `pop_n`s exactly the forwarded prefix off the trie buffer (`:385`). An assertion ties empty buffers to zero `buffered_receipts_gas` (`:282`).

### 5. Congestion control — what the receiver publishes and how the sender reacts

A shard's `CongestionInfo` is rebuilt every chunk. The four pressure signals, each a fraction clamped to `[0,1]` by `clamped_f64_fraction` (`core/primitives/src/congestion_info.rs:474`):
- **incoming** = `delayed_receipts_gas / max_congestion_incoming_gas` (`:331`)
- **outgoing** = `buffered_receipts_gas / max_congestion_outgoing_gas` (`:338`)
- **memory** = `receipt_bytes / max_congestion_memory_consumption` (`:345`)
- **missed-chunks** = `missed_chunks_count / max_congestion_missed_chunks`, but `0` if `count <= 1` (`CongestionControl::missed_chunks_congestion`, `:68`)

`congestion_level` is the **max** of the four (`:44`). A *sender* shard, holding the receiver's published `CongestionInfo`, computes how much gas it may forward via `outgoing_gas_limit(sender_shard)` (`:80`):
- If `is_fully_congested` (`level == 1.0`, `:95`): **red light** — `Gas::ZERO` for everyone *except* the receiver's chosen `allowed_shard`, which gets `allowed_shard_outgoing_gas` (`:83`). This guarantees liveness: exactly one sender keeps trickling.
- Otherwise: `mix_gas(max_outgoing_gas, min_outgoing_gas, level)` — linear interpolation (`mix`, `:484`); higher congestion ⇒ closer to `min_outgoing_gas`.

These per-receiver gas limits are loaded into the sink's `outgoing_limit` in `ReceiptSink::new` (`congestion_control.rs:95`); the **same shard** (receiver == self) gets `Gas::MAX` — backpressure on yourself is pointless (`:107`).

**Transaction admission** is a separate gate: `shard_accepts_transactions` (`congestion_info.rs:123`) rejects new transactions whose receiver is this shard once `congestion_level >= reject_tx_congestion_threshold` (0.8 in the shipped config), returning a typed `RejectTransactionReason` (`:161`). `process_tx_limit` (`:115`) further caps gas accepted for new txs to uncongested shards via `mix_gas(max_tx_gas, min_tx_gas, incoming_congestion)`. (Transaction processing/validation is in [runtime-execution](runtime-execution.md); these limits are consulted by [chain-block-processing](chain-block-processing.md) when selecting transactions.)

**`allowed_shard` selection** — `finalize_allowed_shard` (`congestion_info.rs:360`) is called once at finalize with a `congestion_seed = block_height.wrapping_add(shard_index)` (`runtime/runtime/src/lib.rs:2711`). It picks `all_shards[seed % all_shards.len()]` — deterministic round-robin so that, under full congestion, a different sender is favored each height (`get_new_allowed_shard`, `:370`). The allowed shard also gets the larger `outgoing_receipts_big_size_limit` for *size* (`outgoing_size_limit`, `:103`).

**Building / bootstrapping info.** During apply the sink mutates `own_congestion_info` (buffer push/pop) and the `DelayedReceiptQueueWrapper` accumulates delayed-queue deltas, applied once via `apply_congestion_changes` at finalize (`:2702`). On cold start, `bootstrap_congestion_info` (`congestion_control.rs:743`) scans the entire delayed queue and all outgoing buffers to compute gas+bytes (IO-intensive, only for bootstrap; thereafter info is carried chunk-to-chunk in headers). `validate_extra_and_header` (`congestion_info.rs:204`) checks the chunk header's info matches the chunk-extra (gas/bytes/allowed_shard) — note the TODO that `allowed_shard` validation is incomplete.

### 6. Bandwidth scheduler — byte-rate limiting across links

The scheduler limits the *size* of receipts on each `(sender, receiver)` link, complementing congestion control's gas limit (`bandwidth_scheduler/scheduler.rs:1` module doc). `run_bandwidth_scheduler` (`bandwidth_scheduler/mod.rs:44`) runs **for every shard including missing chunks**, early in apply (before forwarding), so the granted size budget is known when the sink is built:

1. Load `BandwidthSchedulerState` from the trie (link allowances + a sanity-check hash); initialize empty if absent (`:59`).
2. Build a `ShardStatus` per shard from `ApplyState.congestion_info`: `last_chunk_missing`, `allowed_sender_shard_index`, `is_fully_congested` (`:74`).
3. Compute `BandwidthSchedulerParams` from `num_shards` + config (`:99`).
4. `BandwidthScheduler::run` (`scheduler.rs:200`) with the previous block's `BandwidthRequests`, the shard statuses, and `prev_block_hash` as RNG seed. The algorithm (module doc `scheduler.rs:60`): (a) credit each link a fair share of **allowance**; (b) grant **base bandwidth** on every link; (c) process requests in **descending-allowance order** (ties broken by the seeded `ChaCha20Rng`), granting the next requested value if sender/receiver budgets allow, then re-queuing the request with reduced allowance; (d) distribute leftover bandwidth fairly. Links to **fully-congested** receivers get nothing except from that receiver's `allowed_shard` — this keeps gas and byte limits consistent for liveness (`scheduler.rs:75` doc). Links to shards whose **last chunk was missing** get nothing (`scheduler.rs:92` doc).
5. Update + persist the sanity-check hash (every shard must run with identical inputs) and the scheduler state under `StateChangeCause::BandwidthSchedulerStateUpdate` (`mod.rs:124`–`:135`).

The result `GrantedBandwidth` feeds each `OutgoingLimit.size` via `get_granted_bandwidth(self, receiver)` in `ReceiptSink::new` (`congestion_control.rs:113`).

**Generating requests for next height** — `generate_bandwidth_requests` (`congestion_control.rs:503`), at finalize. For each receiver shard it reads the (group) sizes of receipts in that shard's outgoing buffer and calls `BandwidthRequest::make_from_receipt_sizes` (`bandwidth_scheduler.rs:84`): it accumulates running totals and sets a bit for the smallest predefined value ≥ each cumulative size; sizes ≤ `base_bandwidth` need no request (`:103`). Receipts to a **child** shard also fold in the **parent** shard's buffer so a too-small child grant can't strand a parent receipt (`:545`). Sizes are clamped to `max_receipt_size` (bug #12606 again, `:561`). If outgoing-buffer metadata is not yet fully initialized (e.g. right after the metadata upgrade), a *basic* request of just `max_receipt_size` is made to guarantee liveness (`:585`).

### 7. Promise yield / resume and timeouts

A `PromiseYield` receipt parks a promise on a single account (it never crosses shards) and registers a timeout. `process_receipt` stores it under `TrieKey::PromiseYieldReceipt` (`set_promise_yield_receipt`, `runtime/runtime/src/lib.rs:1419`). A `PromiseResume` (from `promise_yield_resume`, or a timeout) carries the awaited `data_id`; `process_receipt` matches it to the stored yield, cleans up `yield_id`↔`data_id` mappings (under `YieldWithId`), stores the data, and executes the yield's action receipt ([runtime-execution](runtime-execution.md)).

**Timeout resolution** — `resolve_promise_yield_timeouts` (`runtime/runtime/src/lib.rs:2930`), run last in receipt processing. It walks the `PromiseYieldIndices` FIFO queue (entries ordered by `expires_at`); it stops at the first entry with `expires_at > block_height` or when over the compute/proof limit (`:2947`, `:2963`). For an expired entry that still has a live `PromiseYieldReceipt`, it creates a `PromiseResume` receipt with `data: None` (a "failed" resume), routed back to the same account via `forward_or_buffer_receipt` (`:2981`–`:3018`). If a real resume already ran this chunk, the timeout resume is later **dropped** as a duplicate when processed (the matching yield is already gone). Processed entries are removed and `processed_yield_timeouts` is returned for the `ApplyResult`.

## Interactions

- **Consumes**: outgoing `Receipt`s and the delayed/incoming receipt streams from [runtime-execution](runtime-execution.md); the block's `BlockCongestionInfo` and previous-height `BandwidthRequests` from `ApplyState` (committed in chunk headers by [sharding-chunks](sharding-chunks.md) / [chain-block-processing](chain-block-processing.md)); the trie via `TrieAccess`/`TrieUpdate` → [state-storage](state-storage.md); shard layout from the `EpochInfoProvider` ([epoch-validators-staking](epoch-validators-staking.md)).
- **Produces**: the chunk's `outgoing_receipts` (delivered to receiver shards next height), the new `CongestionInfo` and `BandwidthRequests` placed in `ApplyResult` and committed to the next chunk header, and the persisted trie queues (delayed, buffered, postponed, yield + scheduler state).
- **Routing entry point** is `ReceiptSink::forward_or_buffer_receipt`, called from `apply_action_receipt` and tx conversion in [runtime-execution](runtime-execution.md).
- Receipt struct/serialization details (e.g. `StateStoredReceipt` borsh) → [data-structures-serialization](data-structures-serialization.md). Trie-key/queue persistence mechanics → [state-storage](state-storage.md).

## Protocol-version-gated behavior

Verified against `core/primitives-core/src/version.rs:453`. Congestion control, the bandwidth scheduler, state-stored receipts, instant promise yield, and the original yield-execution feature are all **long-deprecated/baked-in** (their `_Deprecated*` gates fall below the minimum supported version, so the behavior is unconditional at 86). The features still observable in code paths touching this component:

| Feature | Activates | Effect on this component |
|---|---|---|
| `ClampOutgoingGasAdmission` | 85 (enabled) | In `try_forward`, the admission gas is `gas.min(config.allowed_shard_outgoing_gas)` instead of the raw receipt gas, so an oversized receipt can always be admitted given at least one allowed-shard gas quantum — prevents stuck large receipts. `congestion_control.rs:443` |
| `YieldWithId` | 85 (enabled) | Maintain/clean up `yield_id ↔ data_id` mappings when resolving a `PromiseResume` against a stored yield. `lib.rs:1445` (gated cleanup in `process_receipt` PromiseResume arm) |
| NEP-539 congestion control (`_DeprecatedCongestionControl`) | 68 (baked in) | The entire `CongestionInfo` / `outgoing_gas_limit` / delayed+buffered queue machinery. `congestion_info.rs`, `congestion_control.rs` |
| Bandwidth scheduler (`_DeprecatedBandwidthScheduler`) | 74 (baked in) | Per-link byte limits; before it, only gas limits applied and `default_size_limit` was effectively unbounded — now the no-grant default size is `0`. `bandwidth_scheduler/`, `congestion_control.rs:437` |
| State-stored receipts (`_DeprecatedStateStoredReceipt`) | 72 (baked in) | Queued receipts carry cached `congestion_gas`/`congestion_size`; gated at runtime by `config.use_state_stored_receipt`. `congestion_control.rs:475` |
| Increase max congestion missed chunks (`_DeprecatedIncreaseMaxCongestionMissedChunks`) | 79 (baked in) | Raises `max_congestion_missed_chunks` parameter feeding `missed_chunks_congestion`. `congestion_info.rs:68` |
| `FixContractLoadingError` | 86 (enabled) | No effect here; realized in [contract-vm](contract-vm.md). |

## Invariants & failure modes

- **Forwarding is FIFO and non-skipping**: `forward_from_buffer_to_shard` breaks on the first non-forwardable receipt and pops exactly the forwarded prefix (`congestion_control.rs:379`, `:385`); order within a buffer is preserved.
- **Empty buffer ⇔ zero buffered gas**: asserted when all buffers drain (`congestion_control.rs:282`).
- **No grant ⇒ no bytes**: with the bandwidth scheduler, an undefined size limit defaults to `0`; a shard cannot send to a link it has no grant on (`congestion_control.rs:437`). The scheduler guarantees every nonzero grant can carry ≥ 1 receipt (`scheduler.rs:84` doc).
- **Liveness under full congestion**: a fully-congested receiver still admits `allowed_shard_outgoing_gas` from exactly one rotating `allowed_shard` (`congestion_info.rs:83`), and the bandwidth scheduler mirrors this by only granting bytes from that shard.
- **Gas/byte limit consistency**: links to fully-congested or last-chunk-missing receivers get zero byte grants, preventing a grant that no gas could be sent against (`scheduler.rs:75`, `:92` docs).
- **Same-shard receipts are unthrottled**: gas limit `Gas::MAX` for receiver == self (`congestion_control.rs:107`) — backpressure on oneself would only move bytes between queues.
- **Oversized-receipt workaround**: receipts above `max_receipt_size` are treated as exactly `max_receipt_size` for both forwarding and request generation so they can never get permanently stuck (bug #12606; `congestion_control.rs:417`, `:561`).
- **Determinism**: `BlockCongestionInfo` is a `BTreeMap` because `allowed_shard` selection depends on shard ordering (`congestion_info.rs:392`); the scheduler hashes its inputs into a per-shard sanity-check hash so divergent inputs across validators are detectable (`mod.rs:120`).
- **State-inconsistency = hard error**: a queue index pointing at a missing trie item, a missing `PromiseYieldTimeout` queue entry, or arithmetic overflow on gas/byte counters all return `StorageError::StorageInconsistentState` / `RuntimeError::UnexpectedIntegerOverflow` (`receipts_column_helper.rs:112`, `lib.rs:2956`, `congestion_info.rs:248`).
- **Resharding handling is partial**: the scheduler grants are "slightly wrong (but within limits)" exactly on the resharding boundary where senders and receivers use different layouts (`scheduler.rs:123` doc); parent outgoing buffers are drained before child buffers and folded into child bandwidth requests as a workaround.

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `runtime/runtime/src/congestion_control.rs:162` | `ReceiptSink::forward_or_buffer_receipt` | Per-receipt forward-or-buffer entry point |
| `runtime/runtime/src/congestion_control.rs:236` | `forward_from_buffer` | Drain outgoing buffers first, FIFO, parents then current |
| `runtime/runtime/src/congestion_control.rs:338` | `forward_from_buffer_to_shard` | Forward buffer prefix; break on first non-forward |
| `runtime/runtime/src/congestion_control.rs:403` | `try_forward` | Gas/size admission check + clamp + ClampOutgoingGasAdmission |
| `runtime/runtime/src/congestion_control.rs:466` | `buffer_receipt` | Push to outgoing buffer; bump own congestion info |
| `runtime/runtime/src/congestion_control.rs:678` | `compute_receipt_congestion_gas` | Gas charged to a receipt for congestion (0 for data/yield/resume) |
| `runtime/runtime/src/congestion_control.rs:743` | `bootstrap_congestion_info` | Cold-start scan of delayed queue + buffers |
| `runtime/runtime/src/congestion_control.rs:802` | `DelayedReceiptQueueWrapper` | Accumulate delayed-queue gas/byte deltas; resharding filter |
| `core/primitives/src/congestion_info.rs:80` | `CongestionControl::outgoing_gas_limit` | Sender's gas budget to a receiver |
| `core/primitives/src/congestion_info.rs:44` | `CongestionControl::congestion_level` | max of incoming/outgoing/memory/missed-chunks |
| `core/primitives/src/congestion_info.rs:123` | `shard_accepts_transactions` | Tx rejection above threshold |
| `core/primitives/src/congestion_info.rs:360` | `finalize_allowed_shard` | Round-robin allowed-shard selection |
| `core/primitives/src/congestion_info.rs:460` | `CongestionInfoV1` | The four congestion fields in the chunk header |
| `runtime/runtime/src/bandwidth_scheduler/mod.rs:44` | `run_bandwidth_scheduler` | Per-height byte-grant computation + state persist |
| `runtime/runtime/src/bandwidth_scheduler/scheduler.rs:200` | `BandwidthScheduler::run` | Allowance/base/request/leftover grant algorithm |
| `core/primitives/src/bandwidth_scheduler.rs:84` | `BandwidthRequest::make_from_receipt_sizes` | Build next-height request from buffer sizes |
| `runtime/runtime/src/congestion_control.rs:503` | `generate_bandwidth_requests` | Per-receiver requests, parent-fold, metadata fallback |
| `core/store/src/trie/receipts_column_helper.rs:63` | `TrieQueue` | Trie FIFO push/pop/iter for delayed + buffered |
| `runtime/runtime/src/lib.rs:1514` | `process_action_receipt` | Postpone-or-execute, data-dependency counting |
| `runtime/runtime/src/lib.rs:1307` | `process_receipt` (Data arm) | Deliver data, unblock postponed receipt |
| `runtime/runtime/src/lib.rs:2930` | `resolve_promise_yield_timeouts` | Emit timeout PromiseResume receipts |
| `core/primitives/src/receipt.rs:566` | `ReceiptEnum` | Receipt kinds + discriminants |
| `core/primitives-core/src/version.rs:453` | `ProtocolFeature::protocol_version` | Feature activation versions |

## Open questions

- **`allowed_shard` not fully validated**: `CongestionInfo::validate_extra_and_header` compares gas/bytes/allowed_shard fields but a code TODO (`congestion_info.rs:203`) notes `allowed_shard` validation is incomplete; the precise consensus check on a maliciously-set `allowed_shard` was not traced.
- **Scheduler determinism vs `prev_block_hash` RNG**: ties among equal-allowance requests are broken by `ChaCha20Rng` seeded from `prev_block_hash` (`mod.rs:115`); confirmed all validators share that seed, but the exact RNG draw order inside `BandwidthScheduler::run` (and thus its independence from map-iteration order) was not exhaustively read.
- **`compute_receipt_congestion_gas` excludes data/yield/resume gas**: by design (MVP), receipts other than action receipts contribute zero congestion gas. Whether this under-counting can cause sustained queue growth on data-receipt-heavy workloads is a noted design limitation, not verified against a bound here.
- **`use_state_stored_receipt` flag origin**: this spec treats it as runtime config; its exact derivation from protocol version/parameters lives in [genesis-configuration](genesis-configuration.md) and was not traced.
