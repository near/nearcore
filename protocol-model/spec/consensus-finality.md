# Consensus & finality (Doomslug)

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `chain/chain/src/doomslug.rs`, `chain/chain/src/approval_verification.rs`, `core/primitives/src/block_header.rs`, `core/primitives/src/block.rs`, `chain/chain/src/chain.rs`, `chain/client/src/client.rs`, `chain/client/src/client_actor.rs`

## Role

Doomslug is NEAR's finality gadget. It governs two coupled questions: (1) *when* a block producer is allowed to produce a block at a given height (it must have collected approvals from >2/3 of stake on top of its tip), and (2) *which* ancestor block is "final" (irreversible). Each block producer continuously sends **approvals** (endorsements or skips) to the next height's producer; a producer aggregates these into its block header's `approvals` vector. Doomslug finality (`last_ds_final_block`) is established by two consecutive heights; full finality (`last_final_block`) by three consecutive heights. The component sits between [epoch-validators-staking](epoch-validators-staking.md) (which fixes who the producer/approvers are and their stake) and [chain-block-processing](chain-block-processing.md) (which applies an accepted block and feeds the new tip back into Doomslug). Approvals are gossiped over the network; chunk readiness is supplied by [sharding-chunks](sharding-chunks.md) / [stateless-validation](stateless-validation.md).

## Key data structures

- **`ApprovalInner`** — `core/primitives/src/block_header.rs:433` — the body of an approval, either `Endorsement(CryptoHash)` (the hash of the approved parent block) or `Skip(BlockHeight)` (the height of the parent block being skipped over). `ApprovalInner::new` (`block_header.rs:465`) picks `Endorsement(*parent_hash)` iff `target_height == parent_height + 1`, otherwise `Skip(parent_height)`. Endorsements bind to the exact parent hash; skips bind only to the parent height (deliberately — see Invariants).
- **`Approval`** — `core/primitives/src/block_header.rs:442` — fields `inner: ApprovalInner`, `target_height: BlockHeight` (the height at which this approval may be included — only a block with `height == target_height` may carry it), `signature: Signature`, `account_id: AccountId`. The signed message is `(inner, target_height)` serialized by `Approval::get_data_for_sig` (`block_header.rs:492`): borsh of `inner` concatenated with the little-endian `target_height`.
- **`ApprovalMessage`** — `core/primitives/src/block_header.rs:460` — wraps an `Approval` with the `target` account it is routed to (the next height's block producer).
- **`DoomslugThresholdMode`** — `chain/chain/src/doomslug.rs:46` — `TwoThirds` (production: a block requires >2/3 of stake approving) or `NoApprovals` (tests only: production is never blocked on approvals).
- **`Doomslug`** — `chain/chain/src/doomslug.rs:139` — the full state machine, no chain/storage integration. (The doc-comment references a `PersistentDoomslug` wrapper that no longer exists; the `Doomslug` is held directly as `Client::doomslug`, `chain/client/src/client.rs:128`.) Key fields: `approval_trackers: HashMap<BlockHeight, …>`, `largest_target_height`, `largest_final_height`, `largest_threshold_height`, `largest_approval_height`, `tip: DoomslugTip`, `endorsement_pending: bool`, `timer: DoomslugTimer`.
- **`DoomslugTimer`** — `chain/chain/src/doomslug.rs:58` — `started`, `last_endorsement_sent`, `height`, and the four mutable config delays `endorsement_delay`, `min_delay`, `max_delay`, `chunk_wait_mult`.
- **`DoomslugApprovalsTracker`** — `chain/chain/src/doomslug.rs:73` — accumulates approvals for one `(target_height, ApprovalInner)` pair: a `witness` map account→(approval, arrival time), per-account stakes, total/approved stake for this epoch and next epoch, and `time_passed_threshold` (when the 2/3 threshold was first crossed).
- **`DoomslugApprovalsTrackersAtHeight`** — `chain/chain/src/doomslug.rs:130` — one per `target_height`; holds a tracker per distinct `ApprovalInner` plus `last_approval_per_account`, enforcing one approval per `(target_height, account_id)`.
- **`DoomslugBlockProductionReadiness`** — `chain/chain/src/doomslug.rs:53` — `NotReady` or `ReadySince(Instant)`.
- **`ChunksReadiness`** — `chain/chain/src/doomslug.rs:87` — `Ready(Instant)` (when chunks became ready) or `NotReady`.
- **Header finality fields** — `BlockHeader::last_final_block()` (`core/primitives/src/block_header.rs:1485`) and `BlockHeader::last_ds_final_block()` (`core/primitives/src/block_header.rs:1509`) return stored `CryptoHash`es; `approvals()` (`block_header.rs:1561`) returns the `&[Option<Box<Signature>>]`.

## Behavior

### 1. Endorsement vs. skip encoding

An approval encodes which it is purely via `ApprovalInner` (`block_header.rs:465`): if the producer is voting for height exactly one above its tip, it endorses with the tip's hash; otherwise it skips, recording the tip's height. Validation reconstructs the same choice from `prev_block_height + 1 == block_height` (`approval_verification.rs:22`), so the encoding is not trusted from the wire — it is recomputed.

### 2. Approval production (the timer)

Approvals are produced by `Doomslug::process_timer` (`doomslug.rs:489`), driven periodically by `ClientActor::try_doomslug_timer` (`client_actor.rs:1457`). Note the implementation deviates from the paper: endorsements are emitted from the timer, not on block receipt, to stagger production when the network is fast. Per tick (bounded to `MAX_TIMER_ITERS = 20`, `doomslug.rs:19`):

1. Compute `skip_delay = timer.get_delay(timer.height - largest_final_height)` (`doomslug.rs:493`). `get_delay(n)` (`doomslug.rs:176`) = `min(max_delay, min_delay + (min_delay/10) * (n - 2))` — delay grows with distance from the last final block, capped at `max_delay`.
2. **Endorsement**: if `endorsement_pending` and `now >= last_endorsement_sent + endorsement_delay`, and `tip.height >= largest_target_height`, bump `largest_target_height = tip.height + 1` and create an approval for `tip.height + 1` (an endorsement, since it is exactly one above the tip). Clear `endorsement_pending` (`doomslug.rs:505`).
3. **Skip**: if `now >= timer.started + skip_delay`, create an approval for `timer.height + 1` (a skip), then advance `timer.started += skip_delay` and `timer.height += 1` (`doomslug.rs:532`). Otherwise break.

`create_approval` (`doomslug.rs:565`) signs over `tip.block_hash` / `tip.height` / `target_height`; with no signer it yields `None`. `largest_target_height` is monotonic and is persisted *before* approvals are sent (`client_actor.rs:1463`) so a crash cannot cause the node to later sign a lower target height (slashing protection). An invariant `skip_delay >= 2 * endorsement_delay` is debug-asserted (`doomslug.rs:501`).

### 3. Routing an approval

`Client::send_block_approval` (`client.rs:1779`) routes the approval to `get_block_producer(next_epoch_id, target_height)` (`client.rs:1810`). For a `Skip`, if the epoch transition is not yet final, it additionally sends to the would-be producer in the previous epoch for liveness across forks (`client.rs:1795`). `send_block_approval_to_account` (`client.rs:1755`) short-circuits to `collect_block_approval(..., SelfApproval)` when the target is this node.

### 4. Receiving and tracking approvals

`Client::collect_block_approval` validates a peer approval before tracking: it verifies the signature against the validator's key in the next-block epoch (falling back to the epoch-after-next if the signer is not in the next epoch) via `verify_validator_signature` over `Approval::get_data_for_sig` (`client.rs:2434`); it drops the approval unless we are the block producer for `target_height` and know `parent_hash` (`client.rs:2445`). Only then does it call `Doomslug::on_approval_message` (`client.rs:2477`).

`Doomslug::on_approval_message` (`doomslug.rs:690`) ignores approvals whose `target_height` is below the tip or more than `MAX_HEIGHTS_AHEAD_TO_STORE_APPROVALS = 10_000` (`doomslug.rs:26`) ahead, then routes into `on_approval_message_internal` (`doomslug.rs:664`) → the per-height tracker. `DoomslugApprovalsTrackersAtHeight::process_approval` (`doomslug.rs:314`):
1. Withdraws any prior approval from the same account at this height (`withdraw_approval`, `doomslug.rs:245`) — enforcing **one approval per (target_height, account_id)**; if the old tracker's witness becomes empty it is removed.
2. Returns `NotReady` if the account is not among the supplied `stakes` (`doomslug.rs:342`).
3. Inserts/updates the tracker for the approval's `ApprovalInner` and calls `DoomslugApprovalsTracker::process_approval` (`doomslug.rs:219`), which adds the account's stake to `approved_stake_this_epoch` / `approved_stake_next_epoch` exactly once and evaluates readiness.

`largest_approval_height` and (when threshold crossed) `largest_threshold_height` are updated (`doomslug.rs:676`).

### 5. The 2/3 readiness threshold

`DoomslugApprovalsTracker::get_block_production_readiness` (`doomslug.rs:267`): the block is ready iff `approved_stake_this_epoch > 2/3 * total_stake_this_epoch` **and** (`approved_stake_next_epoch > 2/3 * total_stake_next_epoch` **or** `total_stake_next_epoch == 0`), or the mode is `NoApprovals`. Note the comparison is strictly greater-than. On first crossing it records `time_passed_threshold = now`; thereafter it returns `ReadySince(that instant)`.

### 6. Block-production readiness (timing gate)

`Doomslug::ready_to_produce_block` (`doomslug.rs:722`) decides if this node may produce at `target_height` on its current tip (the caller already checked it is the producer). It looks up the tracker for `ApprovalInner::new(tip.block_hash, tip.height, target_height)`:
1. If no tracker or `get_block_production_readiness() == NotReady`, return `false`.
2. If chunks are `Ready`, return `true` immediately.
3. Otherwise wait: ready only once `now > when + chunk_wait_delay`, where `chunk_wait_delay = get_delay(timer.height - largest_final_height) * chunk_wait_mult` (`doomslug.rs:776`). This lets a producer with full approvals but missing chunks wait a bounded time before producing anyway.

`ClientActor` gates production on this (`client_actor.rs:1254`). Under SPICE an additional `spice_timer.ready_to_produce_block` must also pass (`client_actor.rs:1242`).

### 7. Assembling approvals into a block

When producing, `Client::produce_block_*` fetches the witness for the chosen `ApprovalInner` via `Doomslug::get_witness` (`doomslug.rs:618`), then maps the ordered epoch block approvers (`get_epoch_block_approvers_ordered`) to `Option<Signature>` — present if that account approved, else `None` (`client.rs:1036`). Only signatures are stored; the common `inner`/`target_height` is reconstructable, so the full approval is recoverable for verification.

### 8. Computing `last_ds_final_block` and `last_final_block`

Computed at block construction in `Block::new` (`core/primitives/src/block.rs:217`):
- `last_ds_final_block` = `prev.hash()` if `height == prev.height() + 1` (two consecutive heights → previous block is doomslug-final), else `prev.last_ds_final_block()`.
- `last_final_block` = `prev.last_final_block_for_height(height)` (`block_header.rs:1500`): if `height == prev.height() + 1` **and** `prev.last_ds_final_block() == prev.prev_hash()` (i.e. three consecutive heights), then `prev.prev_hash()`; otherwise carry forward `prev.last_final_block()`.

So **doomslug-final** = two blocks at consecutive heights; **final** = three blocks at consecutive heights. These are header fields, not recomputed by readers.

### 9. Storing finality / updating the tip

After a block is accepted, `ChainUpdate::update_final_head_from_block` (`chain/chain/src/chain_update.rs:412`) advances the persisted final head to the header of `last_final_block()` when its height exceeds the current final head. `Client::check_and_update_doomslug_tip` (`client.rs:1701`) calls `Doomslug::set_tip(block_hash, height, last_final_height)` (`doomslug.rs:643`) whenever the chain head changes; `set_tip` resets the timer (`timer.height = height + 1`, `timer.started = now`), sets `largest_final_height`, prunes trackers outside the retention window (`should_retain_height`, `doomslug.rs:32`), and sets `endorsement_pending = true`.

## Interactions

- **Consumes**: chain head / `last_final_block` heights from [chain-block-processing](chain-block-processing.md) (via `set_tip`); the ordered approver set and per-account stakes (this and next epoch) from [epoch-validators-staking](epoch-validators-staking.md) (`get_epoch_block_approvers_ordered`, `get_block_producer`); chunk readiness from [sharding-chunks](sharding-chunks.md); validator signing keys for signature verification.
- **Produces**: signed `Approval`s gossiped to peers (`NetworkRequests::Approval`, `client.rs:1773`); the `approvals` vector and `last_ds_final_block` / `last_final_block` header fields of produced blocks; the persisted final head.
- **Touches**: block header validation in [chain-block-processing](chain-block-processing.md) (see below); the block-production pipeline (gating). Does not itself apply blocks.

## Protocol-version-gated behavior

The Doomslug finality rule (2/3 threshold, two-/three-block finality, approval encoding) is **not** version-gated at v86 — it is the same across all current header versions (`last_ds_final_block`/`last_final_block` exist on `BlockHeaderV1..V7`, `block_header.rs:1509`).

- **Optimistic block production** — formerly `ProduceOptimisticBlock`, now `_DeprecatedProduceOptimisticBlock` (`core/primitives-core/src/version.rs:305`), activated at version 77 and thus always on at v86. It lets chunk application start before the full block arrives but does **not** change the Doomslug finality rule or the approval threshold; optimistic block metadata is only reused for non-finality header fields (`block.rs:200`). See `docs/architecture/how/optimistic_block.md`.
- **SPICE** (`ProtocolFeature::Spice`, `version.rs`) — not enabled in the stable v86 build (`PROTOCOL_VERSION` gating in `version.rs:636`). When enabled it adds a parallel `spice_timer` gate on production (`client_actor.rs:1242`) and disables optimistic blocks, but Doomslug approval/finality semantics are unchanged.

No `ProtocolFeature` at v86 alters the approval-collection or finality computation otherwise. None known beyond the above.

## Invariants & failure modes

- **2/3 production threshold.** A non-self-produced block must satisfy `Doomslug::can_approved_block_be_produced` (`doomslug.rs:584`): approved stake strictly greater than 2/3 of total, for both this epoch and (when applicable) next epoch. Enforced during header validation in `Chain::validate_header` (`chain/chain/src/chain.rs:969`); failure → `Error::NotEnoughApprovals`.
- **Approval signatures.** `verify_approval_with_approvers_info` (`approval_verification.rs:11`) recomputes the signed message from `(prev_hash, prev_height, height)` and checks every present signature against the ordered approver keys; orphan path uses `verify_approvals_and_threshold_orphan` (`approval_verification.rs:42`). Bad signature → `Error::InvalidApprovals` (`chain.rs:960`). Peer approvals are additionally signature-checked before entering Doomslug (`client.rs:2434`); a bad signature silently drops the message.
- **Finality fields must be correctly derived.** `validate_header` recomputes `expected_last_ds_final_block` and `expected_last_final_block` from the prev header and rejects mismatches with `Error::InvalidFinalityInfo` (`chain.rs:991`).
- **One approval per (target_height, account_id).** Enforced by `last_approval_per_account` + `withdraw_approval` (`doomslug.rs:320`), so an account cannot inflate approved stake by re-voting; asserted by `test_doomslug_one_approval_per_target_height` (`doomslug.rs:1114`).
- **Endorsement binds to hash, skip binds to height.** Endorsements carry the parent hash so two endorsements at the same prev-height conflict (safety); skips carry only the height so a fork at one height does not deadlock future producers (liveness). Rationale documented in `docs/ChainSpec/Consensus.md` "Approval condition".
- **Slashing protection.** `largest_target_height` is monotonic (`doomslug.rs:508`/`535`) and persisted before approvals leave the node (`client_actor.rs:1463`), preventing a post-crash node from signing a lower target height. `set_tip` debug-asserts the tip height strictly increases (`doomslug.rs:649`).
- **Equivocation / double-signing.** Doomslug does not itself slash; conflicting approvals are reconciled to the latest per account. Detection of conflicting endorsements is a property the safety theorem relies on (`docs/ChainSpec/Consensus.md` "Safety").
- **Spam bound.** Approvals are tracked only within `[head - 20, head + 10_000]` heights (`should_retain_height`, `doomslug.rs:32`) to bound memory.

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `chain/chain/src/doomslug.rs:46` | `DoomslugThresholdMode` | `TwoThirds` (prod) vs `NoApprovals` (test) |
| `chain/chain/src/doomslug.rs:176` | `DoomslugTimer::get_delay` | skip/chunk-wait delay grows with distance from last final block |
| `chain/chain/src/doomslug.rs:219` | `DoomslugApprovalsTracker::process_approval` | add approval, add stake once, eval readiness |
| `chain/chain/src/doomslug.rs:245` | `withdraw_approval` | remove a superseded approval, decrement stake |
| `chain/chain/src/doomslug.rs:267` | `get_block_production_readiness` | strict >2/3 this+next epoch threshold |
| `chain/chain/src/doomslug.rs:314` | `DoomslugApprovalsTrackersAtHeight::process_approval` | one approval per (height, account) |
| `chain/chain/src/doomslug.rs:489` | `Doomslug::process_timer` | emit endorsements/skips per tick |
| `chain/chain/src/doomslug.rs:584` | `Doomslug::can_approved_block_be_produced` | stateless 2/3 check used in validation |
| `chain/chain/src/doomslug.rs:618` | `Doomslug::get_witness` | fetch approvals to embed in a block |
| `chain/chain/src/doomslug.rs:643` | `Doomslug::set_tip` | reset timer, prune trackers on new head |
| `chain/chain/src/doomslug.rs:690` | `Doomslug::on_approval_message` | accept/track an incoming approval |
| `chain/chain/src/doomslug.rs:722` | `Doomslug::ready_to_produce_block` | approvals + chunk-wait production gate |
| `core/primitives/src/block_header.rs:433` | `ApprovalInner` | Endorsement(hash) / Skip(height) |
| `core/primitives/src/block_header.rs:465` | `ApprovalInner::new` | endorse iff target == parent+1 |
| `core/primitives/src/block_header.rs:492` | `Approval::get_data_for_sig` | signed message = borsh(inner) ++ target_height LE |
| `core/primitives/src/block_header.rs:1500` | `last_final_block_for_height` | three-consecutive-heights final rule |
| `core/primitives/src/block.rs:217` | `Block::new` | compute last_ds_final / last_final at construction |
| `chain/chain/src/approval_verification.rs:11` | `verify_approval_with_approvers_info` | verify aggregated approval signatures |
| `chain/chain/src/approval_verification.rs:42` | `verify_approvals_and_threshold_orphan` | orphan-path verify + threshold |
| `chain/chain/src/chain.rs:953` | `Chain::validate_header` (approval block) | enforce signatures, 2/3, finality fields |
| `chain/chain/src/chain_update.rs:412` | `update_final_head_from_block` | persist new final head |
| `chain/client/src/client.rs:421` | `Client::new` (doomslug) | construct Doomslug with config delays |
| `chain/client/src/client.rs:1036` | `Client::produce_block_*` | map witness to ordered `Option<Signature>` |
| `chain/client/src/client.rs:1701` | `check_and_update_doomslug_tip` | call `set_tip` on head change |
| `chain/client/src/client.rs:1779` | `Client::send_block_approval` | route approval to next producer(s) |
| `chain/client/src/client.rs:2369` | `Client::collect_block_approval` | validate peer approval, feed Doomslug |
| `chain/client/src/client_actor.rs:1254` | block production gate | call `ready_to_produce_block` |
| `chain/client/src/client_actor.rs:1457` | `try_doomslug_timer` | drive `process_timer`, persist, send |

## Open questions

- The `Doomslug` doc-comment (`doomslug.rs:137`) still refers to a `PersistentDoomslug` integration struct, but no such type exists at this commit; persistence (`largest_target_height`) is handled inline in `try_doomslug_timer`. Treated as a stale comment.
- `docs/ChainSpec/Consensus.md` is broadly accurate but stale in detail: its `get_delay` formula omits the `min_delay/10` step factor and the `max_delay` cap that the code uses (`doomslug.rs:176`), and its `process_timer` pseudocode endorses on `timer_started + ENDORSEMENT_DELAY` whereas the code uses `last_endorsement_sent + endorsement_delay`. The doc does not mention the `chunk_wait_mult` chunk-readiness gate. Code is authoritative.
