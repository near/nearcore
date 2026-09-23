# Consensus & finality (Doomslug)

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `chain/chain/src/doomslug.rs`, `core/primitives/src/block_header.rs`, `core/primitives/src/block.rs`, `chain/chain/src/approval_verification.rs`, `chain/chain/src/signature_verification.rs`, `chain/chain/src/chain.rs`, `chain/client/src/client.rs`, `chain/client/src/client_actor.rs`

## Role

Doomslug is NEAR's finality gadget. It decides (a) *when* a block producer is allowed to produce a block at a given height — namely once it has collected approvals from more than 2/3 of the stake for that height — and (b) *which* ancestor blocks are final. Each block carries a vector of `approvals` (one slot per epoch block-approver) plus two derived fields, `last_ds_final_block` and `last_final_block`, that record the finality frontier at that block. Approvals are gossiped between block producers via networking; the block producer schedule for a height comes from epoch management ([epoch-validators-staking](epoch-validators-staking.md)); accepted blocks flow into the block-processing pipeline ([chain-block-processing](chain-block-processing.md)). The in-memory logic lives in `Doomslug` (no chain/storage coupling), driven by the client actor's periodic timer.

## Key data structures

- **`ApprovalInner`** — `core/primitives/src/block_header.rs:433` — an enum with two variants encoding *what kind* of approval this is: `Endorsement(CryptoHash)` (approve building directly on the block with that hash) or `Skip(BlockHeight)` (skip the parent height, approve producing a block some heights later). `ApprovalInner::new` (`:466`) chooses `Endorsement(parent_hash)` iff `target_height == parent_height + 1`, else `Skip(parent_height)`.
- **`Approval`** — `core/primitives/src/block_header.rs:442` — a signed approval: `{ inner: ApprovalInner, target_height: BlockHeight, signature: Signature, account_id: AccountId }`. Built by `Approval::new` (`:480`); the signed message is `borsh(inner) ++ target_height.to_le_bytes()` via `Approval::get_data_for_sig` (`:492`). Note the signature does **not** cover `account_id`, so validation must independently check the signer key belongs to the claimed account.
- **`ApprovalMessage`** — `core/primitives/src/block_header.rs:460` — `{ approval, target: AccountId }`; the wire wrapper directing an approval to the next block producer.
- **`DoomslugThresholdMode`** — `chain/chain/src/doomslug.rs:46` — `TwoThirds` (production; block requires >2/3 approving stake, guarantees finality) or `NoApprovals` (tests; block production not gated on approvals).
- **`DoomslugBlockProductionReadiness`** — `chain/chain/src/doomslug.rs:53` — `NotReady` or `ReadySince(Instant)` (the moment the threshold was first crossed; used to time chunk waiting).
- **`Doomslug`** — `chain/chain/src/doomslug.rs:139` — the finality state machine. Holds `threshold_mode`, per-height `approval_trackers`, the current `tip` (`DoomslugTip`, `:68`), a `timer` (`DoomslugTimer`, `:58`), `endorsement_pending`, and four tracked heights: `largest_target_height` (highest height we issued an approval for; persisted to guard against equivocation on restart), `largest_final_height`, `largest_threshold_height`, `largest_approval_height`.
- **`DoomslugApprovalsTrackersAtHeight`** — `chain/chain/src/doomslug.rs:130` — one per `target_height`; keyed by `ApprovalInner`, holds a `DoomslugApprovalsTracker` per distinct parent, plus `last_approval_per_account` enforcing **one approval per account per target height**.
- **`DoomslugApprovalsTracker`** — `chain/chain/src/doomslug.rs:73` — accumulates `approved_stake_{this,next}_epoch` against `total_stake_{this,next}_epoch`, keeps a `witness: HashMap<AccountId, (Approval, Utc)>`, and records `time_passed_threshold`.
- **`ChunksReadiness`** — `chain/chain/src/doomslug.rs:87` — `Ready(Instant)` / `NotReady`; gates how long a ready-by-approvals producer waits for chunks.
- **Block header finality fields** — every `BlockHeader` version (V1–V7) stores `last_final_block` and `last_ds_final_block` in `inner_rest`, e.g. `core/primitives/src/block_header.rs:79`–`:389`; accessed via `last_final_block()` (`:1485`) and `last_ds_final_block()` (`:1509`).
- **Header `approvals`** — the per-approver signature vector `Vec<Option<Box<Signature>>>`, ordered to match `get_epoch_block_approvers_ordered`.

## Behavior

### Approval production (the timer)

`Doomslug::process_timer` (`chain/chain/src/doomslug.rs:489`) is the heart of the state machine, called periodically (every `doomslug_step_period`) from `ClientActor::try_doomslug_timer` (`chain/client/src/client_actor.rs:1457`). It runs at most `MAX_TIMER_ITERS = 20` iterations (`:19`) per call to avoid unbounded loops.

1. Each iteration computes `skip_delay = timer.get_delay(timer.height - largest_final_height)` (`:493`). `get_delay` (`:176`) returns `min(max_delay, min_delay + (min_delay/10) * (n-2))` — the delay grows linearly with how many heights have passed since the last doomslug-final block, capped at `max_delay`.
2. **Endorsement path** (`:505`): if `endorsement_pending` and `now >= last_endorsement_sent + endorsement_delay`, and the tip height `>= largest_target_height`, it bumps `largest_target_height` to `tip_height + 1` and creates an `Endorsement` approval for `target_height = tip_height + 1` (`create_approval`, `:565`). Then clears `endorsement_pending`. This is the "approve the block we just accepted" path; endorsements are deliberately staggered by `endorsement_delay` rather than sent instantly on block receipt.
3. **Skip path** (`:532`): if `now >= timer.started + skip_delay`, it emits a `Skip` approval for `timer.height + 1`, updates `largest_target_height`, then advances `timer.started += skip_delay` and `timer.height += 1`. This is how the network moves past a height whose block never arrived.
4. Otherwise it `break`s (`:558`).

Returned approvals are sent by `try_doomslug_timer`: it first **persists `largest_target_height`** to the store (`client_actor.rs:1467`) and commits *before* sending, so a crash/restart cannot cause the node to sign a lower target height and be slashed. Sending goes through `Client::send_block_approval` → `send_block_approval_to_account` (`chain/client/src/client.rs:1755`), which either self-collects (`ApprovalType::SelfApproval`, `:1763`) or gossips an `ApprovalMessage` to the next block producer via `NetworkRequests::Approval` (`:1774`).

### Tip updates

When the chain head advances, `Client::check_and_update_doomslug_tip` (`chain/client/src/client.rs:1701`) computes the last-final height from the head header's `last_final_block()` and calls `Doomslug::set_tip` (`doomslug.rs:643`). `set_tip` records the new tip, sets `largest_final_height`, sets `timer.height = height + 1`, resets `timer.started = now`, prunes approval trackers outside the retention window (`should_retain_height`, `:32`), and sets `endorsement_pending = true` so the next timer tick will endorse the new tip.

### Approval collection & threshold

Incoming approvals (self or peer) reach `Client::collect_block_approval` (`chain/client/src/client.rs:2369`):

1. Resolve the parent hash: `Endorsement` carries it directly; `Skip` resolves via `resolve_skip_parent` (`:2328`).
2. For **peer** approvals, verify the signature against the signer's validator key in the appropriate epoch (`:2434`, `verify_validator_signature`) — trying the next-block epoch first, then epoch-after-next at boundaries. Self approvals skip this.
3. Only forward to Doomslug if we are the block producer for `target_height` (`:2445`), or we don't yet know the parent header (could be a next-epoch approval, buffered).
4. `Doomslug::on_approval_message` (`doomslug.rs:690`) drops approvals outside `[tip.height, tip.height + MAX_HEIGHTS_AHEAD_TO_STORE_APPROVALS]` then calls `on_approval_message_internal` (`:664`), which routes into the per-height tracker.

`DoomslugApprovalsTrackersAtHeight::process_approval` (`doomslug.rs:314`) enforces one approval per account: if the account already voted at this target height, it first `withdraw_approval`s the old vote (subtracting its stake, `:245`) before recording the new one. It ignores approvals from accounts not in `stakes` (`:342`). The per-parent `DoomslugApprovalsTracker::process_approval` (`:219`) adds the approver's `(stake_this_epoch, stake_next_epoch)` exactly once (dedup via `witness`), then evaluates readiness.

`get_block_production_readiness` (`doomslug.rs:267`) is the finality rule: the block is ready iff `approved_stake_this_epoch > 2/3 * total_stake_this_epoch` **and** (`approved_stake_next_epoch > 2/3 * total_stake_next_epoch` **or** `total_stake_next_epoch == 0`), or `threshold_mode == NoApprovals`. The dual-epoch check makes approvals valid across an epoch boundary. It records `time_passed_threshold` the first time the threshold is crossed and returns `ReadySince(that_instant)`.

### Deciding to produce a block

`Doomslug::ready_to_produce_block` (`doomslug.rs:722`) is called from `ClientActor` (`client_actor.rs:1254`). It looks up the tracker for `(tip, target_height)`, requires `get_block_production_readiness == ReadySince(when)` (i.e. >2/3 approvals doomslug-finalizing the previous block), then:
- if chunks are `Ready`, produce immediately (`:773`);
- otherwise wait until `now > when + chunk_wait_delay`, where `chunk_wait_delay = get_delay(...) * chunk_wait_mult` (`:778`). This lets a producer proceed with missing chunks after a bounded wait rather than stall.

When actually producing, `Client::produce_block_on` fills the header `approvals` vector from the doomslug witness for `(prev_hash, prev_height, height)` (`client.rs:1028`, `get_witness` `doomslug.rs:618`), mapping each ordered epoch approver to its signature slot (`:1036`).

### Computing finality fields in a new block

`Block::produce` (`core/primitives/src/block.rs:217`):
- `last_ds_final_block` = `prev.hash()` if `height == prev.height() + 1` (i.e. the new block directly follows its parent with no skipped height), else it inherits `prev.last_ds_final_block()`. So a block is **doomslug-final** exactly when its child follows at the immediately next height.
- `last_final_block` = `prev.last_final_block_for_height(height)` (`:220`). `last_final_block_for_height` (`block_header.rs:1500`) returns `prev.prev_hash()` when `target_height == prev.height() + 1` **and** `prev.last_ds_final_block() == prev.prev_hash()` — i.e. two consecutive-height blocks in a row (BFT-style finality: a block is final once it has a child at the next height that itself has a child at the next height). Otherwise it inherits `prev.last_final_block()`.

Doomslug (single confirmation, ds-final) gives fast optimistic finality; `last_final_block` (the two-consecutive-heights rule) gives the stronger BFT finality that is never reverted.

## Interactions

- **Consumes**: the block-approver set and per-approver stakes for a parent block from [epoch-validators-staking](epoch-validators-staking.md) via `get_epoch_block_approvers_ordered`; the chain head/final head from [chain-block-processing](chain-block-processing.md); chunk readiness from chunk production ([sharding-chunks](sharding-chunks.md)).
- **Produces**: `ApprovalMessage`s gossiped over the network layer; the `approvals` vector, `last_final_block`, and `last_ds_final_block` embedded in each produced block header, consumed downstream by header/block validation and light-client generation.
- Who the block producer *is* for a height is decided in [epoch-validators-staking](epoch-validators-staking.md); chunk endorsements (>2/3 per-chunk stake) are a separate mechanism validated in block preprocessing — see [stateless-validation](stateless-validation.md) and [chain-block-processing](chain-block-processing.md).

## Protocol-version-gated behavior

The core Doomslug approval/finality algorithm is **not** protocol-version-gated in this tree — `get_block_production_readiness`, `can_approved_block_be_produced`, and the `last_final_block`/`last_ds_final_block` computation contain no `ProtocolFeature` checks. Adjacent header-validation behavior that *is* gated (verified against `core/primitives-core/src/version.rs`):

- **`ValidateBlockOrdinalAndEpochSyncDataHash`** — activates at **v85** (`version.rs:569`). In `validate_header` (`chain/chain/src/chain.rs:1004`), when enabled the header's `block_ordinal` must equal `block_merkle_tree.size() + 1` and its `epoch_sync_data_hash` must match the locally recomputed value. Pre-v85 these two checks are skipped. Does not affect approval math.
- **`Spice`** — nightly only (**v180**, `version.rs:586`); not active on the 2.13.0 stable path. When enabled, `validate_header` requires `header.is_spice()` (`chain.rs:913`) and skips `validate_chunk_endorsements_in_header` (`chain.rs:1019`). On stable (PV 86) the non-SPICE path always runs.
- Optimistic block production (the former `_DeprecatedProduceOptimisticBlock`, historically PV 77) is now folded in unconditionally: `Block::produce` will take VRF/timestamp/randomness from an optimistic block when available (`block.rs:200`) but the finality-field computation is identical. There is no live `ProduceOptimisticBlock` gate on 2.13.0 — the feature is deprecated in `version.rs`.

None of the named features that activate at **v83** (`ExcludeExistingCodeFromWitnessForCodeLen`, `InvalidTxGenerateOutcomes`, `FixAccessKeyAllowanceCharging`, `IncludeDeployGlobalContractOutcomeBurntStorage`, `GlobalContractDistributionNonce`, `EthImplicitGlobalContract`; `version.rs:549`) nor the PV-86 feature **`EnforcePerReceiptStorageProofLimit`** (`version.rs:576`) touch consensus/finality. `MIN_SUPPORTED_PROTOCOL_VERSION = 83` (`version.rs:600`), `STABLE_PROTOCOL_VERSION = 86` (`version.rs:628`).

## Invariants & failure modes

- **>2/3 stake to produce/finalize.** Enforced in `get_block_production_readiness` (`doomslug.rs:267`) at production time and re-checked at validation time by `Doomslug::can_approved_block_be_produced` (`doomslug.rs:584`); a header with insufficient approving stake is rejected with `Error::NotEnoughApprovals` (`chain/chain/src/chain.rs:974`). The check is on both this-epoch and next-epoch stakes.
- **Valid approval signatures & correct signer.** During block validation `verify_approval_with_approvers_info` (`chain/chain/src/approval_verification.rs:11`) reconstructs the signed message from `(prev_hash, prev_height, height)` and verifies every present signature against the corresponding ordered approver's key; failure → `Error::InvalidApprovals` (`chain.rs:960`). Orphan headers use `verify_approvals_and_threshold_orphan` (`approval_verification.rs:42`), which also returns `Error::InvalidApprovals` / `Error::NotEnoughApprovals`. The approver order must match `get_epoch_block_approvers_ordered`; more approvals than approvers → immediate reject (`approval_verification.rs:18`).
- **Finality fields must be reproducible.** `validate_header` recomputes the expected `last_ds_final_block` and `last_final_block` from `prev_header` and rejects mismatches with `Error::InvalidFinalityInfo` (`chain.rs:977`–`:995`). This mirrors the production-side computation in `Block::produce`.
- **Block header signature.** `verify_block_header_signature_with_epoch_manager` (`chain/chain/src/signature_verification.rs:55`) checks the header signature against the height's block producer; mismatch → `Error::InvalidSignature` (`chain.rs:883`).
- **No equivocation across restart.** `largest_target_height` is persisted and committed before approvals are sent (`client_actor.rs:1467`), and `set_tip` `debug_assert`s monotonic tip height (`doomslug.rs:649`); the node never signs a target height below one it already committed.
- **One approval per account per height.** Enforced via `last_approval_per_account` + `withdraw_approval` (`doomslug.rs:320`); a later approval from the same account replaces the earlier one rather than double-counting stake. Asserted by `test_doomslug_one_approval_per_target_height` (`doomslug.rs:1113`).
- **Spam resistance.** Approvals outside `[tip.height, tip.height + MAX_HEIGHTS_AHEAD_TO_STORE_APPROVALS]` (10_000, `doomslug.rs:26`) are dropped (`on_approval_message`, `:690`); trackers are pruned on every tip update (`should_retain_height`, `:32`).
- **Overflow safety.** Stake accumulation uses `checked_add`/`checked_sub`/`checked_mul`/`checked_div` throughout (`doomslug.rs:190`, `:597`), panicking on overflow rather than silently wrapping.
- Equivocation *detection/slashing* is not performed in this component: `verify_approvals_and_threshold_orphan` explicitly "ignores next epoch approvals and slashing" (`approval_verification.rs:41`), and challenges are rejected outright at header validation (`Error::InvalidChallenge`, `chain.rs:872`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `chain/chain/src/doomslug.rs:46` | `DoomslugThresholdMode` | TwoThirds (prod) vs NoApprovals (tests) |
| `chain/chain/src/doomslug.rs:139` | `Doomslug` | in-memory finality state machine |
| `chain/chain/src/doomslug.rs:176` | `DoomslugTimer::get_delay` | linear-growth skip/wait delay, capped at max_delay |
| `chain/chain/src/doomslug.rs:219` | `DoomslugApprovalsTracker::process_approval` | accumulate approving stake per parent |
| `chain/chain/src/doomslug.rs:245` | `withdraw_approval` | remove superseded approval's stake |
| `chain/chain/src/doomslug.rs:267` | `get_block_production_readiness` | >2/3 this-epoch AND next-epoch (or next==0) rule |
| `chain/chain/src/doomslug.rs:314` | `DoomslugApprovalsTrackersAtHeight::process_approval` | one-approval-per-account enforcement |
| `chain/chain/src/doomslug.rs:489` | `Doomslug::process_timer` | emit endorsements / skips on timers |
| `chain/chain/src/doomslug.rs:565` | `create_approval` | build a signed `Approval` |
| `chain/chain/src/doomslug.rs:584` | `can_approved_block_be_produced` | stateless >2/3 check used at validation |
| `chain/chain/src/doomslug.rs:643` | `set_tip` | advance tip, reset timer, prune trackers |
| `chain/chain/src/doomslug.rs:690` | `on_approval_message` | horizon check + route into tracker |
| `chain/chain/src/doomslug.rs:722` | `ready_to_produce_block` | approvals + chunk-wait gate for production |
| `core/primitives/src/block_header.rs:433` | `ApprovalInner` | Endorsement vs Skip |
| `core/primitives/src/block_header.rs:466` | `ApprovalInner::new` | Endorsement iff target == parent+1 |
| `core/primitives/src/block_header.rs:480` | `Approval::new` | sign inner ++ target_height |
| `core/primitives/src/block_header.rs:492` | `Approval::get_data_for_sig` | signed message layout |
| `core/primitives/src/block_header.rs:1500` | `last_final_block_for_height` | two-consecutive-heights final rule |
| `core/primitives/src/block.rs:217` | `Block::produce` (finality) | compute `last_ds_final_block` / `last_final_block` |
| `chain/chain/src/approval_verification.rs:11` | `verify_approval_with_approvers_info` | verify every present approval signature |
| `chain/chain/src/approval_verification.rs:42` | `verify_approvals_and_threshold_orphan` | orphan-path approval + threshold check |
| `chain/chain/src/signature_verification.rs:55` | `verify_block_header_signature_with_epoch_manager` | header signature vs height's BP |
| `chain/chain/src/chain.rs:870` | `Chain::validate_header` | approvals, threshold, finality-field, ordinal checks |
| `chain/chain/src/chain.rs:977` | (validate_header) | recompute & enforce finality fields |
| `chain/client/src/client.rs:1701` | `check_and_update_doomslug_tip` | feed head → `set_tip` |
| `chain/client/src/client.rs:2369` | `collect_block_approval` | validate + route incoming approvals |
| `chain/client/src/client.rs:1028` | `produce_block_on` (approvals) | fill header `approvals` from witness |
| `chain/client/src/client_actor.rs:1457` | `try_doomslug_timer` | drive `process_timer`, persist target height, send |
| `core/primitives-core/src/version.rs:569` | `ValidateBlockOrdinalAndEpochSyncDataHash` | v85 header-validation gate |

## Open questions

- The docstring in `chain/chain/AGENTS.md:12` and `chain/chain/src/doomslug.rs:137` reference a `PersistentDoomslug` struct that no longer exists in this tree; persistence of `largest_target_height` is now handled directly in `try_doomslug_timer` via the chain store. The stale comment could be corrected.
- `docs/ChainSpec/Consensus.md` uses the term "block proposer" and models finality abstractly; it appears broadly consistent with the code's two-consecutive-heights rule but was not line-by-line reconciled here — flagged as potentially needing a freshness pass.
