# Consensus & finality (Doomslug)

> Protocol version: 87 (stable) · Release: 2.14.0-rc.1 · Derived from commit: 233252e · Generated: 2026-09-14
> Primary crates/files: `chain/chain/src/doomslug.rs`, `core/primitives/src/block_header.rs`, `core/primitives/src/block.rs`, `chain/chain/src/approval_verification.rs`, `chain/chain/src/signature_verification.rs`, `chain/chain/src/chain.rs`, `chain/chain/src/lightclient.rs`, `chain/client/src/client.rs`, `chain/client/src/client_actor.rs`, `chain/client/src/verified_peer_heights.rs`

## Role

Doomslug is NEAR's finality gadget. It decides (a) *when* a block producer is allowed to produce a block at a given height — namely once it has collected approvals from more than 2/3 of the stake for that height — and (b) *which* ancestor blocks are final. Each block carries a vector of `approvals` (one slot per epoch block-approver) plus two derived fields, `last_ds_final_block` and `last_final_block`, that record the finality frontier at that block. Approvals are gossiped between block producers via networking; the block producer schedule for a height comes from epoch management ([epoch-validators-staking](epoch-validators-staking.md)); accepted blocks flow into the block-processing pipeline ([chain-block-processing](chain-block-processing.md)). The in-memory logic lives in `Doomslug` (no chain/storage coupling), driven by the client actor's periodic timer. Since 2.14 the same header-approval check is also reused *outside* block processing, as a proof-of-height signal that gates sync ([sync](sync.md); see [Verified peer heights](#verified-peer-heights-sync-gating)).

## Key data structures

- **`ApprovalInner`** — `core/primitives/src/block_header.rs:466` — an enum with two variants encoding *what kind* of approval this is: `Endorsement(CryptoHash)` (approve building directly on the block with that hash) or `Skip(BlockHeight)` (skip the parent height, approve producing a block some heights later). `ApprovalInner::new` (`:499`) chooses `Endorsement(parent_hash)` iff `target_height == parent_height + 1`, else `Skip(parent_height)`.
- **`Approval`** — `core/primitives/src/block_header.rs:475` — a signed approval: `{ inner: ApprovalInner, target_height: BlockHeight, signature: Signature, account_id: AccountId }`. Built by `Approval::new` (`:513`); the signed message is `borsh(inner) ++ target_height.to_le_bytes()` via `Approval::get_data_for_sig` (`:525`). Note the signature does **not** cover `account_id`, so validation must independently check the signer key belongs to the claimed account.
- **`ApprovalType`** — `core/primitives/src/block_header.rs:484` — `SelfApproval` / `PeerApproval(PeerId)`; decides whether the signature check is skipped (we made it) or mandatory.
- **`ApprovalMessage`** — `core/primitives/src/block_header.rs:493` — `{ approval, target: AccountId }`; the wire wrapper directing an approval to the next block producer.
- **`DoomslugThresholdMode`** — `chain/chain/src/doomslug.rs:46` — `TwoThirds` (production; block requires >2/3 approving stake, guarantees finality) or `NoApprovals` (tests; block production not gated on approvals).
- **`DoomslugBlockProductionReadiness`** — `chain/chain/src/doomslug.rs:53` — `NotReady` or `ReadySince(Instant)` (the moment the threshold was first crossed; used to time chunk waiting).
- **`Doomslug`** — `chain/chain/src/doomslug.rs:139` — the finality state machine. Holds `threshold_mode`, per-height `approval_trackers`, the current `tip` (`DoomslugTip`, `:68`), a `timer` (`DoomslugTimer`, `:58`), `endorsement_pending`, a debug `history`, and four tracked heights: `largest_target_height` (highest height we issued an approval for; persisted to guard against equivocation on restart), `largest_final_height`, `largest_threshold_height`, `largest_approval_height`.
- **`DoomslugApprovalsTrackersAtHeight`** — `chain/chain/src/doomslug.rs:130` — one per `target_height`; keyed by `ApprovalInner`, holds a `DoomslugApprovalsTracker` per distinct parent, plus `last_approval_per_account` enforcing **one approval per account per target height**.
- **`DoomslugApprovalsTracker`** — `chain/chain/src/doomslug.rs:73` — accumulates `approved_stake_{this,next}_epoch` against `total_stake_{this,next}_epoch`, keeps a `witness: HashMap<AccountId, (Approval, Utc)>`, and records `time_passed_threshold`.
- **`ChunksReadiness`** — `chain/chain/src/doomslug.rs:87` — `Ready(Instant)` / `NotReady`; gates how long a ready-by-approvals producer waits for chunks.
- **`VerifiedPeerHeights`** — `chain/client/src/verified_peer_heights.rs:11` — per-peer highest *proven* height, plus a 32-entry LRU of header hashes whose approvals already verified so each distinct header costs one signature pass however many peers relay it.
- **Block header finality fields** — every `BlockHeader` version (V1–V7) stores `last_final_block` and `last_ds_final_block` in `inner_rest`; accessed via `last_final_block()` (`core/primitives/src/block_header.rs:1544`) and `last_ds_final_block()` (`:1568`). Approvals are read via `approvals()` (`:1620`), the parent height via `prev_height()` (`:1295`, `None` for V1/V2).
- **`BlockHeaderInnerLiteV2`** — `core/primitives/src/block_header.rs:61` — new in 2.14: the light-client-readable inner-lite used **only** by `BlockHeaderV7` (`:710`), adding `chunk_execution_root`. V7 is the SPICE header, so on stable (PV 87) every header still uses `BlockHeaderInnerLite` and `BlockHeader::chunk_execution_root()` (`:1741`) returns `None`.

## Behavior

### Approval production (the timer)

`Doomslug::process_timer` (`chain/chain/src/doomslug.rs:489`) is the heart of the state machine, called periodically (every `doomslug_step_period`) from `ClientActor::try_doomslug_timer` (`chain/client/src/client_actor.rs:1504`). It runs at most `MAX_TIMER_ITERS = 20` iterations (`:19`) per call to avoid unbounded loops.

1. Each iteration computes `skip_delay = timer.get_delay(timer.height - largest_final_height)` (`:493`). `get_delay` (`:176`) returns `min(max_delay, min_delay + (min_delay/10) * (n-2))` — the delay grows linearly with how many heights have passed since the last doomslug-final block, capped at `max_delay`.
2. **Endorsement path** (`:505`): if `endorsement_pending` and `now >= last_endorsement_sent + endorsement_delay`, and the tip height `>= largest_target_height`, it bumps `largest_target_height` to `tip_height + 1` and creates an `Endorsement` approval for `target_height = tip_height + 1` (`create_approval`, `:565`). Then clears `endorsement_pending`. This is the "approve the block we just accepted" path; endorsements are deliberately staggered by `endorsement_delay` rather than sent instantly on block receipt (`debug_assert!(skip_delay >= 2 * endorsement_delay)`, `:501`).
3. **Skip path** (`:532`): if `now >= timer.started + skip_delay`, it emits a `Skip` approval for `timer.height + 1`, raises `largest_target_height` to at least that, then advances `timer.started += skip_delay` and `timer.height += 1`. This is how the network moves past a height whose block never arrived.
4. Otherwise it `break`s (`:558`).

Returned approvals are sent by `try_doomslug_timer`: it first **persists `largest_target_height`** to the store (`client_actor.rs:1514`) and commits *before* sending, so a crash/restart cannot cause the node to sign a lower target height and be slashed. Approvals are only sent if we are a validator in the head epoch or the next epoch (`client_actor.rs:1519`). Sending goes through `Client::send_block_approval` (`chain/client/src/client.rs:1810`) → `send_block_approval_to_account` (`:1786`), which either self-collects (`ApprovalType::SelfApproval`, `:1794`) or gossips an `ApprovalMessage` to the next block producer via `NetworkRequests::Approval` (`:1805`). When the final head is still in the previous epoch, a `Skip` approval is additionally sent to the previous epoch's producer for that target height, for liveness across a not-yet-final epoch transition (`client.rs:1826`).

### Tip updates

When the chain head advances, `Client::check_and_update_doomslug_tip` (`chain/client/src/client.rs:1732`) computes the last-final height from the head header's `last_final_block()` (falling back to the genesis height when that is `CryptoHash::default()`) and calls `Doomslug::set_tip` (`doomslug.rs:643`). `set_tip` records the new tip, sets `largest_final_height`, sets `timer.height = height + 1`, resets `timer.started = now`, prunes approval trackers outside the retention window (`should_retain_height`, `:32` — from `head - 20` up to `head + 10_000`), and sets `endorsement_pending = true` so the next timer tick will endorse the new tip.

### Approval collection & threshold

Incoming approvals (self or peer) reach `Client::collect_block_approval` (`chain/client/src/client.rs:2401`):

1. Resolve the parent hash: `Endorsement` carries it directly; `Skip` resolves via `resolve_skip_parent` (`:2360`), which prefers, among the blocks at `parent_height`, one whose epoch makes *us* the producer for `target_height`.
2. For **peer** approvals, verify the signature against the signer's validator key in the appropriate epoch (`:2466`, `verify_validator_signature`) — trying the next-block epoch first, then epoch-after-next at boundaries. Self approvals skip this.
3. Only forward to Doomslug if we are the block producer for `target_height` (`:2477`), or we don't yet know the parent header (could be a next-epoch approval, buffered).
4. `Doomslug::on_approval_message` (`doomslug.rs:690`) drops approvals outside `[tip.height, tip.height + MAX_HEIGHTS_AHEAD_TO_STORE_APPROVALS]` then calls `on_approval_message_internal` (`:664`), which routes into the per-height tracker.

`DoomslugApprovalsTrackersAtHeight::process_approval` (`doomslug.rs:314`) enforces one approval per account: if the account already voted at this target height, it first `withdraw_approval`s the old vote (subtracting its stake, `:245`) before recording the new one. It ignores approvals from accounts not in `stakes` (`:342`). The per-parent `DoomslugApprovalsTracker::process_approval` (`:219`) adds the approver's `(stake_this_epoch, stake_next_epoch)` exactly once (dedup via `witness`), then evaluates readiness.

`get_block_production_readiness` (`doomslug.rs:267`) is the finality rule: the block is ready iff `approved_stake_this_epoch > 2/3 * total_stake_this_epoch` **and** (`approved_stake_next_epoch > 2/3 * total_stake_next_epoch` **or** `total_stake_next_epoch == 0`), or `threshold_mode == NoApprovals`. The dual-epoch check makes approvals valid across an epoch boundary. It records `time_passed_threshold` the first time the threshold is crossed and returns `ReadySince(that_instant)`.

### Deciding to produce a block

`Doomslug::ready_to_produce_block` (`doomslug.rs:722`) is called from `ClientActor` (`client_actor.rs:1289`) for every height from `latest_known + 1` up to `get_largest_height_crossing_threshold()` where we are the block producer. It looks up the tracker for `(tip, target_height)`, requires `get_block_production_readiness == ReadySince(when)` (i.e. >2/3 approvals doomslug-finalizing the previous block), then:
- if chunks are `Ready`, produce immediately (`:773`);
- otherwise wait until `now > when + chunk_wait_delay`, where `chunk_wait_delay = get_delay(...) * chunk_wait_mult` (`:778`). This lets a producer proceed with missing chunks after a bounded wait rather than stall.

Under SPICE (nightly, PV 180) this is additionally conjoined with `spice_timer.ready_to_produce_block` (`client_actor.rs:1278`); on stable that term is unconditionally `true` (`:1285`).

When actually producing, `Client::produce_block_on` fills the header `approvals` vector from the doomslug witness for `(prev_hash, prev_height, height)` (`client.rs:1027`, `get_witness` `doomslug.rs:618`), mapping each ordered epoch approver to its signature slot (`client.rs:1035`) and `debug_assert`ing that the witness is fully consumed (`:1044`).

### Computing finality fields in a new block

`Block::produce` (`core/primitives/src/block.rs:113`):
- `last_ds_final_block` = `prev.hash()` if `height == prev.height() + 1` (i.e. the new block directly follows its parent with no skipped height), else it inherits `prev.last_ds_final_block()` (`:219`). So a block is **doomslug-final** exactly when its child follows at the immediately next height.
- `last_final_block` = `prev.last_final_block_for_height(height)` (`:222`). `last_final_block_for_height` (`block_header.rs:1559`) returns `prev.prev_hash()` when `target_height == prev.height() + 1` **and** `prev.last_ds_final_block() == prev.prev_hash()` — i.e. two consecutive-height blocks in a row (BFT-style finality: a block is final once it has a child at the next height that itself has a child at the next height). Otherwise it inherits `prev.last_final_block()`.

Doomslug (single confirmation, ds-final) gives fast optimistic finality; `last_final_block` (the two-consecutive-heights rule) gives the stronger BFT finality that is never reverted.

### Light client blocks

`Chain::create_light_client_block` (`chain/chain/src/chain.rs:694`) picks the last final block as seen from a header, walking back further if the final block is within two heights of an epoch change, and `create_light_client_block_view` (`chain/chain/src/lightclient.rs:34`) packages it. The proof that the light client checks is exactly the Doomslug two-consecutive-heights rule: the view carries the block's inner-lite/inner-rest hashes, the *next* block's inner hash, and `approvals_after_next` — the approvals of the block two heights later (`lightclient.rs:62`). The `chunk_execution_root` field of the view is `block_header.chunk_execution_root()` (`lightclient.rs:49`), i.e. `None` outside SPICE.

New in 2.14 (#16394/#16396): the on-disk form in `DBCol::EpochLightClientBlocks` is now a versioned `StoredLightClientBlock` enum (`core/store/src/light_client_block.rs:22`) with existing rows migrated, decoupling the never-GC'd stored rows from the RPC-shaped `LightClientBlockView`. This is a storage-format change only — no consensus rule changed.

### Verified peer heights (sync gating)

New in 2.14 (#16133, narrowed by #16266). A peer's advertised height used to be taken at face value when deciding to enter sync; it is now cross-checked against block approvals where possible.

1. On an **unsolicited** `BlockResponse`, `ClientActor` calls `Client::note_verified_peer_height` before any drop path (`chain/client/src/client_actor.rs:635`). Requested blocks are skipped — they prove nothing we did not already know.
2. `note_verified_peer_height` (`chain/client/src/client.rs:1225`) prunes entries at or below our head, ignores heights that could not change the sync decision (`peer_height_requires_sync`, `:1209` — `peer_height > head + threshold`, with `threshold = 0` while already syncing), and otherwise records the peer's height if `Chain::verify_header_approvals_without_ancestry` succeeds.
3. `Chain::verify_header_approvals_without_ancestry` (`chain/chain/src/chain.rs:4192`) checks the producer signature first (`partial_verify_orphan_header_signature`, `:844`) because the header hash commits to the approvals, then requires `header.prev_height()` to exist (V1/V2 headers are rejected with `Error::Other("header too old to verify approvals without ancestry")`), and delegates to `verify_approvals_and_threshold_orphan` with the header's own epoch info. A header from an epoch we don't know errors out, so far-ahead blocks simply never become "verified".
4. `ClientActor::syncing_info` (`client_actor.rs:1714`) then chooses between two sources. If `head_is_stale` (`:1755`) — our head's producer-signed timestamp is at least one epoch of `min_block_production_delay` old — it falls back to the unvalidated `highest_height_peers` (`sync_requirement_from_claimed_peers`, `:1769`), since a peer cannot fake our own clock. Otherwise it uses `sync_requirement_from_verified_peers` (`:1793`), capping each connected peer's claimed height by what we proved for it (uncapped peers read as level with us, so they are never a sync target). After state sync, when every verified height sits below the new head, our own header head is used as the sync target instead (`:1745`).

## Interactions

- **Consumes**: the block-approver set and per-approver stakes for a parent block from [epoch-validators-staking](epoch-validators-staking.md) via `get_epoch_block_approvers_ordered`; the chain head/final head from [chain-block-processing](chain-block-processing.md); chunk readiness from chunk production ([sharding-chunks](sharding-chunks.md)).
- **Produces**: `ApprovalMessage`s gossiped over the network layer; the `approvals` vector, `last_final_block`, and `last_ds_final_block` embedded in each produced block header, consumed downstream by header/block validation and light-client generation; the per-peer verified-height signal consumed by the sync state machine ([sync](sync.md)).
- Who the block producer *is* for a height is decided in [epoch-validators-staking](epoch-validators-staking.md).
- Chunk endorsements (>2/3 per-chunk stake) are a **separate** mechanism from block approvals and are validated in block preprocessing — see [stateless-validation](stateless-validation.md). In particular, 2.14's fix to derive chunk-endorsement signed bytes from `chunk_hash` (#16193) does not touch `Approval::get_data_for_sig`; the block-approval signed message is unchanged.
- Chunk-header signature verification now resolves the producer from a grandparent anchor / the `ChunkProducers` DB column (`chain/chain/src/signature_verification.rs:161` `resolve_anchored_producer`, `:190` `resolve_and_verify_anchored_producer`); that is chunk-level and does not affect block approvals — see [sharding-chunks](sharding-chunks.md).

## Protocol-version-gated behavior

The core Doomslug approval/finality algorithm is **not** protocol-version-gated in this tree — `get_block_production_readiness`, `can_approved_block_be_produced`, and the `last_final_block`/`last_ds_final_block` computation contain no `ProtocolFeature` checks. Adjacent header-validation behavior that *is* gated (verified against `core/primitives-core/src/version.rs`):

- **`ValidateBlockOrdinalAndEpochSyncDataHash`** — variant at `version.rs:437`, activates at **v85** (listed in the `=> 85` match arm, `version.rs:611`/`:617`). In `validate_header` (`chain/chain/src/chain.rs:1027`), when enabled the header's `block_ordinal` must equal `block_merkle_tree.size() + 1` and its `epoch_sync_data_hash` must match the locally recomputed value. Pre-v85 these two checks are skipped. Does not affect approval math.
- **`Spice`** — nightly only (**v180**, `version.rs:354` / `:638`); **not active on the 2.14.0-rc.1 stable path** (stable is 87). Changes in 2.14: `validate_header` now requires the header's spice-ness to *match* its epoch in both directions (`chain.rs:929` — previously only a spice epoch with a non-spice header was rejected), and dispatches to `validate_spice_chunk_endorsements_in_header` instead of `validate_chunk_endorsements_in_header` (`chain.rs:1043`). SPICE also adds `BlockHeaderV7` with `BlockHeaderInnerLiteV2`/`chunk_execution_root` and per-chunk execution-root commitment, and gates block production on an extra `spice_timer` (`client_actor.rs:1278`). None of this is reachable at PV 87.
- **`EarlyKickout`** — moved from nightly to **stable v87** (`version.rs:626`). It changes how a *chunk* producer is resolved (the `ChunkProducers` DB column, DB version 50) and early kickout/blacklisting; it does not change block-approval collection, the >2/3 rule, or the finality fields. See [epoch-validators-staking](epoch-validators-staking.md) and [sharding-chunks](sharding-chunks.md).
- Optimistic block production (the former `_DeprecatedProduceOptimisticBlock`, historically PV 77 — `version.rs:305` / `:582`) is folded in unconditionally: `Block::produce` will take VRF/timestamp/randomness from an optimistic block when available (`block.rs:203`) but the finality-field computation is identical. There is no live `ProduceOptimisticBlock` gate.

- **`PostQuantumSignatures`** — v85 (`version.rs:609`/`:617`) — is the only other v84–v87 feature that touches signatures, but it is scoped to *transaction* / access-key signatures (`core/primitives/src/transaction.rs:372`, `:505`). Block approvals are signed with the validator key via `Approval::new` and verified with `verify_validator_signature`; neither is gated on it.

None of the features newly stabilized at **v87** (`FixContractLoadingError`, `RejectEmptyMethodName`, `RejectDelegateV2`, `RejectWithdrawFromGasKeyInDelegate`, `RemoveGasRewards`, `EnforceStorageProofLimitForAllActions`, `ReceiptPromiseInputSizeLimit`, `FixMlDsaCostCharging`, `GlobalContractSameChunkCallFix`, `UniversalAccounts`; `version.rs:619`–`:629`) touch consensus or finality — they are runtime/action-validation features. `MIN_SUPPORTED_PROTOCOL_VERSION = 84` (`version.rs:652`, raised from 83), `STABLE_PROTOCOL_VERSION = 87` (`version.rs:680`). The v83 features that the previous release listed here are now `_Deprecated*` and unconditional.

## Invariants & failure modes

- **>2/3 stake to produce/finalize.** Enforced in `get_block_production_readiness` (`doomslug.rs:267`) at production time and re-checked at validation time by `Doomslug::can_approved_block_be_produced` (`doomslug.rs:584`); a header with insufficient approving stake is rejected with `Error::NotEnoughApprovals` (`chain/chain/src/chain.rs:997`). The check is on both this-epoch and next-epoch stakes. Note the two sites are not textually identical: `can_approved_block_be_produced` passes an epoch vacuously when *that epoch's* 2/3 threshold is zero (`doomslug.rs:614` — `approved > threshold || threshold.is_zero()`, applied to both epochs), whereas `get_block_production_readiness` allows the vacuous case only for the next epoch (`doomslug.rs:272`).
- **Valid approval signatures & correct signer.** During block validation `verify_approval_with_approvers_info` (`chain/chain/src/approval_verification.rs:11`) reconstructs the signed message from `(prev_hash, prev_height, height)` and verifies every present signature against the corresponding ordered approver's key; failure → `Error::InvalidApprovals` (`chain.rs:983`). The approver order must match `get_epoch_block_approvers_ordered`; more approvals than approvers → immediate reject (`approval_verification.rs:18`). The no-ancestry path uses `verify_approvals_and_threshold_orphan` (`approval_verification.rs:42`), which returns `Error::InvalidApprovals` / `Error::NotEnoughApprovals`; it zips against a *heuristic* approver list (`get_heuristic_block_approvers_ordered`, `approval_verification.rs:87` — current-epoch `block_producers_settlement` deduped, with `get_approval_stake(false)`) and deliberately carries **no** `approvals.len() > block_approvers.len()` guard.
  - That truncation is reachable on honest input: the real ordered list (`EpochManager::get_all_block_approvers_ordered`, `chain/epoch-manager/src/lib.rs:1491`) appends the *next* epoch's settlement near an epoch boundary, so an honest header's `approvals` can be longer than the heuristic list. It is trivially reachable on hostile input too, since a byzantine block producer signs whatever approvals vector it likes and the producer-signature pre-check (`chain.rs:4199`) only proves *who* built the header, not that the vector is canonical.
  - It is nevertheless **not** a threshold-bypass: `zip` pairs index *i* with index *i*, so no approval is ever credited to the wrong approver, and the threshold is evaluated by `Doomslug::can_approved_block_be_produced` (`doomslug.rs:584`), which itself zips `approvals` against stakes derived from the **same** truncated `block_approvers` list (`approval_verification.rs:76`, `doomslug.rs:600`/`:607`). Extra approvals therefore contribute zero stake — the check can only become stricter, never weaker, and the worst case is a false negative. The path also never admits a block to the chain; its sole production caller only records a peer height for sync gating (see below).
- **`prev_height` sanity.** `validate_header` rejects a header whose `prev_height` disagrees with the actual parent (`Error::Other("Invalid prev_height")`, `chain.rs:962`). On the no-ancestry path there is no parent to compare against, so since 2.14 (#16389) `verify_approvals_and_threshold_orphan` uses `checked_add` and returns `Error::InvalidBlockHeight(prev_block_height)` for `prev_height == u64::MAX` instead of panicking on overflow and aborting the node (`approval_verification.rs:57`). Asserted by `verify_approvals_and_threshold_orphan_rejects_max_prev_height` (`approval_verification.rs:114`).
- **Finality fields must be reproducible.** `validate_header` recomputes the expected `last_ds_final_block` and `last_final_block` from `prev_header` and rejects mismatches with `Error::InvalidFinalityInfo` (`chain.rs:1000`–`:1017`). This mirrors the production-side computation in `Block::produce`.
- **Block header signature.** `verify_block_header_signature_with_epoch_manager` (`chain/chain/src/signature_verification.rs:78`) checks the header signature against the height's block producer; mismatch → `Error::InvalidSignature` (`chain.rs:898`). Orphan candidates get the cheaper, non-slashing-aware `partial_verify_orphan_header_signature` (`chain.rs:844`, used at `:2463` and `:4199`).
- **Finalizability pre-check.** `check_if_finalizable` (`chain.rs:857`) walks up to `NUM_PARENTS_TO_CHECK_FINALITY = 20` (`chain.rs:129`) ancestors; a header whose height is already below the final head and that does not reach it is rejected with `Error::CannotBeFinalized`.
- **No equivocation across restart.** `largest_target_height` is persisted and committed before approvals are sent (`client_actor.rs:1514`), and `set_tip` `debug_assert`s monotonic tip height (`doomslug.rs:649`); the node never signs a target height below one it already committed.
- **One approval per account per height.** Enforced via `last_approval_per_account` + `withdraw_approval` (`doomslug.rs:320`); a later approval from the same account replaces the earlier one rather than double-counting stake. Asserted by `test_doomslug_one_approval_per_target_height` (`doomslug.rs:1114`).
- **Spam resistance.** Approvals outside `[tip.height, tip.height + MAX_HEIGHTS_AHEAD_TO_STORE_APPROVALS]` (10_000, `doomslug.rs:26`) are dropped (`on_approval_message`, `:690`); trackers are pruned on every tip update, keeping only `MAX_HEIGHTS_BEFORE_TO_STORE_APPROVALS = 20` heights of history (`doomslug.rs:29`, `should_retain_height` `:32`). In the sync path, a peer's claimed height only counts once one of its relayed headers passed the >2/3 approval check, and the per-header LRU bounds signature work to one pass per distinct header (`verified_peer_heights.rs:41`).
- **Overflow safety.** Stake accumulation uses `checked_add`/`checked_sub`/`checked_mul`/`checked_div` throughout (`doomslug.rs:190`, `:597`), panicking on overflow rather than silently wrapping.
- Equivocation *detection/slashing* is not performed in this component: `verify_approvals_and_threshold_orphan` explicitly "ignores next epoch approvals and slashing" (`approval_verification.rs:41`), and challenges are rejected outright at header validation (`Error::InvalidChallenge`, `chain.rs:887`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `chain/chain/src/doomslug.rs:26` | `MAX_HEIGHTS_AHEAD_TO_STORE_APPROVALS` | 10_000-height approval horizon |
| `chain/chain/src/doomslug.rs:32` | `should_retain_height` | tracker retention window (head-20 .. head+10_000) |
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
| `chain/chain/src/doomslug.rs:618` | `get_witness` | approvals to embed in a produced header |
| `chain/chain/src/doomslug.rs:643` | `set_tip` | advance tip, reset timer, prune trackers |
| `chain/chain/src/doomslug.rs:690` | `on_approval_message` | horizon check + route into tracker |
| `chain/chain/src/doomslug.rs:722` | `ready_to_produce_block` | approvals + chunk-wait gate for production |
| `core/primitives/src/block_header.rs:61` | `BlockHeaderInnerLiteV2` | SPICE-only inner-lite with `chunk_execution_root` |
| `core/primitives/src/block_header.rs:466` | `ApprovalInner` | Endorsement vs Skip |
| `core/primitives/src/block_header.rs:499` | `ApprovalInner::new` | Endorsement iff target == parent+1 |
| `core/primitives/src/block_header.rs:513` | `Approval::new` | sign inner ++ target_height |
| `core/primitives/src/block_header.rs:525` | `Approval::get_data_for_sig` | signed message layout |
| `core/primitives/src/block_header.rs:1295` | `prev_height` | `None` for V1/V2 headers |
| `core/primitives/src/block_header.rs:1559` | `last_final_block_for_height` | two-consecutive-heights final rule |
| `core/primitives/src/block.rs:219` | `Block::produce` (finality) | compute `last_ds_final_block` / `last_final_block` |
| `chain/chain/src/approval_verification.rs:11` | `verify_approval_with_approvers_info` | verify every present approval signature |
| `chain/chain/src/approval_verification.rs:42` | `verify_approvals_and_threshold_orphan` | no-ancestry approval + threshold check |
| `chain/chain/src/approval_verification.rs:57` | (checked_add guard) | reject `u64::MAX` prev_height instead of panicking |
| `chain/chain/src/approval_verification.rs:87` | `get_heuristic_block_approvers_ordered` | current-epoch-only approver list the orphan path zips against |
| `chain/epoch-manager/src/lib.rs:1491` | `get_all_block_approvers_ordered` | the real (this + next epoch) ordered approver list |
| `chain/chain/src/chain.rs:2463` | (orphan path) | producer-signature-only orphan admission; the approval check here is commented out (`chain.rs:2468`) |
| `chain/client/src/client.rs:1233` | (only production caller) | `note_verified_peer_height` → `verify_header_approvals_without_ancestry` |
| `chain/chain/src/signature_verification.rs:78` | `verify_block_header_signature_with_epoch_manager` | header signature vs height's BP |
| `chain/chain/src/lightclient.rs:34` | `create_light_client_block_view` | package final block + approvals_after_next |
| `chain/chain/src/chain.rs:844` | `partial_verify_orphan_header_signature` | epoch-only header signature check |
| `chain/chain/src/chain.rs:857` | `check_if_finalizable` | reject headers that cannot reach the final head |
| `chain/chain/src/chain.rs:885` | `Chain::validate_header` | approvals, threshold, finality-field, ordinal checks |
| `chain/chain/src/chain.rs:1000` | (validate_header) | recompute & enforce finality fields |
| `chain/chain/src/chain.rs:4192` | `verify_header_approvals_without_ancestry` | >2/3 approval proof for a header with no known parent |
| `chain/client/src/client.rs:1027` | `produce_block_on` (approvals) | fill header `approvals` from witness |
| `chain/client/src/client.rs:1209` | `peer_height_requires_sync` | heights that could change the sync decision |
| `chain/client/src/client.rs:1225` | `note_verified_peer_height` | record a peer height proven by approvals |
| `chain/client/src/client.rs:1732` | `check_and_update_doomslug_tip` | feed head → `set_tip` |
| `chain/client/src/client.rs:1810` | `send_block_approval` | route approval, incl. previous-epoch skip target |
| `chain/client/src/client.rs:2401` | `collect_block_approval` | validate + route incoming approvals |
| `chain/client/src/client_actor.rs:635` | `BlockResponse` handler | feed unsolicited blocks to the verified-height signal |
| `chain/client/src/client_actor.rs:1289` | (block production loop) | `ready_to_produce_block` → `produce_block` |
| `chain/client/src/client_actor.rs:1504` | `try_doomslug_timer` | drive `process_timer`, persist target height, send |
| `chain/client/src/client_actor.rs:1714` | `syncing_info` | verified vs claimed peer heights, own header head |
| `chain/client/src/client_actor.rs:1755` | `head_is_stale` | ~1 epoch of wall clock without head progress |
| `chain/client/src/verified_peer_heights.rs:31` | `record_if_verified` | per-peer proven height + per-header LRU |
| `core/store/src/light_client_block.rs:22` | `StoredLightClientBlock` | versioned on-disk light client block (2.14) |
| `core/primitives-core/src/version.rs:437` | `ValidateBlockOrdinalAndEpochSyncDataHash` | v85 header-validation gate |
| `core/primitives-core/src/version.rs:652` | `MIN_SUPPORTED_PROTOCOL_VERSION` | 84 |
| `core/primitives-core/src/version.rs:680` | `STABLE_PROTOCOL_VERSION` | 87 |

## Open questions

- The docstring at `chain/chain/src/doomslug.rs:137` still references a `PersistentDoomslug` struct that does not exist anywhere in this tree; persistence of `largest_target_height` is handled directly in `try_doomslug_timer` via the chain store. (The previous revision of this spec also blamed `chain/chain/AGENTS.md:12`; that file no longer mentions it — corrected.)
- ~~`verify_approvals_and_threshold_orphan` has no length guard~~ — **resolved** (see *Invariants & failure modes*). The guard is genuinely absent and genuinely reachable, but it is not a consensus-safety issue. `verify_approvals_and_threshold_orphan` has exactly **one** production caller in the tree — `Chain::verify_header_approvals_without_ancestry` (`chain.rs:4192`), itself called only from `Client::note_verified_peer_height` (`client.rs:1233`); the second potential caller in the orphan-admission path is commented out (`chain.rs:2468`), and the only other reference is the unit test at `approval_verification.rs:123`. That single caller *does* check the producer signature first (`chain.rs:4199`), but that check does not constrain the approvals vector's length, so truncation is reachable — and harmless, because the truncated approvals are excluded from the stake tally as well as from signature verification.
- `docs/ChainSpec/Consensus.md` (unchanged since 2.13) models finality abstractly and uses the term "block proposer"; it appears broadly consistent with the code's two-consecutive-heights rule but was not line-by-line reconciled here — flagged as potentially needing a freshness pass. It says nothing about the new verified-peer-height sync gating.
- ~~test-loop coverage for the ported light-client tests (#15971)~~ — **resolved**: the ported tests live in `test-loop-tests/src/tests/light_client.rs` (`test_next_light_client_block` `:94`, `test_next_light_client_block_epoch_boundary` `:185`, `test_light_client_execution_outcome_proof` `:266`, `..._across_resharding` `:374`), with the NEP-25 check — recomputed block hash, approval signatures against the epoch's block producers, >2/3 stake, `next_bp_hash` on epoch change — in `validate_light_client_block` (`:42`). The corresponding pytests were removed.
