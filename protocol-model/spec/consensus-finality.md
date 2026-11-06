# Consensus & finality (Doomslug)

> Protocol version: 88 (stable) · Release: 2.14.0 · Derived from commit: bc4e9e8 · Generated: 2026-11-03
> Primary crates/files: `chain/chain/src/doomslug.rs`, `core/primitives/src/block_header.rs`, `core/primitives/src/block.rs`, `chain/chain/src/approval_verification.rs`, `chain/chain/src/signature_verification.rs`, `chain/chain/src/chain.rs`, `chain/chain/src/lightclient.rs`, `chain/client/src/client.rs`, `chain/client/src/client_actor.rs`, `chain/client/src/verified_peer_heights.rs`

## Role

Doomslug is NEAR's finality gadget. It decides (a) *when* a block producer is allowed to produce a block at a given height — namely once it has collected approvals from more than 2/3 of the stake for that height — and (b) *which* ancestor blocks are final. Each block carries a vector of `approvals` (one slot per epoch block-approver) plus two derived fields, `last_ds_final_block` and `last_final_block`, that record the finality frontier at that block. Approvals are gossiped between block producers via networking; the block producer schedule for a height comes from epoch management ([epoch-validators-staking](epoch-validators-staking.md)); accepted blocks flow into the block-processing pipeline ([chain-block-processing](chain-block-processing.md)). The in-memory logic lives in `Doomslug` (no chain/storage coupling), driven by the client actor's periodic timer. Since 2.14 the same header-approval check is also reused *outside* block processing, as a proof-of-height signal that gates sync ([sync](sync.md); see [Verified peer heights](#verified-peer-heights-sync-gating)). Header *admission* — which `BlockHeader` enum variants `validate_header` will accept in an epoch — is also described here, because the approval vector and the finality fields live in the header and the one new gate in this release, `RejectBlockHeaderV7BeforeSpice` (v87), is enforced inside `validate_header`.

## Key data structures

- **`ApprovalInner`** — `core/primitives/src/block_header.rs:533` — an enum with two variants encoding *what kind* of approval this is: `Endorsement(CryptoHash)` (approve building directly on the block with that hash) or `Skip(BlockHeight)` (skip the parent height, approve producing a block some heights later). `ApprovalInner::new` (`:566`) chooses `Endorsement(parent_hash)` iff `target_height == parent_height + 1`, else `Skip(parent_height)`.
- **`Approval`** — `core/primitives/src/block_header.rs:542` — a signed approval: `{ inner: ApprovalInner, target_height: BlockHeight, signature: Signature, account_id: AccountId }`. Built by `Approval::new` (`:580`); the signed message is `borsh(inner) ++ target_height.to_le_bytes()` via `Approval::get_data_for_sig` (`:592`). Note the signature does **not** cover `account_id`, so validation must independently check the signer key belongs to the claimed account.
- **`ApprovalType`** — `core/primitives/src/block_header.rs:551` — `SelfApproval` / `PeerApproval(PeerId)`; decides whether the signature check is skipped (we made it) or mandatory.
- **`ApprovalMessage`** — `core/primitives/src/block_header.rs:560` — `{ approval, target: AccountId }`; the wire wrapper directing an approval to the next block producer.
- **`DoomslugThresholdMode`** — `chain/chain/src/doomslug.rs:46` — `TwoThirds` (production; block requires >2/3 approving stake, guarantees finality) or `NoApprovals` (tests; block production not gated on approvals).
- **`DoomslugBlockProductionReadiness`** — `chain/chain/src/doomslug.rs:53` — `NotReady` or `ReadySince(Instant)` (the moment the threshold was first crossed; used to time chunk waiting).
- **`Doomslug`** — `chain/chain/src/doomslug.rs:139` — the finality state machine. Holds `threshold_mode`, per-height `approval_trackers`, the current `tip` (`DoomslugTip`, `:68`), a `timer` (`DoomslugTimer`, `:58`), `endorsement_pending`, a debug `history`, and four tracked heights: `largest_target_height` (highest height we issued an approval for; persisted to guard against equivocation on restart), `largest_final_height`, `largest_threshold_height`, `largest_approval_height`.
- **`DoomslugApprovalsTrackersAtHeight`** — `chain/chain/src/doomslug.rs:130` — one per `target_height`; keyed by `ApprovalInner`, holds a `DoomslugApprovalsTracker` per distinct parent, plus `last_approval_per_account` enforcing **one approval per account per target height**.
- **`DoomslugApprovalsTracker`** — `chain/chain/src/doomslug.rs:73` — accumulates `approved_stake_{this,next}_epoch` against `total_stake_{this,next}_epoch`, keeps a `witness: HashMap<AccountId, (Approval, Utc)>`, and records `time_passed_threshold`.
- **`ChunksReadiness`** — `chain/chain/src/doomslug.rs:87` — `Ready(Instant)` / `NotReady`; gates how long a ready-by-approvals producer waits for chunks.
- **`VerifiedPeerHeights`** — `chain/client/src/verified_peer_heights.rs:11` — per-peer highest *proven* height, plus a 32-entry LRU of header hashes whose approvals already verified so each distinct header costs one signature pass however many peers relay it.
- **Block header finality fields** — every `BlockHeader` version (V1–V8) stores `last_final_block` and `last_ds_final_block` in `inner_rest`; accessed via `last_final_block()` (`core/primitives/src/block_header.rs:1677`) and `last_ds_final_block()` (`:1702`). Approvals are read via `approvals()` (`:1758`), the parent height via `prev_height()` (`:1407`, `None` for V1/V2). Every variant from V3 onward exposes an approvals vector of the same shape, so the approval/finality logic below is header-version-agnostic.
- **`BlockHeaderV8`** — `core/primitives/src/block_header.rs:807` — new at 2.14.0 final (#16621). It is now the **SPICE** header: `inner_lite: BlockHeaderInnerLiteV2` (`:61`, adds `chunk_execution_root`) plus `inner_rest: BlockHeaderInnerRestV8` (`:466`, field-for-field identical to `BlockHeaderInnerRestV7`, duplicated so later V8 edits cannot move the V7 layout). `BlockHeader::is_spice()` (`:1989`) returns `true` only for V8, and `chunk_execution_root()` (`:1889`) returns `Some` only for V8. Its approval and finality fields (`approvals`, `last_final_block`, `last_ds_final_block`, `prev_height`, `block_ordinal`) are the same as V6/V7, so nothing in the Doomslug path branches on it. The serialization shape is documented in [data-structures-serialization](data-structures-serialization.md).
- **`BlockHeaderV7`** — `core/primitives/src/block_header.rs:777` — at 2.14.0 final V7 is a **dead variant**: `BlockHeader::new_impl` constructs only V8 (when `Spice` is enabled, `:1179`), V6 (when `DynamicResharding` is enabled — stable since v85, so this is the live stable header, `:1209`) or V5 (`:1241`). No code path produces V7. Its `inner_lite` was changed back from `BlockHeaderInnerLiteV2` to plain `BlockHeaderInnerLite`, making the V7 borsh layout byte-identical to the one 2.13 shipped, and `is_spice()` now returns `false` for it. It remains decodable, which is why it needs an explicit rejection rule (see [Header version admission](#header-version-admission)).

## Behavior

### Approval production (the timer)

`Doomslug::process_timer` (`chain/chain/src/doomslug.rs:489`) is the heart of the state machine, called periodically (every `doomslug_step_period`) from `ClientActor::try_doomslug_timer` (`chain/client/src/client_actor.rs:1525`). It runs at most `MAX_TIMER_ITERS = 20` iterations (`:19`) per call to avoid unbounded loops.

1. Each iteration computes `skip_delay = timer.get_delay(timer.height - largest_final_height)` (`:493`). `get_delay` (`:176`) returns `min(max_delay, min_delay + (min_delay/10) * (n-2))` — the delay grows linearly with how many heights have passed since the last doomslug-final block, capped at `max_delay`.
2. **Endorsement path** (`:505`): if `endorsement_pending` and `now >= last_endorsement_sent + endorsement_delay`, and the tip height `>= largest_target_height`, it bumps `largest_target_height` to `tip_height + 1` and creates an `Endorsement` approval for `target_height = tip_height + 1` (`create_approval`, `:565`). Then clears `endorsement_pending`. This is the "approve the block we just accepted" path; endorsements are deliberately staggered by `endorsement_delay` rather than sent instantly on block receipt (`debug_assert!(skip_delay >= 2 * endorsement_delay)`, `:501`).
3. **Skip path** (`:532`): if `now >= timer.started + skip_delay`, it emits a `Skip` approval for `timer.height + 1`, raises `largest_target_height` to at least that, then advances `timer.started += skip_delay` and `timer.height += 1`. This is how the network moves past a height whose block never arrived.
4. Otherwise it `break`s (`:558`).

Returned approvals are sent by `try_doomslug_timer`: it first **persists `largest_target_height`** to the store (`client_actor.rs:1535`) and commits *before* sending, so a crash/restart cannot cause the node to sign a lower target height and be slashed. Approvals are only sent if we are a validator in the head epoch or the next epoch (`client_actor.rs:1540`). Sending goes through `Client::send_block_approval` (`chain/client/src/client.rs:1810`) → `send_block_approval_to_account` (`:1786`), which either self-collects (`ApprovalType::SelfApproval`, `:1794`) or gossips an `ApprovalMessage` to the next block producer via `NetworkRequests::Approval` (`:1805`). When the final head is still in the previous epoch, a `Skip` approval is additionally sent to the previous epoch's producer for that target height, for liveness across a not-yet-final epoch transition (`client.rs:1826`).

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

`Doomslug::ready_to_produce_block` (`doomslug.rs:722`) is called from `ClientActor` (`client_actor.rs:1310`) for every height from `latest_known + 1` up to `get_largest_height_crossing_threshold()` where we are the block producer. It looks up the tracker for `(tip, target_height)`, requires `get_block_production_readiness == ReadySince(when)` (i.e. >2/3 approvals doomslug-finalizing the previous block), then:
- if chunks are `Ready`, produce immediately (`:773`);
- otherwise wait until `now > when + chunk_wait_delay`, where `chunk_wait_delay = get_delay(...) * chunk_wait_mult` (`:778`). This lets a producer proceed with missing chunks after a bounded wait rather than stall.

Under SPICE (PV 180 — reachable only in a `protocol_feature_spice` build, see [Protocol-version-gated behavior](#protocol-version-gated-behavior)) this is additionally conjoined with `spice_timer.ready_to_produce_block` (`client_actor.rs:1299`); on stable that term is unconditionally `true` (`:1307`).

When actually producing, `Client::produce_block_on` fills the header `approvals` vector from the doomslug witness for `(prev_hash, prev_height, height)` (`client.rs:1027`, `get_witness` `doomslug.rs:618`), mapping each ordered epoch approver to its signature slot (`client.rs:1035`) and `debug_assert`ing that the witness is fully consumed (`:1044`).

### Computing finality fields in a new block

`Block::produce` (`core/primitives/src/block.rs:113`):
- `last_ds_final_block` = `prev.hash()` if `height == prev.height() + 1` (i.e. the new block directly follows its parent with no skipped height), else it inherits `prev.last_ds_final_block()` (`:219`). So a block is **doomslug-final** exactly when its child follows at the immediately next height.
- `last_final_block` = `prev.last_final_block_for_height(height)` (`:222`). `last_final_block_for_height` (`block_header.rs:1693`) returns `prev.prev_hash()` when `target_height == prev.height() + 1` **and** `prev.last_ds_final_block() == prev.prev_hash()` — i.e. two consecutive-height blocks in a row (BFT-style finality: a block is final once it has a child at the next height that itself has a child at the next height). Otherwise it inherits `prev.last_final_block()`.

Doomslug (single confirmation, ds-final) gives fast optimistic finality; `last_final_block` (the two-consecutive-heights rule) gives the stronger BFT finality that is never reverted.

### Header version admission

Before `validate_header` (`chain/chain/src/chain.rs:885`) looks at approvals or finality fields at all, it decides whether the header's *enum variant* is admissible in the epoch. The order is load-bearing: `challenges_present()` → `Error::InvalidChallenge` (`chain.rs:887`), then the future-timestamp bound (`Error::InvalidBlockFutureTime`, `:891`), then the producer signature (`Error::InvalidSignature`, `:898`), then the two epoch-id checks (`Error::InvalidEpochHash`, `:908`/`:914`), then the three version gates below, then `next_bp_hash` / chunk mask / `prev_height` / timestamp monotonicity, and finally (only for `provenance != PRODUCED`) approvals, threshold, finality fields, merkle root, block ordinal and chunk endorsements.

1. `header.latest_protocol_version() < epoch_protocol_version` → `Error::InvalidProtocolVersion` (`chain.rs:920`), logged at `error` level as "header protocol version smaller than epoch protocol version" (`:924`).
2. **Spice-ness must match the epoch, in both directions** (`chain.rs:928`–`:938`): `spice_epoch = ProtocolFeature::Spice.enabled(epoch_protocol_version)` must equal `header.is_spice()`, else `Error::InvalidProtocolVersion` with the log line "header spice-ness does not match its epoch". This check is **ungated** — it applies at every protocol version. The check's text is unchanged from 2.14.0-rc.1, but its *coverage* changed with `is_spice()`: at rc.1 `is_spice()` was true for V7, so this one check already rejected V7 in a non-spice epoch; at 2.14.0 final V7 is non-spice, so a V7 header in a non-spice epoch now passes it and would be handled exactly like a V6 header. Hence step 3.
3. **`RejectBlockHeaderV7BeforeSpice`** (`chain.rs:941`–`:950`): when enabled for `epoch_protocol_version` (**v87**), any `BlockHeader::BlockHeaderV7` is rejected with `Error::InvalidProtocolVersion` and the warning "block header v7 is not valid in this epoch". Below v87 the gate is off and a V7 header is accepted and processed as a V6-equivalent header, which is what nodes running 2.13 do. The gate deliberately reproduces 2.13's permissive behaviour until the whole network has voted to v87, rather than diverging from it immediately. In a spice epoch the V7 rejection is redundant: step 2 already rejects V7 there, since `is_spice()` is false for it.
   - Asserted end-to-end by `block_header_v7_accepted_before_reject_feature` (`test-loop-tests/src/tests/block_header_v7_before_spice.rs:87` — at v86 a re-signed V7 copy of a V6 block becomes the canonical block at its height on all four nodes) and `block_header_v7_rejected_with_reject_feature` (`:108` — at v87 no node stores a V7 header, and the node that broadcast it falls behind).

Only test utilities (`test-utils/testlib/src/process_blocks.rs:111`) and `block_header.rs`'s per-variant accessor arms mention `BlockHeaderV7` elsewhere; the one place production code matches on it *specifically* is `chain.rs:942`. (`block.rs:232` lists V7 only inside a `debug_assert` arm shared with V3–V6 and V8.)

#### Why 2.13 accepts a V7 header and 2.14.0-rc.1 does not

The three binaries disagree, and the disagreement is entirely about byzantine input — no honest producer emits V7 or V8 anywhere in the stable range, because `BlockHeader::new_impl` picks V8 only when `Spice` (v180) is enabled, V6 when `DynamicResharding` (v85, `version.rs:620`) is, and V5 otherwise (`block_header.rs:1135`/`:1179`/`:1209`/`:1241`). At PV 84 honest producers emit V5 and at PV 85–88 they emit V6, so honest traffic never diverges across any of these binaries.

| Binary | V7 layout | `is_spice()` for V7 | V7 at PV ≤ 86 | V7 at PV ≥ 87 |
|---|---|---|---|---|
| 2.13.x | `BlockHeaderInnerLite` + `BlockHeaderInnerRestV7` | `true` | **accepted**, handled as V6 — its spice check is one-directional (`Spice.enabled(epoch_pv) && !header.is_spice()`, 2.13.4 `chain.rs:915`), so a spice-ish header in a non-spice epoch is not caught | same (no gate exists) |
| 2.14.0-rc.1 (BASE) | `BlockHeaderInnerLiteV2` + `BlockHeaderInnerRestV7` | `true` | **rejected** — the check became bidirectional (BASE `chain.rs:929`), so `false != true` fires | **rejected**, same check (rc.1 has no `RejectBlockHeaderV7BeforeSpice`) |
| 2.14.0 final (TARGET) | `BlockHeaderInnerLite` + `BlockHeaderInnerRestV7` (byte-identical to 2.13) | `false` | **accepted**, handled as V6 — step 2 passes (`false == false`) | **rejected** by step 3 |

So the interop statement is: at PV ≤ 86 an rc.1 node rejects a byzantine V7 that a 2.14.0-final (and a 2.13) node accepts as V6; at PV ≥ 87 all three agree to reject except 2.13, which has no gate and still accepts. Note also that rc.1's V7 `inner_lite` carries an extra trailing `chunk_execution_root` (`BlockHeaderInnerLiteV2`, `block_header.rs:61`), so rc.1 and 2.14.0-final do not even agree on the *byte layout* of header tag 7: a V7 header produced for one is overwhelmingly likely to fail borsh decoding at the other (the 32-byte shift lands inside `inner_rest`'s length-prefixed `Vec` fields) before any validation rule is reached.

### Light client blocks

`Chain::create_light_client_block` (`chain/chain/src/chain.rs:694`) picks the last final block as seen from a header, walking back further if the final block is within two heights of an epoch change, and `create_light_client_block_view` (`chain/chain/src/lightclient.rs:34`) packages it. The proof that the light client checks is exactly the Doomslug two-consecutive-heights rule: the view carries the block's inner-lite/inner-rest hashes, the *next* block's inner hash, and `approvals_after_next` — the approvals of the block two heights later (`lightclient.rs:62`). The `chunk_execution_root` field of the view is `block_header.chunk_execution_root()` (`lightclient.rs:49`), i.e. `None` outside SPICE.

New in 2.14 (#16394/#16396): the on-disk form in `DBCol::EpochLightClientBlocks` is now a versioned `StoredLightClientBlock` enum (`core/store/src/light_client_block.rs:22`) with existing rows migrated, decoupling the never-GC'd stored rows from the RPC-shaped `LightClientBlockView`. This is a storage-format change only — no consensus rule changed.

### Verified peer heights (sync gating)

New in 2.14 (#16133, narrowed by #16266). A peer's advertised height used to be taken at face value when deciding to enter sync; it is now cross-checked against block approvals where possible.

1. On an **unsolicited** `BlockResponse`, `ClientActor` calls `Client::note_verified_peer_height` before any drop path (`chain/client/src/client_actor.rs:636`). Requested blocks are skipped — they prove nothing we did not already know.
2. `note_verified_peer_height` (`chain/client/src/client.rs:1225`) prunes entries at or below our head, ignores heights that could not change the sync decision (`peer_height_requires_sync`, `:1209` — `peer_height > head + threshold`, with `threshold = 0` while already syncing), and otherwise records the peer's height if `Chain::verify_header_approvals_without_ancestry` succeeds.
3. `Chain::verify_header_approvals_without_ancestry` (`chain/chain/src/chain.rs:4204`) checks the producer signature first (`partial_verify_orphan_header_signature`, `:844`) because the header hash commits to the approvals, then requires `header.prev_height()` to exist (V1/V2 headers are rejected with `Error::Other("header too old to verify approvals without ancestry")`), and delegates to `verify_approvals_and_threshold_orphan` with the header's own epoch info. A header from an epoch we don't know errors out, so far-ahead blocks simply never become "verified".
4. `ClientActor::syncing_info` (`client_actor.rs:1735`) then chooses between two sources. If `head_is_stale` (`:1776`) — our head's producer-signed timestamp is at least one epoch of `min_block_production_delay` old — it falls back to the unvalidated `highest_height_peers` (`sync_requirement_from_claimed_peers`, `:1790`), since a peer cannot fake our own clock. Otherwise it uses `sync_requirement_from_verified_peers` (`:1814`), capping each connected peer's claimed height by what we proved for it (uncapped peers read as level with us, so they are never a sync target). After state sync, when every verified height sits below the new head, our own header head is used as the sync target instead (`:1766`).

## Interactions

- **Consumes**: the block-approver set and per-approver stakes for a parent block from [epoch-validators-staking](epoch-validators-staking.md) via `get_epoch_block_approvers_ordered`; the chain head/final head from [chain-block-processing](chain-block-processing.md); chunk readiness from chunk production ([sharding-chunks](sharding-chunks.md)).
- **Produces**: `ApprovalMessage`s gossiped over the network layer; the `approvals` vector, `last_final_block`, and `last_ds_final_block` embedded in each produced block header, consumed downstream by header/block validation and light-client generation; the per-peer verified-height signal consumed by the sync state machine ([sync](sync.md)).
- Who the block producer *is* for a height is decided in [epoch-validators-staking](epoch-validators-staking.md).
- Chunk endorsements (>2/3 per-chunk stake) are a **separate** mechanism from block approvals and are validated in block preprocessing — see [stateless-validation](stateless-validation.md). In particular, 2.14's fix to derive chunk-endorsement signed bytes from `chunk_hash` (#16193) does not touch `Approval::get_data_for_sig`; the block-approval signed message is unchanged.
- Chunk-header signature verification now resolves the producer from a grandparent anchor / the `ChunkProducers` DB column (`chain/chain/src/signature_verification.rs:161` `resolve_anchored_producer`, `:190` `resolve_and_verify_anchored_producer`); that is chunk-level and does not affect block approvals — see [sharding-chunks](sharding-chunks.md).
- **State-sync block admission (#16418) is not part of this component.** 2.14.0 final rearranged `ClientActor::maybe_receive_state_sync_blocks` (`chain/client/src/client_actor.rs:2104`) so a block received during state sync is first classified by *where it belongs* and only then verified, by the new `validate_state_sync_block` (`:2060`): `Chain::verify_block_hash_and_signature` (`chain/chain/src/chain.rs:1083`) — body hash + header producer signature, banning the sender with `ReasonForBan::BadBlockHeader` on mismatch — followed by `Chain::validate_block`. That path never runs `validate_header`, so it performs **no** approval, threshold or finality-field check and does not interact with Doomslug. It belongs to [sync](sync.md) and [chain-block-processing](chain-block-processing.md). The companion `Chain::get_orphan` accessor (`chain/chain/src/orphan.rs:458`) has no production caller in this tree — only `test-loop-tests/src/tests/sync/adversarial_sync_block.rs:104`.

## Protocol-version-gated behavior

The core Doomslug approval/finality algorithm is **not** protocol-version-gated in this tree — `get_block_production_readiness`, `can_approved_block_be_produced`, and the `last_final_block`/`last_ds_final_block` computation contain no `ProtocolFeature` checks. `chain/chain/src/doomslug.rs` is byte-for-byte unchanged between 2.14.0-rc.1 (v87) and 2.14.0 final (v88). Adjacent header-validation behavior that *is* gated (verified against `core/primitives-core/src/version.rs`):

- **`RejectBlockHeaderV7BeforeSpice`** — variant at `version.rs:505`, activates at **v87** (`version.rs:642`). **New in 2.14.0 final.** From v87, `validate_header` rejects any `BlockHeader::BlockHeaderV7` with `Error::InvalidProtocolVersion` (`chain/chain/src/chain.rs:941`). Below v87 a V7 header in a non-spice epoch is accepted and handled exactly like a V6 header. See [Header version admission](#header-version-admission). This is the only new v87/v88 feature that touches this component.
- **`ValidateBlockOrdinalAndEpochSyncDataHash`** — variant at `version.rs:437`, activates at **v85** (listed in the `=> 85` match arm, `version.rs:625`/`:631`). In `validate_header` (`chain/chain/src/chain.rs:1039`), when enabled the header's `block_ordinal` must equal `block_merkle_tree.size() + 1` and its `epoch_sync_data_hash` must match the locally recomputed value. Pre-v85 these two checks are skipped. Does not affect approval math.
- **`Spice`** — **v180** (`version.rs:354` / `:656`). Not reachable on the stable path (88) *or* on nightly (157): `PROTOCOL_VERSION` is `SPICE_PROTOCOL_VERSION = 200` only in a build with the `protocol_feature_spice` cargo feature, which is deliberately kept out of nightly (`PROTOCOL_VERSION`, `version.rs:710`; `SPICE_PROTOCOL_VERSION`, `:707`). In `validate_header` the header's spice-ness must *match* its epoch in both directions (`chain.rs:928`–`:938`), and a spice epoch dispatches to `validate_spice_chunk_endorsements_in_header` instead of `validate_chunk_endorsements_in_header` (`chain.rs:1054`). SPICE's header is now **`BlockHeaderV8`** (`BlockHeaderInnerLiteV2` + `BlockHeaderInnerRestV8`, carrying `chunk_execution_root` and the per-chunk execution-root commitment), produced only when `Spice.enabled(..)` (`core/primitives/src/block_header.rs:1135`/`:1179`); `BlockHeaderV7` was demoted to a non-spice dead variant in the same change. SPICE also gates block production on an extra `spice_timer` (`client_actor.rs:1299`). None of this is reachable at PV 88.
- **`EarlyKickout`** — stable at **v87** (`version.rs:639`). It changes how a *chunk* producer is resolved (the `ChunkProducers` DB column, DB version 50) and early kickout/blacklisting; it does not change block-approval collection, the >2/3 rule, or the finality fields. See [epoch-validators-staking](epoch-validators-staking.md) and [sharding-chunks](sharding-chunks.md).
- **`PostQuantumSignatures`** — v85 (`version.rs:408` / `:623`) — touches signatures, but is scoped to *transaction* / access-key signatures (`core/primitives/src/transaction.rs:375`, `:508`). Block approvals are signed with the validator key via `Approval::new` and verified with `verify_validator_signature`; neither is gated on it.
- Optimistic block production (the former `_DeprecatedProduceOptimisticBlock`, historically PV 77 — `version.rs:305` / `:596`) is folded in unconditionally: `Block::produce` will take VRF/timestamp/randomness from an optimistic block when available (`block.rs:203`) but the finality-field computation is identical. There is no live `ProduceOptimisticBlock` gate.

### The v87 / v88 feature sets

`STABLE_PROTOCOL_VERSION = 88` (`version.rs:698`, raised from 87), `MIN_SUPPORTED_PROTOCOL_VERSION = 84` (`version.rs:670`, unchanged), `NIGHTLY_PROTOCOL_VERSION = 157` (`:701`), `SPICE_PROTOCOL_VERSION = 200` (`:707`).

| Version | Feature | Touches consensus/finality? |
|---|---|---|
| 87 | `FixContractLoadingError`, `RejectEmptyMethodName`, `RejectDelegateV2`, `RejectWithdrawFromGasKeyInDelegate`, `RemoveGasRewards`, `EnforceStorageProofLimitForAllActions`, `GlobalContractSameChunkCallFix` (`version.rs:633`–`:638`, `:640`) | No — runtime / action validation. See [runtime-execution](runtime-execution.md). |
| 87 | `EarlyKickout` (`version.rs:639`) | No — chunk-producer assignment; see [epoch-validators-staking](epoch-validators-staking.md). |
| 87 | `RejectUniversalAccountTransfers` (`version.rs:641`) | No — action validation for `0u` account transfers; see [runtime-execution](runtime-execution.md), [accounts-keys](accounts-keys.md). |
| 87 | **`RejectBlockHeaderV7BeforeSpice`** (`version.rs:642`) | **Yes** — header admission in `validate_header`. |
| 88 | `UniversalAccounts` (`version.rs:643`) — **moved from 87 to 88** | No — account model; see [accounts-keys](accounts-keys.md). |
| 88 | `RefundRepeatedUniversalStateInitDeposit` (`version.rs:644`) | No — runtime refund accounting. |
| 88 | `CanonicalUniversalStateInit` (`version.rs:645`) | No — action/encoding validation and fees. |

Two features that 2.14.0-rc.1 had stabilized at 87 were **de-stabilized to nightly** in 2.14.0 final and are therefore *not* active at stable 88: `FixMlDsaCostCharging` and `ReceiptPromiseInputSizeLimit`, both now at **157** (`version.rs:652`–`:653`). Neither touches consensus or finality — they are gas-charging and receipt-size features (see [runtime-execution](runtime-execution.md)). Note `CHANGELOG.md`'s `## [2.14.0] / ### Protocol Changes` still carries an ML-DSA cost-charging bullet (#16064) that no longer describes the stable release; the code is authoritative.

### Activation schedule

`get_protocol_upgrade_schedule` (`core/primitives/src/version.rs:37`) is populated for both networks, and 2.14.0 activates in **two steps two days apart**:

| Network | → 87 | → 88 |
|---|---|---|
| mainnet (`version.rs:59`–`:71`) | 2026-11-04 00:00 UTC | 2026-11-06 00:00 UTC |
| testnet (`version.rs:72`–`:84`) | 2026-09-23 00:00 UTC | 2026-10-21 00:00 UTC |

For this component the first step is the operative one: `RejectBlockHeaderV7BeforeSpice` arms at 87, i.e. in the first epoch whose protocol version is ≥ 87, two days before `UniversalAccounts` turns on at 88. The gate is evaluated on `epoch_protocol_version` (the epoch's agreed version), not on the node's binary version, so every node in an epoch applies the same rule.

## Invariants & failure modes

- **>2/3 stake to produce/finalize.** Enforced in `get_block_production_readiness` (`doomslug.rs:267`) at production time and re-checked at validation time by `Doomslug::can_approved_block_be_produced` (`doomslug.rs:584`); a header with insufficient approving stake is rejected with `Error::NotEnoughApprovals` (`chain/chain/src/chain.rs:1009`). The check is on both this-epoch and next-epoch stakes. Note the two sites are not textually identical: `can_approved_block_be_produced` passes an epoch vacuously when *that epoch's* 2/3 threshold is zero (`doomslug.rs:614` — `approved > threshold || threshold.is_zero()`, applied to both epochs), whereas `get_block_production_readiness` allows the vacuous case only for the next epoch (`doomslug.rs:272`).
- **Valid approval signatures & correct signer.** During block validation `verify_approval_with_approvers_info` (`chain/chain/src/approval_verification.rs:11`) reconstructs the signed message from `(prev_hash, prev_height, height)` and verifies every present signature against the corresponding ordered approver's key; failure → `Error::InvalidApprovals` (`chain.rs:995`). The approver order must match `get_epoch_block_approvers_ordered`; more approvals than approvers → immediate reject (`approval_verification.rs:18`). The no-ancestry path uses `verify_approvals_and_threshold_orphan` (`approval_verification.rs:42`), which returns `Error::InvalidApprovals` / `Error::NotEnoughApprovals`; it zips against a *heuristic* approver list (`get_heuristic_block_approvers_ordered`, `approval_verification.rs:87` — current-epoch `block_producers_settlement` deduped, with `get_approval_stake(false)`) and deliberately carries **no** `approvals.len() > block_approvers.len()` guard.
  - That truncation is reachable on honest input: the real ordered list (`EpochManager::get_all_block_approvers_ordered`, `chain/epoch-manager/src/lib.rs:1504`) appends the *next* epoch's settlement near an epoch boundary, so an honest header's `approvals` can be longer than the heuristic list. It is trivially reachable on hostile input too, since a byzantine block producer signs whatever approvals vector it likes and the producer-signature pre-check (`chain.rs:4211`) only proves *who* built the header, not that the vector is canonical.
  - It is nevertheless **not** a threshold-bypass: `zip` pairs index *i* with index *i*, so no approval is ever credited to the wrong approver, and the threshold is evaluated by `Doomslug::can_approved_block_be_produced` (`doomslug.rs:584`), which itself zips `approvals` against stakes derived from the **same** truncated `block_approvers` list (`approval_verification.rs:76`, `doomslug.rs:600`/`:607`). Extra approvals therefore contribute zero stake — the check can only become stricter, never weaker, and the worst case is a false negative. The path also never admits a block to the chain; its sole production caller only records a peer height for sync gating (see below).
- **`prev_height` sanity.** `validate_header` rejects a header whose `prev_height` disagrees with the actual parent (`Error::Other("Invalid prev_height")`, `chain.rs:974`). On the no-ancestry path there is no parent to compare against, so since 2.14 (#16389) `verify_approvals_and_threshold_orphan` uses `checked_add` and returns `Error::InvalidBlockHeight(prev_block_height)` for `prev_height == u64::MAX` instead of panicking on overflow and aborting the node (`approval_verification.rs:57`). Asserted by `verify_approvals_and_threshold_orphan_rejects_max_prev_height` (`approval_verification.rs:114`).
- **Finality fields must be reproducible.** `validate_header` recomputes the expected `last_ds_final_block` and `last_final_block` from `prev_header` and rejects mismatches with `Error::InvalidFinalityInfo` (`chain.rs:1012`–`:1029`). This mirrors the production-side computation in `Block::produce`.
- **Block header signature.** `verify_block_header_signature_with_epoch_manager` (`chain/chain/src/signature_verification.rs:78`) checks the header signature against the height's block producer; mismatch → `Error::InvalidSignature` (`chain.rs:898`). Orphan candidates get the cheaper, non-slashing-aware `partial_verify_orphan_header_signature` (`chain.rs:844`, used at `:2475` and `:4211`).
- **Header variant must be admissible in the epoch.** `validate_header` rejects a header whose `is_spice()` disagrees with `ProtocolFeature::Spice.enabled(epoch_protocol_version)` in either direction (`chain.rs:928`–`:938`), and from **v87** rejects `BlockHeaderV7` outright (`chain.rs:941`–`:950`); both return `Error::InvalidProtocolVersion`. Below v87 a V7 header is admissible and is treated as a V6 header, which is what 2.13 nodes do (for a different reason — see [Why 2.13 accepts a V7 header…](#why-213-accepts-a-v7-header-and-2140-rc1-does-not)) — the gate exists so the stricter rule takes effect only once the epoch's agreed protocol version is 87. Asserted by `block_header_v7_accepted_before_reject_feature` / `block_header_v7_rejected_with_reject_feature` (`test-loop-tests/src/tests/block_header_v7_before_spice.rs:87`, `:108`). A node that broadcasts a V7 header at ≥ v87 forks itself off: the test asserts its final head stays below the V7 height while the other validators advance past it.
- **Finalizability pre-check.** `check_if_finalizable` (`chain.rs:857`) walks up to `NUM_PARENTS_TO_CHECK_FINALITY = 20` (`chain.rs:129`) ancestors; a header whose height is already below the final head and that does not reach it is rejected with `Error::CannotBeFinalized`.
- **No equivocation across restart.** `largest_target_height` is persisted and committed before approvals are sent (`client_actor.rs:1535`), and `set_tip` `debug_assert`s monotonic tip height (`doomslug.rs:649`); the node never signs a target height below one it already committed.
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
| `core/primitives/src/block_header.rs:61` | `BlockHeaderInnerLiteV2` | SPICE-only inner-lite with `chunk_execution_root`, used by V8 |
| `core/primitives/src/block_header.rs:466` | `BlockHeaderInnerRestV8` | V8 inner-rest; same fields as V7, kept separate |
| `core/primitives/src/block_header.rs:533` | `ApprovalInner` | Endorsement vs Skip |
| `core/primitives/src/block_header.rs:566` | `ApprovalInner::new` | Endorsement iff target == parent+1 |
| `core/primitives/src/block_header.rs:580` | `Approval::new` | sign inner ++ target_height |
| `core/primitives/src/block_header.rs:592` | `Approval::get_data_for_sig` | signed message layout |
| `core/primitives/src/block_header.rs:777` | `BlockHeaderV7` | dead variant: non-spice, 2.13-compatible layout, never produced |
| `core/primitives/src/block_header.rs:807` | `BlockHeaderV8` | the SPICE header from 2.14.0 final |
| `core/primitives/src/block_header.rs:1135` | `BlockHeader::new_impl` (spice branch) | V8 produced iff `Spice` enabled; else V6 (`:1209`) or V5 (`:1241`) |
| `core/primitives/src/block_header.rs:1407` | `prev_height` | `None` for V1/V2 headers |
| `core/primitives/src/block_header.rs:1693` | `last_final_block_for_height` | two-consecutive-heights final rule |
| `core/primitives/src/block_header.rs:1758` | `approvals` | header approval vector, all variants V3+ |
| `core/primitives/src/block_header.rs:1889` | `chunk_execution_root` | `Some` only for V8 |
| `core/primitives/src/block_header.rs:1989` | `is_spice` | `true` only for V8 (was V7 at rc.1) |
| `core/primitives/src/block.rs:219` | `Block::produce` (finality) | compute `last_ds_final_block` / `last_final_block` |
| `chain/chain/src/approval_verification.rs:11` | `verify_approval_with_approvers_info` | verify every present approval signature |
| `chain/chain/src/approval_verification.rs:42` | `verify_approvals_and_threshold_orphan` | no-ancestry approval + threshold check |
| `chain/chain/src/approval_verification.rs:57` | (checked_add guard) | reject `u64::MAX` prev_height instead of panicking |
| `chain/chain/src/approval_verification.rs:87` | `get_heuristic_block_approvers_ordered` | current-epoch-only approver list the orphan path zips against |
| `chain/epoch-manager/src/lib.rs:1504` | `get_all_block_approvers_ordered` | the real (this + next epoch) ordered approver list |
| `chain/chain/src/chain.rs:844` | `partial_verify_orphan_header_signature` | epoch-only header signature check |
| `chain/chain/src/chain.rs:857` | `check_if_finalizable` | reject headers that cannot reach the final head |
| `chain/chain/src/chain.rs:885` | `Chain::validate_header` | version gates, approvals, threshold, finality-field, ordinal checks |
| `chain/chain/src/chain.rs:928` | (validate_header) | bidirectional spice-ness vs epoch check, ungated |
| `chain/chain/src/chain.rs:941` | (validate_header) | **v87+**: reject `BlockHeaderV7` → `Error::InvalidProtocolVersion` |
| `chain/chain/src/chain.rs:1012` | (validate_header) | recompute & enforce finality fields |
| `chain/chain/src/chain.rs:1054` | (validate_header) | spice vs non-spice chunk-endorsement dispatch |
| `chain/chain/src/chain.rs:1083` | `verify_block_hash_and_signature` | body-hash + producer-signature check used by the state-sync path ([sync](sync.md)) |
| `chain/chain/src/chain.rs:2475` | (orphan path) | producer-signature-only orphan admission; the approval check here is commented out (`chain.rs:2480`) |
| `chain/chain/src/chain.rs:4204` | `verify_header_approvals_without_ancestry` | >2/3 approval proof for a header with no known parent |
| `chain/chain/src/signature_verification.rs:78` | `verify_block_header_signature_with_epoch_manager` | header signature vs height's BP |
| `chain/chain/src/lightclient.rs:34` | `create_light_client_block_view` | package final block + approvals_after_next |
| `chain/client/src/client.rs:1027` | `produce_block_on` (approvals) | fill header `approvals` from witness |
| `chain/client/src/client.rs:1209` | `peer_height_requires_sync` | heights that could change the sync decision |
| `chain/client/src/client.rs:1225` | `note_verified_peer_height` | record a peer height proven by approvals |
| `chain/client/src/client.rs:1732` | `check_and_update_doomslug_tip` | feed head → `set_tip` |
| `chain/client/src/client.rs:1810` | `send_block_approval` | route approval, incl. previous-epoch skip target |
| `chain/client/src/client.rs:2401` | `collect_block_approval` | validate + route incoming approvals |
| `chain/client/src/client_actor.rs:636` | `BlockResponse` handler | feed unsolicited blocks to the verified-height signal |
| `chain/client/src/client_actor.rs:1310` | (block production loop) | `ready_to_produce_block` → `produce_block` |
| `chain/client/src/client_actor.rs:1525` | `try_doomslug_timer` | drive `process_timer`, persist target height, send |
| `chain/client/src/client_actor.rs:1735` | `syncing_info` | verified vs claimed peer heights, own header head |
| `chain/client/src/client_actor.rs:1776` | `head_is_stale` | ~1 epoch of wall clock without head progress |
| `chain/client/src/client_actor.rs:2060` | `validate_state_sync_block` | hash+signature then `validate_block` for state-sync blocks ([sync](sync.md)) |
| `chain/client/src/verified_peer_heights.rs:31` | `record_if_verified` | per-peer proven height + per-header LRU |
| `core/store/src/light_client_block.rs:22` | `StoredLightClientBlock` | versioned on-disk light client block (2.14) |
| `core/primitives-core/src/version.rs:437` | `ValidateBlockOrdinalAndEpochSyncDataHash` | v85 header-validation gate |
| `core/primitives-core/src/version.rs:505` | `RejectBlockHeaderV7BeforeSpice` | v87 header-admission gate (`:642`) |
| `core/primitives-core/src/version.rs:670` | `MIN_SUPPORTED_PROTOCOL_VERSION` | 84 |
| `core/primitives-core/src/version.rs:698` | `STABLE_PROTOCOL_VERSION` | 88 |
| `core/primitives/src/version.rs:37` | `get_protocol_upgrade_schedule` | mainnet 87 @ 2026-11-04 (`:59`), 88 @ 2026-11-06; testnet arm at `:72` |
| `test-loop-tests/src/tests/block_header_v7_before_spice.rs:87` | `block_header_v7_accepted_before_reject_feature` | V7 accepted and canonical at v86 |
| `test-loop-tests/src/tests/block_header_v7_before_spice.rs:108` | `block_header_v7_rejected_with_reject_feature` | V7 rejected at v87; its producer falls behind |

## Open questions

- The docstring at `chain/chain/src/doomslug.rs:137` still references a `PersistentDoomslug` struct that does not exist anywhere in this tree; persistence of `largest_target_height` is handled directly in `try_doomslug_timer` via the chain store. (The previous revision of this spec also blamed `chain/chain/AGENTS.md:12`; that file no longer mentions it — corrected.)
- ~~`verify_approvals_and_threshold_orphan` has no length guard~~ — **resolved** (see *Invariants & failure modes*). The guard is genuinely absent and genuinely reachable, but it is not a consensus-safety issue. `verify_approvals_and_threshold_orphan` has exactly **one** production caller in the tree — `Chain::verify_header_approvals_without_ancestry` (`chain.rs:4204`), itself called only from `Client::note_verified_peer_height` (`client.rs:1233`); the second potential caller in the orphan-admission path is commented out (`chain.rs:2480`), and the only other reference is the unit test at `approval_verification.rs:123`. That single caller *does* check the producer signature first (`chain.rs:4211`), but that check does not constrain the approvals vector's length, so truncation is reachable — and harmless, because the truncated approvals are excluded from the stake tally as well as from signature verification.
- `docs/ChainSpec/Consensus.md` (unchanged since 2.13) models finality abstractly and uses the term "block proposer"; it appears broadly consistent with the code's two-consecutive-heights rule but was not line-by-line reconciled here — flagged as potentially needing a freshness pass. It says nothing about the new verified-peer-height sync gating.
- ~~test-loop coverage for the ported light-client tests (#15971)~~ — **resolved**: the ported tests live in `test-loop-tests/src/tests/light_client.rs` (`test_next_light_client_block` `:94`, `test_next_light_client_block_epoch_boundary` `:185`, `test_light_client_execution_outcome_proof` `:266`, `..._across_resharding` `:374`), with the NEP-25 check — recomputed block hash, approval signatures against the epoch's block producers, >2/3 stake, `next_bp_hash` on epoch change — in `validate_light_client_block` (`:42`). The corresponding pytests were removed.
- `CHANGELOG.md` for 2.14.0 has **no** entry for `BlockHeaderV8` / `RejectBlockHeaderV7BeforeSpice`, although it is a protocol change that alters header acceptance at v87. Whether this is a deliberate omission (the feature is only reachable via a byzantine producer, since no honest node ever emits V7) or an oversight could not be determined from the tree.
- Mixed-binary behaviour below v87 could not be made safe by code alone — see the table in [Header version admission](#why-213-accepts-a-v7-header-and-2140-rc1-does-not) for the full matrix. The residual open item is operational, not a code question: whether any 2.14.0-rc.1 binary is still in service on testnet (which passed v87 on 2026-09-23 and v88 on 2026-10-21, so the gate is already armed there) and whether any 2.13.x node, which has no gate at all and keeps accepting V7 at every protocol version, is still following the chain. Neither can be determined from the tree.
