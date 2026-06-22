# Epoch management, validators & staking

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `chain/epoch-manager/src/lib.rs`, `chain/epoch-manager/src/validator_selection.rs`, `chain/epoch-manager/src/reward_calculator.rs`, `chain/epoch-manager/src/validator_stats.rs`, `chain/epoch-manager/src/shard_assignment/mod.rs`, `chain/epoch-manager/src/adapter.rs`, `core/primitives/src/epoch_info.rs`, `core/primitives/src/epoch_manager.rs`, `core/primitives/src/types.rs`

## Role

The epoch manager owns the validator economy. It divides the chain into fixed-length **epochs**, and at each epoch boundary it deterministically computes the next-next epoch's validator set from staking proposals: who produces blocks, who produces chunks, who validates chunks, how seats are priced, how rewards and the protocol-treasury cut are distributed, who is kicked out (low uptime), and which shard layout and chunk-producer-to-shard assignment is in force. It also tallies the per-block-producer protocol-version votes that drive network upgrades. Stake proposals arrive as the output of `Stake` actions executed by [runtime-execution](runtime-execution.md); the producer schedule it computes is consumed by [consensus-finality](consensus-finality.md) (approver set + per-account stake) and [sharding-chunks](sharding-chunks.md) (chunk producer/validator assignment). Reward amounts feed the inflation accounting in [economics](economics.md); the version tally is the input side of [protocol-versioning](protocol-versioning.md). Shard-layout *migration* (resharding mechanics) lives in [sharding-chunks](sharding-chunks.md); only the per-epoch *choice* of layout and producer assignment lives here.

## Key data structures

- **`EpochInfo`** — `core/primitives/src/epoch_info.rs:22` — the frozen description of one epoch: validators, role settlements, samplers, stake changes, rewards, kickouts, seat price, protocol version. Versioned `V1..V5`; `EpochInfo::new` (`epoch_info.rs:199`) emits **V5** when `DynamicResharding` is enabled (carries `shard_layout` + `last_resharding`), else **V4**. Pre-V4 variants lack the chunk-validator `validator_mandates` and stake-weighted samplers; pre-V3 lack the RNG seed and fall back to round-robin sampling (`epoch_info.rs:537`).
- **`EpochInfoV5`** — `core/primitives/src/epoch_info.rs:49` — fields: `validators: Vec<ValidatorStake>`, `validator_to_index`, `block_producers_settlement: Vec<ValidatorId>`, `chunk_producers_settlement: Vec<Vec<ValidatorId>>` (per shard-index), `stake_change`, `validator_reward`, `validator_kickout`, `minted_amount`, `seat_price`, `protocol_version`, `shard_layout`, `last_resharding`, plus the private `rng_seed`, `block_producers_sampler`, `chunk_producers_sampler` (`StakeWeightedIndex` per shard) and `validator_mandates`.
- **`EpochConfig`** — `core/primitives/src/epoch_manager.rs:103` — the per-protocol-version tunables: `epoch_length`, `num_block_producer_seats`, `num_chunk_producer_seats`, `num_chunk_validator_seats`, `minimum_stake_ratio`, `target_validator_mandates_per_shard`, kickout thresholds (`block_producer_kickout_threshold`, `chunk_producer_kickout_threshold`, `chunk_validator_only_kickout_threshold`), `validator_max_kickout_stake_perc`, `online_min_threshold` / `online_max_threshold`, `protocol_upgrade_stake_threshold`, `max_inflation_rate`, `minimum_validators_per_shard`, `chunk_producer_assignment_changes_limit`, `shuffle_shard_assignment_for_chunk_producers`. Resolved per version by `config.for_protocol_version(..)`.
- **`ValidatorStake`** — `core/primitives/src/types.rs:594` — enum, only `V1(ValidatorStakeV1)` exists at v86; holds `account_id`, `public_key`, `stake: Balance`. A staking proposal and a selected validator share this type.
- **`EpochId`** — `core/primitives/src/types.rs:556` — newtype over `CryptoHash`. The id of an epoch is **the hash of the last block of the epoch two epochs prior** (epoch T's id = hash of last block of T-2); for the genesis/first epochs it is `EpochId::default()`. Computed as `EpochId(*first_block_info.prev_hash())` of the epoch's first block (`lib.rs:1659`, `get_next_epoch_id_from_info`).
- **`ApprovalStake`** — `core/primitives/src/types.rs:571` — a validator's `stake_this_epoch` + `stake_next_epoch`, used to seed the Doomslug approver set across the epoch boundary (see [consensus-finality](consensus-finality.md)).
- **`ValidatorKickoutReason`** — `core/primitives/src/types.rs:1253` — `NotEnoughBlocks`, `NotEnoughChunks`, `NotEnoughChunkEndorsements`, `Unstaked`, `NotEnoughStake{stake,threshold}`, `DidNotGetASeat`, `ProtocolVersionTooOld{version,network_version}`. Variant 0 is `_UnusedSlashed` — slashing is not active (see Invariants).
- **`AssignmentStrategy`** — `chain/epoch-manager/src/shard_assignment/mod.rs:140` — `CarryOver` (layout unchanged), `Fresh` (layout changed, pre-sticky), or `StickyResharding{shard_idx_mapping}` (layout changed, sticky-by-`ShardId`). Selected by `AssignmentStrategy::select` (`mod.rs:170`).
- **`EpochInfoAggregator`** — `chain/epoch-manager/src/epoch_info_aggregator.rs:19` — incrementally accumulated over the epoch as blocks are recorded: `block_tracker` (per-validator produced/expected blocks), `shard_tracker` (per-shard per-validator chunk production + endorsement stats), `all_proposals` (latest stake proposal per account), and `version_tracker` (each block producer's voted protocol version).

## Behavior

### 1. Epoch boundary detection

A new block is decided to be in a new epoch when its predecessor was the epoch's last block. `is_next_block_in_next_epoch_impl` (`lib.rs:1596`): with `estimated_next_epoch_start = epoch_first_block.height + epoch_length`, the next block starts a new epoch iff `last_final_height + 3 >= estimated_next_epoch_start` (for `epoch_length <= 3`, a test-only shortcut uses `block_height + 1 >= …`, `lib.rs:1611`). So an epoch ends once a block is finalized within 3 of the estimated end — the boundary is **finality-driven**, not a fixed height. `record_block_info_impl` (`lib.rs:933`) stamps each block's `epoch_id`/`epoch_first_block`: genesis and the first real block get `EpochId::default()` (`lib.rs:959`); otherwise if the parent ends an epoch, the block gets `get_next_epoch_id_from_info(parent)` and becomes the new `epoch_first_block` (`lib.rs:964`); else it copies the parent's. When the recorded block is itself the last in its epoch, `finalize_epoch` runs (`lib.rs:1000`).

### 2. EpochId and the T-2 rule

Epoch T's `EpochId` is the hash of the last block of epoch T-2 (`lib.rs:1659`). Correspondingly, the `EpochInfo` for the epoch *after next* (T+2) is computed at the end of epoch T and stored under `EpochId(last_block_hash_of_T)` (`finalize_epoch`, `lib.rs:910`/`921`). This 2-epoch lead is why staking takes ~2 epochs to take effect and why `compute_stake_return_info` looks back three epochs.

### 3. Finalizing an epoch → computing T+2's validators

`finalize_epoch` (`lib.rs:797`) runs on the last block of epoch T, in this order:
1. `collect_blocks_info` (`lib.rs:804`→`516`) reads the epoch's `EpochInfoAggregator` (`get_epoch_info_aggregator_upto_last`, `lib.rs:531`) and produces an `EpochSummary`: proposals, kickouts, per-validator block/chunk/endorsement stats, and `next_next_epoch_version`. The protocol-version tally (step 9) and the uptime kickouts via `compute_validators_to_reward_and_kickout` (step 6) both run *inside* this call (`lib.rs:544`, `lib.rs:640`), not as separate later steps.
2. Compute rewards via `RewardCalculator::calculate_reward` (`lib.rs:849`, step 7); kicked-out validators are first removed from the reward stats (`lib.rs:829`–`838`).
3. Choose the T+2 shard layout (`next_next_shard_layout`, `lib.rs:862`→`679`) and the `AssignmentStrategy` (`AssignmentStrategy::select`, `lib.rs:875`).
4. Call `proposals_to_epoch_info` (`lib.rs:882`, step 4) to select roles and build the new `EpochInfo`, stored under `EpochId(*last_block_hash)` (`lib.rs:910`/`921`).

If selection fails because everyone unstaked (`ThresholdError` / `NotEnoughValidators`), it clones the previous `EpochInfo` and just bumps `epoch_height` (`lib.rs:896`–`907`).

### 4. Validator selection from proposals

`proposals_to_epoch_info` (`validator_selection.rs:166`):
1. `apply_epoch_update_to_proposals` (`validator_selection.rs:290`) folds last-epoch validators and new `Stake`-action proposals into one map keyed by account: kicked-out accounts are dropped (stake→0); accounts kicked out last epoch for `ProtocolVersionTooOld` are barred from re-entry (`validator_selection.rs:303`); a re-proposing account uses its new stake, otherwise it carries forward its prior stake; the previous epoch's **reward is added to the stake** (auto-restake, `validator_selection.rs:322`).
2. `select_validators_from_proposals` (`validator_selection.rs:45`) runs `select_validators` three independent times over the same proposal multiset — once each for chunk-producer, block-producer, and chunk-validator seats — using `num_chunk_producer_seats`, `num_block_producer_seats`, `num_chunk_validator_seats`.
3. `select_validators` (`validator_selection.rs:357`) takes the top-N by `OrderedValidatorStake` (max-heap keyed by stake, ties broken by *lexicographically smallest* account id, `validator_selection.rs:404`), but stops early if the next proposal's stake is `<= min_stake_ratio` of the running total (`validator_selection.rs:369`). The returned **threshold** (seat price) is `last_selected_stake + 1` if all seats filled, else `ceil(min_stake_ratio * total / (1 - min_stake_ratio))` (`validator_selection.rs:381`). The epoch's `seat_price` is `min(bp, cp, cv)` thresholds (`validator_selection.rs:88`).
4. Accounts that were validators last epoch but won no role are kicked out with `NotEnoughStake{stake, threshold}` (`validator_selection.rs:210`).
5. `validator_to_index`/`all_validators` are built in role-priority order (chunk producers, then block producers, then chunk validators), each account once (`validator_selection.rs:118`).
6. Chunk producers are assigned to shards (step 8); block producers map to local indices; chunk validators are turned into `ValidatorMandates` over all validators (`validator_selection.rs:248`, target `target_validator_mandates_per_shard`, default 68).

**Roles are not mutually exclusive**: an account with enough stake is simultaneously a block producer, chunk producer, and chunk validator. A "chunk-only producer" is simply an account that won a chunk-producer seat but not a block-producer seat (there are more CP/CV seats than BP seats).

### 5. Block / chunk / chunk-validator selection at a height

Within an epoch, the per-height producer is sampled deterministically from the settlement and stake-weighted sampler:
- **Block producer**: `EpochInfo::sample_block_producer(height)` (`epoch_info.rs:535`) — for V3+, hash `(rng_seed ++ height_le)` (`block_produce_seed`, `epoch_info.rs:632`) seeds `block_producers_sampler.sample` (stake-weighted). V1/V2 use round-robin `settlement[height % len]`.
- **Chunk producer**: `sample_chunk_producer(layout, shard_id, height)` (`epoch_info.rs:560`) — seed is `(rng_seed ++ height_le ++ shard_id_le)` (`chunk_produce_seed`, `epoch_info.rs:639`), indexing into that shard's `chunk_producers_sampler`. Exposed via `get_chunk_producer_info(ChunkProductionKey)` (`adapter.rs:507`), which always recomputes via `sample_chunk_producer`. A separate hash-keyed entry point `get_chunk_producer_info_db(prev_block_hash, shard_id)` (`adapter.rs:980`) instead reads the persisted `DBCol::ChunkProducers` (keyed by `get_block_shard_id(prev_block_hash, shard_id)`) when `EarlyKickout` is enabled for the block's epoch, else falls back to computation (nightly only at v86 — see Protocol-version-gated).
- **Chunk validators**: `sample_chunk_validators(height)` (`epoch_info.rs:597`) draws from `validator_mandates` using a ChaCha20 RNG seeded by `(rng_seed ++ height_le)` (`chunk_validate_rng`, `epoch_info.rs:654`), cached per `(epoch_id, shard_id, height)` by `get_chunk_validator_assignments` (`lib.rs:1064`). The seed is height-derived so every node agrees on the set and order.

`get_block_producer_info` (`adapter.rs:496`) and `get_block_producer` (`adapter.rs:487`) are the public entry points.

### 6. Kickouts (uptime) and the max-kickout-stake guard

`compute_validators_to_reward_and_kickout` (`lib.rs:372`):
1. For each validator, aggregate block stats, per-shard chunk production + endorsement stats (`lib.rs:389`); SPICE epochs add endorsement stats from the last block header instead (`lib.rs:409`).
2. Compute each validator's online ratio via `get_sortable_validator_online_ratio` (average of block / chunk / endorsement produced-over-expected, `validator_stats.rs:110`), sort ascending by (ratio, stake, account-id) (`lib.rs:427`).
3. `compute_exempted_kickout` (`lib.rs:315`): walk validators from *highest* uptime down, exempting them until exempted stake reaches `(100 - validator_max_kickout_stake_perc)%` of total stake — bounding total kicked-out stake so network instability can't evict too many at once. Already-kicked-out validators can't be exemptions.
4. A non-exempt validator is kicked out for `NotEnoughBlocks` if its block production is below `block_producer_kickout_threshold`, `NotEnoughChunks` below `chunk_producer_kickout_threshold`, and (if it was *only* a chunk validator: `block.expected == 0 && chunk.expected == 0`) `NotEnoughChunkEndorsements` below `chunk_validator_only_kickout_threshold` (`lib.rs:468`–`499`).
5. Anti-stall: if every validator would be kicked out (or was already), the one with the most produced blocks is spared (`lib.rs:507`).

### 7. Rewards and the protocol treasury

`RewardCalculator::calculate_reward` (`reward_calculator.rs:51`):
1. `epoch_total_reward = max_inflation_rate.numer * total_supply * epoch_duration_ns / (num_seconds_per_year * max_inflation_rate.denom * 1e9)` (`reward_calculator.rs:69`). `epoch_duration` is the wall-clock nanoseconds between the last blocks of the two epochs (`finalize_epoch`, `lib.rs:827`).
2. `epoch_protocol_treasury = epoch_total_reward * protocol_reward_rate` (`reward_calculator.rs:78`), credited to `protocol_treasury_account`. For the mainnet genesis version the rate is hardcoded to 1/10 (`reward_calculator.rs:63`).
3. The remaining `epoch_validator_reward` is split per validator pro-rata by stake, scaled by an online multiplier: 0 below `online_min_threshold`, linearly ramping to 1 at `online_max_threshold` (`reward_calculator.rs:109`–`141`). A validator with zero expected blocks/chunks/endorsements gets nothing.
4. The endorsement component of the online ratio is gated by `endorsement_cutoff_threshold` (set to `chunk_validator_only_kickout_threshold` in `finalize_epoch`, `lib.rs:845`): below the cutoff the endorsement ratio is treated as 0, above it as 1 (`validator_stats.rs:124`).

Reward issuance economics (the inflation curve, `minted_amount`) is detailed in [economics](economics.md); only the split mechanics live here.

### 8. Shard layout and chunk-producer-to-shard assignment

`next_next_shard_layout` (`lib.rs:679`) picks T+2's layout: a static layout from the next-next config if defined; under dynamic resharding it derives a split layout from the last block's `shard_split` (`lib.rs:738`) or carries the next layout forward. `last_resharding` is set to `next_epoch_height + 1` when the layout changes (`lib.rs:871`).

`assign_chunk_producers_to_shards` (`shard_assignment/mod.rs:409`) builds `chunk_producers_settlement`:
- If `chunk_producers.len() < min_validators_per_shard * num_shards`, a satisfy-shards path runs and the strategy/prev-assignment are **ignored** (`mod.rs:399`).
- Otherwise `assign_to_balance_shards` (`mod.rs:287`) seeds from the prev epoch per `AssignmentStrategy` (`Fresh`=empty, `CarryOver`=copy by `ShardIndex`, `StickyResharding`=sticky by `ShardId` with split children inheriting a stake-balanced subset of the parent's producers), assigns *new* producers to the least-populated shards first, then rebalances (random swaps from max to min shard) until each shard has `min_validators_per_shard` and the spread is ≤1 or `chunk_producer_assignment_changes_limit` is hit (`mod.rs:336`–`375`). Sticky resharding raises that limit so split children fill up in one epoch (`mod.rs:431`).
- If `shuffle_shard_assignment_for_chunk_producers` is set, the whole settlement is permuted by a dedicated `shard_assignment_rng` (`validator_selection.rs:236`, `epoch_info.rs:671`).

### 9. Protocol-version vote tally

Each block records its producer's `latest_protocol_version` into `version_tracker` (first-seen per producer, `epoch_info_aggregator.rs:189`). At epoch end (`collect_blocks_info`, `lib.rs:544`): sum stake per voted version, find the version with the most stake, and adopt it as `next_next_epoch_version` iff its stake exceeds `protocol_upgrade_stake_threshold * total_block_producer_stake` (`lib.rs:567`–`577`); otherwise keep the next epoch's version. Validators that voted for a version *below* the adopted one are kicked out with `ProtocolVersionTooOld` (`lib.rs:588`). The adopted version becomes T+2's `EpochInfo::protocol_version`. This is the input feeding [protocol-versioning](protocol-versioning.md).

## Interactions

- **Consumes**: staking proposals (`block_info.proposals_iter`) produced by `Stake` actions in [runtime-execution](runtime-execution.md); block/chunk production + endorsement stats fed into the `EpochInfoAggregator` by [chain-block-processing](chain-block-processing.md) as blocks are recorded; per-protocol-version `EpochConfig`; the last block's `shard_split` from dynamic resharding ([sharding-chunks](sharding-chunks.md)).
- **Produces**: the per-height block/chunk producer and chunk-validator assignment consumed by [consensus-finality](consensus-finality.md) (`get_epoch_block_approvers_ordered`, `get_block_producer`) and [sharding-chunks](sharding-chunks.md) / [stateless-validation](stateless-validation.md); `ApprovalStake` (this+next epoch) for the Doomslug threshold across boundaries (`get_all_block_approvers_ordered`, `lib.rs:1109`); validator rewards + `minted_amount` consumed by [economics](economics.md); `stake_change` (stake-return amounts) applied by the runtime; the adopted `next_next_epoch_version` for [protocol-versioning](protocol-versioning.md); the per-epoch `ShardLayout`.
- **Touches**: the `EpochManagerAdapter` trait (`adapter.rs`) is the boundary every other component calls through.

## Protocol-version-gated behavior

Verified against `core/primitives-core/src/version.rs`:

- **`DynamicResharding`** — activates at **v85** (`version.rs:559`), so it is **on at v86**. `EpochInfo::new` emits V5 (with embedded `shard_layout` + `last_resharding`) rather than V4 (`epoch_info.rs:227`); `next_next_shard_layout` may derive a split layout per epoch (`lib.rs:704`/`738`). Shard-layout migration itself: [sharding-chunks](sharding-chunks.md).
- **`StickyReshardingValidatorAssignment`** — activates at **v85** (`version.rs:560`), **on at v86**. On a layout change `AssignmentStrategy::select` returns `StickyResharding` (sticky by `ShardId`, split children inherit a stake-balanced subset of the parent's chunk producers) instead of `Fresh` (`mod.rs:178`–`188`), and raises the assignment-changes limit (`mod.rs:431`). Pre-v85 layout changes used `Fresh`, dropping all stickiness.
- **`ShuffleShardAssignments`** — nightly only, `protocol_version()` = **143** (`version.rs:578`), **off at v86**. When on it would set `shuffle_shard_assignment_for_chunk_producers`, permuting `chunk_producers_settlement` each epoch (`validator_selection.rs:236`).
- **`EarlyKickout`** — nightly only, `protocol_version()` = **152** (`version.rs:579`), **off at v86**. When on, chunk-producer lookups read the persisted `DBCol::ChunkProducers` column instead of recomputing (`adapter.rs:980`, behind `#[cfg(feature = "nightly")]`); at v86 the deterministic `sample_chunk_producer` computation path is always used.
- **Chunk-validator seats / stateless validation** — the chunk-validator role, `validator_mandates`, `num_chunk_validator_seats`, the endorsement-based uptime/kickout terms, and `NotEnoughChunkEndorsements` all originate from stateless validation (NEP-509), whose feature flag is `_DeprecatedStatelessValidation` at **v69** (`version.rs:515`) — always on at v86. The EpochInfo V3→V4 bump added these structures (`epoch_info.rs:73`).
- **`Spice`** — nightly-adjacent, `protocol_version()` = **180** (`version.rs:582`), **off at v86**. When on, chunk endorsements are accumulated per-validator on the epoch's last block header rather than embedded per-shard, so kickout/reward endorsement stats come from `last_block_info.spice_chunk_endorsement_stats()` via a `spice_endorsement_tracker` instead of the per-shard `shard_tracker` (`lib.rs:622`, consumed at `lib.rs:409`). At v86 the per-shard path is always used and the SPICE tracker is empty.
- Earlier selection-algorithm changes are deprecated/always-on at v86: `_DeprecatedAliasValidatorSelectionAlgorithm` (v49), `_DeprecatedChunkOnlyProducers` / `_DeprecatedMaxKickoutStake` (v56), `_DeprecatedFixStakingThreshold` / `_DeprecatedFixChunkProducerStakingThreshold` (v74), `_DeprecatedFixMinStakeRatio` (v71). Their behavior is baked into the current `select_validators` / kickout code; no runtime branch remains at v86.

No other `ProtocolFeature` alters epoch/validator selection at v86 beyond the above.

## Invariants & failure modes

- **Slashing is inactive.** The only slash kickout variant is `_UnusedSlashed` (`types.rs:1255`) and validator info is always emitted with `is_slashed: false` (`lib.rs:1367`, `lib.rs:1455`). Equivocation detection exists in consensus but does not feed an on-chain slash at v86.
- **Bounded kickout stake.** Total kicked-out stake never exceeds `validator_max_kickout_stake_perc%` of total stake (enforced by `compute_exempted_kickout`, `lib.rs:315`), and not all validators can be kicked out in one go (the top block-producer is spared, `lib.rs:507`).
- **Determinism.** All sampling (block/chunk producer, chunk validator, shard-assignment shuffle/rebalance) is seeded from the epoch `rng_seed` + height (+ shard) and re-derivable on every node (`epoch_info.rs:632`/`639`/`654`, `mod.rs:325`); chunk-validator order is height-derived precisely so nodes agree (`epoch_info.rs:654`).
- **Seat-price / ratio condition.** A proposal is rejected if its stake is not strictly greater than `min_stake_ratio` of the running total (`validator_selection.rs:369`); this caps any single validator's selection probability and yields the seat price (`validator_selection.rs:381`).
- **Proposals must be unique per account** — debug-asserted in `proposals_to_epoch_info` (`validator_selection.rs:179`).
- **No-validator failure modes.** `proposals_to_epoch_info` errors with `ThresholdError` / `NotEnoughValidators`; `finalize_epoch` swallows these by reusing the previous epoch's set with an incremented height (`lib.rs:896`–`907`), so the chain never stalls on validator selection.
- **Re-entry bar.** An account kicked out for `ProtocolVersionTooOld` in epoch T-1 cannot rejoin in T (`validator_selection.rs:303`), preventing an under-upgraded validator from oscillating in and out.
- **Unstake detection.** An account whose latest proposal has zero stake while it still has a nonzero `stake_change` is kicked out as `Unstaked` (`lib.rs:603`).
- **`assign_to_balance_shards` hard assert** — panics if rebalancing exceeds `max(chunk_producers.len(), changes_limit)` iterations, which would indicate a producer placed on two shards (`mod.rs:345`); the `StickyResharding`/`CarryOver` precondition (each validator on ≤1 prev shard) protects this (`mod.rs:244`).

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `chain/epoch-manager/src/lib.rs:797` | `EpochManager::finalize_epoch` | compute & store T+2 EpochInfo at epoch end |
| `chain/epoch-manager/src/lib.rs:516` | `collect_blocks_info` | aggregate proposals/stats, run version tally |
| `chain/epoch-manager/src/lib.rs:372` | `compute_validators_to_reward_and_kickout` | uptime kickouts + reward stats |
| `chain/epoch-manager/src/lib.rs:315` | `compute_exempted_kickout` | bound total kicked-out stake |
| `chain/epoch-manager/src/lib.rs:544` | version tally loop | adopt next_next_epoch_version above threshold |
| `chain/epoch-manager/src/lib.rs:679` | `next_next_shard_layout` | pick T+2 shard layout |
| `chain/epoch-manager/src/lib.rs:933` | `record_block_info_impl` | stamp epoch_id, trigger finalize |
| `chain/epoch-manager/src/lib.rs:1596` | `is_next_block_in_next_epoch_impl` | finality-driven epoch boundary |
| `chain/epoch-manager/src/lib.rs:1659` | `get_next_epoch_id_from_info` | EpochId = hash of last block of T-2 |
| `chain/epoch-manager/src/lib.rs:1064` | `get_chunk_validator_assignments` | sample + cache chunk validators |
| `chain/epoch-manager/src/lib.rs:1109` | `get_all_block_approvers_ordered` | this+next epoch approver set + stakes |
| `chain/epoch-manager/src/validator_selection.rs:166` | `proposals_to_epoch_info` | full selection → new EpochInfo |
| `chain/epoch-manager/src/validator_selection.rs:290` | `apply_epoch_update_to_proposals` | fold proposals + rewards (auto-restake) |
| `chain/epoch-manager/src/validator_selection.rs:357` | `select_validators` | top-N by stake with min-stake-ratio cutoff |
| `chain/epoch-manager/src/validator_selection.rs:404` | `OrderedValidatorStake::key` | stake desc, account-id asc tiebreak |
| `chain/epoch-manager/src/reward_calculator.rs:51` | `RewardCalculator::calculate_reward` | total reward, treasury cut, per-validator split |
| `chain/epoch-manager/src/validator_stats.rs:16` | `get_validator_online_ratio` | average block/chunk/endorsement uptime |
| `chain/epoch-manager/src/shard_assignment/mod.rs:409` | `assign_chunk_producers_to_shards` | producer→shard assignment |
| `chain/epoch-manager/src/shard_assignment/mod.rs:170` | `AssignmentStrategy::select` | CarryOver / Fresh / StickyResharding |
| `chain/epoch-manager/src/adapter.rs:496` | `get_block_producer_info` | sample block producer for height |
| `chain/epoch-manager/src/adapter.rs:507` | `get_chunk_producer_info` | sample chunk producer for (epoch,shard,height) |
| `core/primitives/src/epoch_info.rs:199` | `EpochInfo::new` | emit V5 (DynamicResharding) else V4 |
| `core/primitives/src/epoch_info.rs:535` | `sample_block_producer` | stake-weighted, seed = hash(seed++height) |
| `core/primitives/src/epoch_info.rs:560` | `sample_chunk_producer` | per-shard stake-weighted sampler |
| `core/primitives/src/epoch_info.rs:597` | `sample_chunk_validators` | mandate sampling via ChaCha20 |
| `core/primitives/src/epoch_manager.rs:103` | `EpochConfig` | per-version seats/thresholds/inflation |
| `core/primitives/src/types.rs:556` | `EpochId` | hash of last block of T-2 |
| `core/primitives/src/types.rs:1253` | `ValidatorKickoutReason` | kickout reason variants |

## Open questions

- `version_tracker` records the *first* protocol version seen per block producer over the epoch (`epoch_info_aggregator.rs:189`, `.entry().or_insert_with(..)`), not the latest. A producer that upgrades mid-epoch therefore has its pre-upgrade vote counted for the whole epoch; whether this is intentional (votes are sticky within an epoch) or merely incidental is not documented in code.
- `docs/architecture/how/epoch.md` was named as a cross-check source in the plan but not re-read line-by-line here; the epoch-boundary rule (`last_final_height + 3 >= estimated_next_epoch_start`) and the T-2 EpochId rule are taken from code and should be confirmed against that doc on the next verification pass.
