# Epoch management, validators & staking

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `chain/epoch-manager/src/lib.rs`, `chain/epoch-manager/src/validator_selection.rs`, `chain/epoch-manager/src/reward_calculator.rs`, `chain/epoch-manager/src/shard_assignment/mod.rs`, `chain/epoch-manager/src/shard_assignment/sticky_resharding.rs`, `core/primitives/src/epoch_info.rs`, `core/primitives/src/epoch_manager.rs`, `core/primitives/src/types.rs`, `core/primitives-core/src/version.rs`

## Role

The epoch manager owns the validator economy. It divides the chain into variable-length **epochs** (nominally `epoch_length` blocks, but the boundary is finality-driven), and on the last block of each epoch T it deterministically computes the validator set for the epoch *after next* (T+2) from the staking proposals accumulated during T: who produces blocks, who produces chunks, who validates chunks, how seats are priced, how rewards and the protocol-treasury cut are distributed, who is kicked out (low uptime / missed chunks / stale protocol vote), which shard layout is in force and how chunk producers map to shards. It also tallies the per-block-producer protocol-version votes that drive network upgrades. Stake proposals arrive as the output of `Stake` actions executed by [runtime-execution](runtime-execution.md); the producer schedule it computes is consumed by [consensus-finality](consensus-finality.md) (approver set + per-account stake) and [sharding-chunks](sharding-chunks.md) (chunk producer/validator assignment). Reward amounts feed the inflation accounting in [economics](economics.md); the version tally is the input side of [protocol-versioning](protocol-versioning.md). Shard-layout *migration* (resharding mechanics) lives in [sharding-chunks](sharding-chunks.md); only the per-epoch *choice* of layout and producer assignment lives here.

## Key data structures

- **`EpochInfo`** — `core/primitives/src/epoch_info.rs:22` — the frozen description of one epoch: validators, role settlements, samplers, stake changes, rewards, kickouts, seat price, protocol version. Versioned `V1..V5`. `EpochInfo::new` (`epoch_info.rs:199`) emits **V5** when `DynamicResharding` is enabled (adds `shard_layout` + `last_resharding`, `epoch_info.rs:227`), else **V4**. `Default` is `V2` (`epoch_info.rs:112`). Sampling and stake-weighted selection require V3+; chunk-validator `validator_mandates` require V4+ (`sample_chunk_validators`, `epoch_info.rs:597`).
- **`EpochInfoV5`** — `core/primitives/src/epoch_info.rs:49` — public fields: `epoch_height`, `validators: Vec<ValidatorStake>`, `validator_to_index`, `block_producers_settlement: Vec<ValidatorId>`, `chunk_producers_settlement: Vec<Vec<ValidatorId>>` (indexed by *shard index*), `stake_change`, `validator_reward`, `validator_kickout`, `minted_amount`, `seat_price`, `protocol_version`, `shard_layout`, `last_resharding: Option<EpochHeight>`; private `rng_seed`, `block_producers_sampler: StakeWeightedIndex`, `chunk_producers_sampler: Vec<StakeWeightedIndex>`, `validator_mandates`. V4 (`epoch_info.rs:85`) is identical minus `shard_layout`/`last_resharding` and carries deprecated fisherman/hidden-validator fields.
- **`EpochConfig`** — `core/primitives/src/epoch_manager.rs:103` — per-protocol-version tunables: `epoch_length`, `num_block_producer_seats`, `num_chunk_producer_seats`, `num_chunk_validator_seats`, `minimum_stake_ratio`, `target_validator_mandates_per_shard`, kickout thresholds (`block_producer_kickout_threshold`, `chunk_producer_kickout_threshold`, `chunk_validator_only_kickout_threshold`), `validator_max_kickout_stake_perc`, `online_min_threshold` / `online_max_threshold`, `protocol_upgrade_stake_threshold`, `max_inflation_rate`, `minimum_validators_per_shard`, `chunk_producer_assignment_changes_limit`, `shuffle_shard_assignment_for_chunk_producers`, and `shard_layout_config` (static layout or dynamic-resharding config). Resolved per version by `self.config.for_protocol_version(..)` (e.g. `lib.rs:807`).
- **`ValidatorStake`** — `core/primitives/src/types.rs:594` — enum, only `V1(ValidatorStakeV1)` exists at v86; `ValidatorStakeV1` (`types.rs:791`) holds `account_id`, `public_key`, `stake: Balance`. A staking proposal and a selected validator share this type.
- **`EpochId`** — `core/primitives/src/types.rs:556` — newtype `EpochId(pub CryptoHash)`. An epoch's id is **the hash of the last block of the epoch two epochs prior** (T's id = hash of last block of T-2). Computed as `EpochId(*first_block_info.prev_hash())` from the epoch's first block (`get_next_epoch_id_from_info`, `lib.rs:1659`). The genesis/first epochs use `EpochId::default()` (`lib.rs:945`, `lib.rs:959`).
- **`ApprovalStake`** — `core/primitives/src/types.rs:571` — a validator's `stake_this_epoch` + `stake_next_epoch`, so epoch-boundary blocks can carry Doomslug approvals from both epochs (consumed by [consensus-finality](consensus-finality.md)).
- **`ValidatorKickoutReason`** — `core/primitives/src/types.rs:1253` — `_UnusedSlashed = 0` (deprecated — slashing not active, see Invariants), `NotEnoughBlocks`, `NotEnoughChunks`, `Unstaked`, `NotEnoughStake{stake,threshold}`, `DidNotGetASeat`, `NotEnoughChunkEndorsements`, `ProtocolVersionTooOld{version,network_version}`.
- **`AssignmentStrategy`** — `chain/epoch-manager/src/shard_assignment/mod.rs:140` — `CarryOver` (layout unchanged), `Fresh` (layout changed, pre-sticky), `StickyResharding{shard_idx_mapping}` (layout changed, sticky-by-`ShardId`). Chosen by `AssignmentStrategy::select` (`mod.rs:170`).
- **`RewardCalculator`** — `chain/epoch-manager/src/reward_calculator.rs:27` — holds `protocol_reward_rate`, `protocol_treasury_account`, `num_seconds_per_year`, `genesis_protocol_version`; `calculate_reward` (`reward_calculator.rs:51`) produces per-validator rewards + total minted.
- **`EpochSummary`** — produced by `collect_blocks_info` (`lib.rs:516`): `all_proposals`, `validator_kickout`, `validator_block_chunk_stats`, `next_next_epoch_version`, consumed by `finalize_epoch`.

## Behavior

### 1. Epoch boundary detection (finality-driven)

The next block after `block_info` starts a new epoch iff `last_final_height + 3 >= estimated_next_epoch_start`, where `estimated_next_epoch_start = epoch_first_block.height + epoch_length` — `is_next_block_in_next_epoch_impl` (`lib.rs:1596`, decision at `lib.rs:1614`). For `epoch_length <= 3` a test-only shortcut uses `block_height + 1 >= estimated_next_epoch_start` (`lib.rs:1611`). So an epoch ends once a block is *finalized* within 3 of the estimated end; the boundary is not a fixed height. (SPICE additionally blocks the transition while execution lags a full epoch behind, `lib.rs:1625`.)

### 2. Recording a block and stamping its epoch

`record_block_info_impl` (`lib.rs:933`) stamps each block's `epoch_id`/`epoch_first_block`:
1. Genesis is special-cased as a new epoch under `EpochId(genesis_hash)` (`lib.rs:942`).
2. The first real block (parent is genesis) gets `EpochId::default()` and becomes `epoch_first_block` (`lib.rs:957`).
3. If the parent ends its epoch (`is_next_block_in_next_epoch(parent)`), the block gets `get_next_epoch_id_from_info(parent)` and becomes the new `epoch_first_block` (`lib.rs:962`).
4. Otherwise it copies the parent's `epoch_id`/`epoch_first_block` (`lib.rs:970`).
The epoch-info aggregator is advanced only when the last-final block moves forward (`lib.rs:985`), so no aggregator rollback is ever needed. If the recorded block is itself the last block of its epoch, `finalize_epoch` runs (`lib.rs:1000`).

### 3. EpochId and the T-2 rule

Epoch T's `EpochId` is `EpochId(hash of last block of T-2)` (`get_next_epoch_id_from_info` takes the first block of T and returns `EpochId(prev_hash)`, i.e. the last block of the previous epoch — `lib.rs:1659`). Correspondingly the `EpochInfo` computed at the end of epoch T is for T+2 and is stored under `EpochId(*last_block_hash_of_T)` (`lib.rs:910`, `lib.rs:921`). This two-epoch lead is why a `Stake` action takes ~2 epochs to affect the validator set.

### 4. Finalizing an epoch → computing T+2's validators

`finalize_epoch` (`lib.rs:797`) runs on the last block of epoch T, in this order:
1. `collect_blocks_info` (`lib.rs:804` → `lib.rs:516`) reads the epoch's `EpochInfoAggregator` (`get_epoch_info_aggregator_upto_last`, `lib.rs:531`) and builds an `EpochSummary`: proposals, kickouts, per-validator block/chunk/endorsement stats, and `next_next_epoch_version`. The version tally (step 8) and the uptime/miss kickouts (`compute_validators_to_reward_and_kickout`, step 6) both run *inside* this call (`lib.rs:544`, `lib.rs:640`).
2. Rewards: kicked-out validators whose reason is `NotEnoughBlocks`/`NotEnoughChunks`/`NotEnoughChunkEndorsements` are first removed from the reward stats (`lib.rs:829`), then `RewardCalculator::calculate_reward` runs over `epoch_duration = block.timestamp - prev_epoch_last_block.timestamp` (`lib.rs:827`, `lib.rs:849`). `endorsement_cutoff_threshold` is set to `chunk_validator_only_kickout_threshold` (`lib.rs:845`).
3. Choose the T+2 shard layout (`next_next_shard_layout`, `lib.rs:862` → `lib.rs:679`) and set `last_resharding` if the layout changed (`lib.rs:871`).
4. Choose the `AssignmentStrategy` from the T+1 vs T+2 layouts (`AssignmentStrategy::select`, `lib.rs:875`).
5. `proposals_to_epoch_info` (`lib.rs:882`, step 5) selects roles and builds the new `EpochInfo`, stored under `EpochId(*last_block_hash)` (`lib.rs:910`/`921`).
If selection fails because everyone unstaked (`EpochError::ThresholdError` or `NotEnoughValidators`), it clones the previous `EpochInfo` and just bumps `epoch_height` (`lib.rs:896`, `lib.rs:902`).

### 5. Validator selection from proposals

`proposals_to_epoch_info` (`validator_selection.rs:166`):
1. `apply_epoch_update_to_proposals` (`validator_selection.rs:290`) merges last-epoch proposals with the previous validator set into a per-account proposal map. Priority: an account in `validator_kickout` is dropped and its `stake_change` set to 0; an account kicked out for `ProtocolVersionTooOld` in T-1 is *barred from re-entry* in T (`validator_selection.rs:303`); otherwise a fresh proposal wins, else the prior stake is carried; rewards from last epoch are added to the carried stake (`validator_selection.rs:322`).
2. `select_validators_from_proposals` (`validator_selection.rs:45`) runs `select_validators` three independent times over the *same* proposal set — once each for chunk-producer, block-producer and chunk-validator seats (`validator_selection.rs:54`–`73`) — using `num_chunk_producer_seats`, `num_block_producer_seats`, `num_chunk_validator_seats`. The overall stake `threshold` is the minimum of the three role thresholds (`validator_selection.rs:88`). An account may hold several roles.
3. `select_validators` (`validator_selection.rs:357`) takes the top-N by stake from a max-heap, but stops early if adding proposal `p` would make `p.stake / running_total <= minimum_stake_ratio` (`validator_selection.rs:369`) — this is why low-relative-stake proposals are excluded even when seats remain. The threshold is `last_selected_stake + 1` when all N seats fill, otherwise `min_stake_ratio * total / (1 - min_stake_ratio)` rounded up (`validator_selection.rs:381`).
4. Proposals selected for no role, but which were validators in T, are kicked out with `NotEnoughStake{stake, threshold}` and their `stake_change` set to 0 (`validator_selection.rs:204`).
5. Ordering: proposals are compared by `(stake, Reverse(account_id))` (`OrderedValidatorStake::key`, `validator_selection.rs:405`) — larger stake wins, ties broken by lexicographically *smaller* account id.
6. `get_chunk_producers_assignment` (`validator_selection.rs:98`) builds `all_validators` and `validator_to_index` by walking chunk producers, then block producers, then chunk validators, assigning each account a stable local `ValidatorId` in that order (`validator_selection.rs:118`), then assigns chunk producers to shards (step 9).
7. Chunk validators are wired via `ValidatorMandates::new` over `all_validators` with `target_validator_mandates_per_shard` mandates per shard (`validator_selection.rs:248`); a validator's mandate count derives from its stake vs the seat-price threshold.
8. Finally builds the `EpochInfo` with `epoch_height = prev + 1` (`validator_selection.rs:261`).

### 6. Kickouts

`compute_validators_to_reward_and_kickout` (`lib.rs:372`) computes per-validator `BlockChunkValidatorStats` by summing the block tracker and, over all shards, the chunk-production and endorsement trackers (`lib.rs:389`–`412`). Then:
- **Block producers**: `NotEnoughBlocks` if produced/expected ratio `< block_producer_kickout_threshold` (`lib.rs:468`).
- **Chunk producers**: `NotEnoughChunks` if chunk-production ratio `< chunk_producer_kickout_threshold` (`lib.rs:477`).
- **Chunk-validator-only** (expected 0 blocks and 0 chunks): `NotEnoughChunkEndorsements` if endorsement ratio `< chunk_validator_only_kickout_threshold` (`lib.rs:485`).
- **Stale vote** (in `collect_blocks_info`): any validator whose voted version `< next_next_epoch_version` is kicked with `ProtocolVersionTooOld` (`lib.rs:588`).
- **Unstaked** (in `collect_blocks_info`): a zero-stake proposal from an account that still had nonzero stake in T+1 is kicked with `Unstaked` (`lib.rs:603`).

Two safety valves cap kickouts:
- **Stake cap**: at most `validator_max_kickout_stake_perc` of total validator stake may be kicked. `compute_exempted_kickout` exempts validators (ordered by online ratio, then stake, then account id — `validator_comparator`, `lib.rs:427`) until the exempt set reaches `100 - validator_max_kickout_stake_perc` of stake (`lib.rs:452`).
- **Never kick everyone**: if every validator would be kicked or was already kicked, the one with the most produced blocks is saved (`all_kicked_out` → `validator_kickout.remove(max_validator)`, `lib.rs:507`).

### 7. Reward computation and protocol treasury

`RewardCalculator::calculate_reward` (`reward_calculator.rs:51`):
1. `epoch_total_reward = max_inflation_rate * total_supply * epoch_duration / (num_seconds_per_year * NUM_NS_IN_SECOND)` (`reward_calculator.rs:69`).
2. `epoch_protocol_treasury = epoch_total_reward * protocol_reward_rate` credited to `protocol_treasury_account` (`reward_calculator.rs:78`, `reward_calculator.rs:84`). If `genesis_protocol_version == PROD_GENESIS_PROTOCOL_VERSION` (29) the rate is hardcoded to `1/10` (`reward_calculator.rs:63`).
3. The remaining `epoch_validator_reward` is split pro-rata by stake, scaled by an online multiplier `min(1, (uptime - online_min)/(online_max - online_min))` (`reward_calculator.rs:119`–`140`); a validator below `online_min_threshold`, or with zero expected blocks/chunks/endorsements, gets 0 (`reward_calculator.rs:109`). The `production_ratio` blends block, chunk and endorsement ratios via `get_validator_online_ratio` (validator_stats), applying the endorsement cutoff.
4. Returns `(rewards_map, actual_minted)`, where `actual_minted` sums treasury + all validator rewards (`reward_calculator.rs:143`) — this is `minted_amount` stored on the next `EpochInfo`.
Inflation-curve economics (why `max_inflation_rate` is what it is) live in [economics](economics.md).

### 8. Protocol-version vote tally

Inside `collect_blocks_info` (`lib.rs:544`): each block producer's most-recent voted version is weighted by that producer's stake (`lib.rs:547`). `total_block_producer_stake` is the deduped sum of block-producer stakes (`lib.rs:534`). The highest-staked version becomes `next_next_epoch_version` iff its stake `> total_block_producer_stake * protocol_upgrade_stake_threshold`, otherwise the current `next_epoch_info.protocol_version()` is kept (`lib.rs:567`–`580`). Validators voting below the resulting version are kicked out (step 6). See [protocol-versioning](protocol-versioning.md) for how votes are emitted and how the on/off schedule gates them.

### 9. Shard layout choice and chunk-producer assignment

- **Layout for T+2**: `next_next_shard_layout` (`lib.rs:679`) returns the static layout if `next_next_epoch_config` defines one (`lib.rs:694`); else, if dynamic resharding isn't yet active in T, reuses the T+1 layout (`lib.rs:700`); else, if `block_info.shard_split()` names a shard+boundary, derives a new V3 layout via `derive_v3` (`lib.rs:738`), otherwise reuses T+1's.
- **Strategy**: `AssignmentStrategy::select` (`mod.rs:170`) → `CarryOver` if layouts equal; `Fresh` if `StickyReshardingValidatorAssignment` is off; else `StickyResharding` (falling back to `Fresh` if sticky construction fails).
- **Assignment**: `assign_chunk_producers_to_shards` (`mod.rs:409`) errors `NotEnoughValidators` if `chunk_producers.len() < min_validators_per_shard`. When there are too few producers to fill every shard without repeats (`< min_validators_per_shard * num_shards`), it uses `assign_to_satisfy_shards` (repeats allowed, strategy ignored — `mod.rs:442`); otherwise `assign_to_balance_shards` (`mod.rs:446`) seeds from the strategy (`get_initial_chunk_producer_assignment`, `mod.rs:248`), places new validators onto the least-populated shards, then rebalances via random moves from the largest to smallest shard until balanced or `chunk_producer_assignment_changes_limit` moves are used (`mod.rs:327`–`375`). Sticky resharding overrides the change limit (`needs_changes_limit_override`, `mod.rs:196`) to top up freshly split children in one epoch.
- **Shuffle**: if `shuffle_shard_assignment_for_chunk_producers` is set, the whole `chunk_producers_settlement` is shuffled with the shard-assignment RNG (`validator_selection.rs:236`) — controlled by `ShuffleShardAssignments` (nightly only, see below).

### 10. Per-height producer/validator sampling

Given a finished `EpochInfo` (V3+), producers are sampled stake-weighted and deterministically from `rng_seed` + height (+ shard):
- **Block producer**: `sample_block_producer(height)` seeds `StakeWeightedIndex` with `hash(rng_seed || height)` (`epoch_info.rs:535`, seed at `epoch_info.rs:632`).
- **Chunk producer**: `sample_chunk_producer(shard_layout, shard_id, height)` samples the per-shard `chunk_producers_sampler` from `hash(rng_seed || height || shard_id)` (`epoch_info.rs:560`, seed at `epoch_info.rs:639`).
- **Chunk validators**: `sample_chunk_validators(height)` shuffles the `validator_mandates` with a ChaCha20 RNG seeded by `hash(rng_seed || height)` (`epoch_info.rs:597`, `epoch_info.rs:654`); empty for V1–V3.
Pre-V3 `EpochInfo` sampled block/chunk producers by round-robin (`height % settlement.len()`, `epoch_info.rs:537`/`568`) rather than stake-weighted.

## Interactions

- **Consumes**: `Stake`-action proposals and block/chunk/endorsement production stats accumulated during the epoch by the `EpochInfoAggregator`; block timestamps (for `epoch_duration`); shard-split requests on the last block (`block_info.shard_split()`), from [sharding-chunks](sharding-chunks.md)/dynamic resharding.
- **Produces**: `EpochInfo` (validator set, settlements, samplers, `stake_change`, `validator_reward`, `validator_kickout`, `minted_amount`, `seat_price`, `shard_layout`), consumed by [consensus-finality](consensus-finality.md) (block-producer/approver schedule + `ApprovalStake`), [sharding-chunks](sharding-chunks.md) (chunk producer/validator assignment, shard layout), and [chain-block-processing](chain-block-processing.md).
- **Rewards** feed inflation accounting in [economics](economics.md). **Version tally** is the input to [protocol-versioning](protocol-versioning.md). **Stake changes** originate from [runtime-execution](runtime-execution.md). Resharding *migration* mechanics: [sharding-chunks](sharding-chunks.md).

## Protocol-version-gated behavior

Activation versions verified against `core/primitives-core/src/version.rs` in this tree (`protocol_version` match, `version.rs:458`). `STABLE_PROTOCOL_VERSION = 86` (`version.rs:628`); `MIN_SUPPORTED_PROTOCOL_VERSION = 83` (`version.rs:600`). MAINNET is scheduled to vote PV 86 on 2026-07-20 (`core/primitives/src/version.rs:60`).

| Feature | Activates | Effect on this component |
| --- | --- | --- |
| `DynamicResharding` | **v85** (`version.rs:564`) | `EpochInfo::new` emits **V5** (carries `shard_layout` + `last_resharding`) instead of V4 (`epoch_info.rs:227`); enables the shard-split path in `next_next_shard_layout` (`lib.rs:704`). |
| `StickyReshardingValidatorAssignment` | **v85** (`version.rs:565`) | `AssignmentStrategy::select` returns `StickyResharding` (sticky-by-`ShardId`) instead of `Fresh` on layout changes (`mod.rs:178`); child shards inherit parent producers via bin-packing (`sticky_resharding.rs`). |
| `ContinuousEpochSync`, `ValidateBlockOrdinalAndEpochSyncDataHash`, `GasKeys`, `StrictNonce`, `PostQuantumSignatures`, `UniqueChunkTransactions`, `DelegateV2`, … | **v85** (`version.rs:559`–`575`) | Batch of features stabilized at v85; only the two above alter epoch/validator selection logic here. |
| `EnforcePerReceiptStorageProofLimit` | **v86** (`version.rs:576`) | The sole PV-86 feature on this release; it governs witness storage-proof limits (see [stateless-validation](stateless-validation.md)), not epoch/validator logic. |
| `ShuffleShardAssignments` | **nightly, v143** (`version.rs:582`) | Gates `shuffle_shard_assignment_for_chunk_producers`; the config flag is false on stable, so `chunk_producers_settlement` is not shuffled at v86 (`validator_selection.rs:236`). |
| `EarlyKickout` | **nightly, v152** (`version.rs:583`) | Pre-computes/persists chunk-producer assignments in `DBCol::ChunkProducers` for early kickout; **not active at v86**. |

Note: the removed/renamed features an older baseline would list here are now deprecated placeholders: `_DeprecatedChunkOnlyProducers`/`_DeprecatedMaxKickoutStake` (v56), `_DeprecatedStatelessValidation` (v69), `_DeprecatedFixStakingThreshold`/`_DeprecatedFixChunkProducerStakingThreshold` (v74), `_DeprecatedChunkEndorsementsInBlockHeader` (v72). Their behavior is baked in unconditionally at v86 (min supported is v83). There is **no `FixContractLoadingError`** feature on this release; the PV-86 feature is `EnforcePerReceiptStorageProofLimit`.

## Invariants & failure modes

- **Slashing is inactive.** `ValidatorKickoutReason::_UnusedSlashed` is variant 0 and deprecated (`types.rs:1255`); no code path produces a slash. Stake is only ever returned or lost via kickout/unstake, never confiscated.
- **Never kick out everyone.** Enforced by the `all_kicked_out` guard that restores the top block producer (`lib.rs:507`).
- **Kickout stake is capped** at `validator_max_kickout_stake_perc` via `compute_exempted_kickout` (`lib.rs:452`).
- **Two-epoch staking lag** is structural: T+2's `EpochInfo` is computed at the end of T and stored under `EpochId(last_block_of_T)` (`lib.rs:910`).
- **Proposals must be unique per account** — `debug_assert!` in `proposals_to_epoch_info` (`validator_selection.rs:179`); genesis must carry zero proposals — `assert_eq!` (`lib.rs:944`).
- **Determinism**: all sampling is a pure function of `(rng_seed, height, shard_id)` (`epoch_info.rs:632`/`639`/`654`); shard-assignment randomness uses `EpochInfo::shard_assignment_rng` seeded from `rng_seed` (`epoch_info.rs:671`) so all honest nodes derive the identical settlement.
- **Everyone unstaked** → selection returns `ThresholdError`/`NotEnoughValidators`; handled by cloning prev `EpochInfo` and bumping height (`lib.rs:896`), so the chain does not stall.
- **Too few chunk producers for a shard** → `assign_chunk_producers_to_shards` returns `NotEnoughValidators` (`mod.rs:423`), surfaced as `EpochError::NotEnoughValidators` (`validator_selection.rs:152`).
- **`ProtocolVersionTooOld` bars re-entry** the following epoch (`validator_selection.rs:303`), preventing a validator that voted stale from immediately rejoining.
- **Balancing assertion**: `assign_to_balance_shards` asserts it never exceeds the hard change limit and never balances a shard with itself (`mod.rs:345`, `mod.rs:351`) — a panic here indicates a duplicate chunk producer in the prev assignment.

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `chain/epoch-manager/src/lib.rs:797` | `finalize_epoch` | Orchestrates T+2 computation on the last block of T. |
| `chain/epoch-manager/src/lib.rs:516` | `collect_blocks_info` | Aggregates proposals/stats, runs version tally + kickouts, builds `EpochSummary`. |
| `chain/epoch-manager/src/lib.rs:372` | `compute_validators_to_reward_and_kickout` | Uptime/miss kickouts + stake cap + never-kick-all guard. |
| `chain/epoch-manager/src/lib.rs:544` | (in `collect_blocks_info`) | Protocol-version vote tally against `protocol_upgrade_stake_threshold`. |
| `chain/epoch-manager/src/lib.rs:679` | `next_next_shard_layout` | Chooses the T+2 shard layout (static / reuse / derive-split). |
| `chain/epoch-manager/src/lib.rs:1596` | `is_next_block_in_next_epoch_impl` | Finality-driven epoch boundary test. |
| `chain/epoch-manager/src/lib.rs:1659` | `get_next_epoch_id_from_info` | `EpochId = hash(last block of prev epoch)` — the T-2 rule. |
| `chain/epoch-manager/src/lib.rs:933` | `record_block_info_impl` | Stamps `epoch_id`/`epoch_first_block`; triggers `finalize_epoch`. |
| `chain/epoch-manager/src/validator_selection.rs:166` | `proposals_to_epoch_info` | Full selection → `EpochInfo` build. |
| `chain/epoch-manager/src/validator_selection.rs:45` | `select_validators_from_proposals` | Three independent role selections; threshold = min of the three. |
| `chain/epoch-manager/src/validator_selection.rs:357` | `select_validators` | Top-N-by-stake with `minimum_stake_ratio` cutoff; threshold formula. |
| `chain/epoch-manager/src/validator_selection.rs:290` | `apply_epoch_update_to_proposals` | Merges proposals + prior set + rewards; bars stale-version re-entry. |
| `chain/epoch-manager/src/validator_selection.rs:405` | `OrderedValidatorStake::key` | Ordering `(stake, Reverse(account_id))`. |
| `chain/epoch-manager/src/reward_calculator.rs:51` | `calculate_reward` | Inflation reward, treasury cut, online-scaled pro-rata split. |
| `chain/epoch-manager/src/shard_assignment/mod.rs:170` | `AssignmentStrategy::select` | CarryOver / Fresh / StickyResharding. |
| `chain/epoch-manager/src/shard_assignment/mod.rs:409` | `assign_chunk_producers_to_shards` | Satisfy-vs-balance shard assignment. |
| `chain/epoch-manager/src/shard_assignment/mod.rs:287` | `assign_to_balance_shards` | New-validator placement + capped rebalancing. |
| `core/primitives/src/epoch_info.rs:199` | `EpochInfo::new` | V4/V5 emit switch on `DynamicResharding`; builds samplers. |
| `core/primitives/src/epoch_info.rs:535` | `sample_block_producer` | Stake-weighted (V3+) / round-robin (≤V2) block producer per height. |
| `core/primitives/src/epoch_info.rs:560` | `sample_chunk_producer` | Per-shard chunk producer sampling. |
| `core/primitives/src/epoch_info.rs:597` | `sample_chunk_validators` | Mandate-based chunk validator sampling (V4+). |
| `core/primitives/src/epoch_manager.rs:103` | `EpochConfig` | Per-version tunables. |
| `core/primitives/src/types.rs:594` | `ValidatorStake` | V1-only staking/validator record. |
| `core/primitives/src/types.rs:1253` | `ValidatorKickoutReason` | Kickout taxonomy; slot 0 unused-slashed. |
| `core/primitives-core/src/version.rs:458` | `ProtocolFeature::protocol_version` | v85/v86 activation of the features above (v85 arm at `version.rs:559`, v86 at `version.rs:576`). |

## Open questions

- None.
