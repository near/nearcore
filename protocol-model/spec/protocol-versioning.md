# Protocol versioning & upgrades

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `core/primitives-core/src/version.rs`, `core/primitives/src/version.rs`, `core/primitives/src/upgrade_schedule.rs`, `core/parameters/src/config_store.rs`, `core/parameters/src/parameter_table.rs`, `chain/epoch-manager/src/lib.rs`, `chain/client/src/client.rs`

## Role

This component defines the *protocol version* — a single `u32` that names the exact set of consensus rules the whole network agrees to run at a given epoch — and the machinery that (a) gates individual behavioral changes on that number, (b) selects a `RuntimeConfig` for it, and (c) drives the on-chain vote that advances it at epoch boundaries. Every other component's "Protocol-version-gated behavior" section ultimately resolves against the `ProtocolFeature` registry and `enabled(version)` predicate defined here (`core/primitives-core/src/version.rs:591` — `ProtocolFeature::enabled`). The vote tally that turns validator ballots into the next epoch's version lives in the epoch manager — see [epoch-validators-staking](epoch-validators-staking.md); this spec covers the version/feature/config mechanism and the schedule that decides *what* a node votes for.

## Key data structures

- **`ProtocolVersion`** — `core/primitives-core/src/types.rs` (re-exported at `core/primitives/src/version.rs:17`) — a `u32`. There is no enum wrapping; a raw integer *is* the protocol version. Ordering is the whole semantics: a feature is active iff `current_version >= feature_activation_version`.

- **`ProtocolFeature`** — `core/primitives-core/src/version.rs:11` — a `#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]` enum whose variants are the registry of every protocol change that has ever gated behavior. Long-dead variants are kept as `#[deprecated] _Deprecated*` placeholders so the historical version→feature map stays intact; live named variants are the ones code still branches on. Each variant maps to the version at which it activates via `protocol_version()` (`core/primitives-core/src/version.rs:458`).

- **`STABLE_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:628` — `= 86`. The version a stable (non-nightly) binary runs and ultimately votes for.
- **`NIGHTLY_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:631` — `= 155`. Big enough to enable all nightly-only features.
- **`SPICE_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:637` — `= 200`.
- **`PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:640` — the largest version this binary supports, chosen at compile time by `cfg!`: spice ⇒ 200, else nightly ⇒ 155, else stable ⇒ 86.
- **`MIN_SUPPORTED_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:600` — `= 83`. The oldest version the current binary can still process (older VM backends etc. have been removed).
- **`PROD_GENESIS_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:597` — `= 29`, the mainnet/testnet genesis version.

- **`ProtocolUpgradeVotingSchedule`** — `core/primitives/src/upgrade_schedule.rs:31` — holds the `client_protocol_version` and a sorted `Vec<(DateTime<Utc>, ProtocolVersion)>`. Decides which version a node *votes* for at a given wall-clock time. An empty schedule means "always vote for the client version" (`new_immediate`, `upgrade_schedule.rs:45`).

- **`RuntimeConfigStore`** — `core/parameters/src/config_store.rs:73` — `BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>` mapping each version *at which parameters changed* to the fully-materialized config. Lookups floor to the greatest key `<=` the requested version.

- **`ParameterTable` / `ParameterTableDiff`** — `core/parameters/src/parameter_table.rs:346` / `:362` — a `BTreeMap<Parameter, ParameterValue>` and a keyed set of `(old, new)` edits. Diffs are applied in version order onto a base table to build each version's config.

## Behavior

### 1. Feature gating: compile-time membership vs. runtime activation

Two distinct mechanisms, and only one is authoritative for mainnet consensus:

1. **Compile-time (`cfg!(feature = "nightly"/"protocol_feature_spice")`)** only picks the *ceiling* — `PROTOCOL_VERSION` — that this binary will ever run/vote for (`core/primitives-core/src/version.rs:640`). Nightly-only features get activation versions above the stable ceiling (`FixContractLoadingCost => 129`, `ShuffleShardAssignments => 143`, `EarlyKickout => 152`, `Spice => 180`; `version.rs:579-586`) so a stable binary, capped at 86, can never reach them.
2. **Runtime (`ProtocolFeature::enabled(current_version)`)** is authoritative: `protocol_version >= self.protocol_version()` (`core/primitives-core/src/version.rs:591-593`). Consensus code branches on the *epoch's* protocol version, not on cargo features. So the same stable binary changes behavior purely as a function of the number it observes on-chain.

`protocol_version()` (`core/primitives-core/src/version.rs:458`) is a `const fn` giant match returning each variant's activation version. On this 2.13.0 tree the tail of the stable range is:
- v83: `ExcludeExistingCodeFromWitnessForCodeLen`, `InvalidTxGenerateOutcomes`, `FixAccessKeyAllowanceCharging`, `IncludeDeployGlobalContractOutcomeBurntStorage`, `GlobalContractDistributionNonce`, `EthImplicitGlobalContract` (plus several `_Deprecated*`) — `version.rs:549-557`. These sit at the current `MIN_SUPPORTED` floor.
- v84: `Wasmtime` — `version.rs:558`.
- v85: a large batch including `GasKeys`, `DynamicResharding`, `DelegateV2`, `StrictNonce`, `PostQuantumSignatures`, `ExecutionMetadataV4`, `UniqueChunkTransactions`, `ContinuousEpochSync`, `StickyReshardingValidatorAssignment`, `ClampOutgoingGasAdmission`, `AccountCostIncrease`, `YieldWithId`, `SignedContractCodeResponse`, `ValidateBlockOrdinalAndEpochSyncDataHash`, and the fix features — `version.rs:559-575`.
- v86 (the PV this release votes in): **`EnforcePerReceiptStorageProofLimit`** — `version.rs:576`. This is the sole named v86 feature on 2.13.0.

### 2. Assembling the `RuntimeConfig` for a version (base + ordered diffs)

`RuntimeConfigStore::new` (`core/parameters/src/config_store.rs:88`):
1. Parse the embedded base file `res/runtime_configs/parameters.yaml` into a `ParameterTable` (`config_store.rs:22,90`).
2. Materialize it into a `RuntimeConfig` and store it under key `0` (`config_store.rs:95-101`).
3. Iterate `CONFIG_DIFFS` — a hard-coded, version-ordered list of `(ProtocolVersion, yaml)` diff files (`config_store.rs:26-66`) — and for each: parse the diff, `params.apply_diff(diff)` to mutate the running table in place, then snapshot a fresh `RuntimeConfig` under that version (`config_store.rs:117-153`). Because diffs mutate a single accumulating table, each stored config is the base plus *all* diffs up to and including its version.
4. `apply_diff` (`parameter_table.rs:526`) is strict: for each parameter it checks the recorded `old` value matches the table's current value, erroring with `WrongOldValue` / `NoOldValueExists` / `OldValueExists` otherwise (`parameter_table.rs:530-550`); a `new` of `None` deletes the parameter (`parameter_table.rs:552-556`). This makes the diff chain self-verifying against drift.
5. The materialization `RuntimeConfig::try_from(&ParameterTable)` (`parameter_table.rs:409`) reads typed parameters into fees, wasm/VM config, congestion-control, witness, and bandwidth-scheduler sub-configs.

Lookup: `get_config(version)` does a `BTreeMap::range((Unbounded, Included(version))).next_back()` — the greatest key `<=` version — and panics if none exists (`config_store.rs:240-248`). So versions *between* two diff entries reuse the lower entry's config. On this tree `CONFIG_DIFFS` runs through `85.yaml`, then `129.yaml` and `155.yaml` for nightly (`config_store.rs:63-65`); there is no separate `86.yaml`, so PV 86 reuses the v85 config.

`for_chain_id` (`config_store.rs:170`) layers chain-specific overrides on top: testnet overrides genesis (v0) config for historical compatibility (`config_store.rs:172-175`); benchmarknet/congestion-control-test mutate the `PROTOCOL_VERSION` entry (`config_store.rs:176-206`).

### 3. What a node votes for (client side)

Each block a producer emits carries a `latest_protocol_version` field set from the schedule. In `Client` block production (`chain/client/src/client.rs:1174`), the value passed to `Block::produce` is `self.upgrade_schedule.protocol_version_to_vote_for(now_utc, next_epoch_protocol_version)`.

`protocol_version_to_vote_for_at_date` (`core/primitives/src/upgrade_schedule.rs:130`):
1. If the next epoch's version already `>=` the client's own ceiling, vote for the client version (nothing higher to offer) — `upgrade_schedule.rs:136`.
2. If the schedule is empty, vote for the client version immediately — `upgrade_schedule.rs:140`.
3. Otherwise walk the sorted schedule; for each `(time, version)` where `now >= time`, adopt it, stopping once a version exceeds the next-epoch version (`upgrade_schedule.rs:149-158`). Net effect: the node votes for the highest scheduled version whose date has passed, but never jumps more than one scheduled step past the current network version at a time.

### 4. The mainnet/testnet schedule on this release

`get_protocol_upgrade_schedule(chain_id)` (`core/primitives/src/version.rs:37`):
- **MAINNET**: single entry `(2026-07-20 00:00:00 UTC, 86)` (`version.rs:59-66`). So on 2.13.0, mainnet nodes begin voting for PV 86 on 2026-07-20; the actual upgrade lands 1–2 epochs later.
- **TESTNET**: `(2026-06-30 00:00:00 UTC, 85)` (`version.rs:67-74`).
- Unknown chains: empty schedule ⇒ vote immediately for the client version (`version.rs:75-78`).

The schedule is validated by `new_from_env_or_schedule` (`upgrade_schedule.rs:73`): the final entry's version must equal `client_protocol_version` (`PROTOCOL_VERSION`) or it returns `InvalidFinalUpgrade` (`upgrade_schedule.rs:95-102`); entries must be strictly increasing in datetime (`InvalidDateTimeOrder`, `:104-111`) and increase by exactly 1 in version (`InvalidProtocolVersionOrder`, `:113-120`). Env var `NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE` can replace the schedule with `now` (empty ⇒ immediate) or `sequential` (one bump per epoch from `MIN_SUPPORTED+1` to client version) — tests only (`upgrade_schedule.rs:78-90,195-211`).

### 5. Tallying votes into the next epoch's version (epoch side)

At the epoch's last block, `collect_blocks_info` (`chain/epoch-manager/src/lib.rs:516`) tallies. The per-validator votes are collected during aggregation: `EpochInfoAggregator::update_tail` records each block producer's `latest_protocol_version` into `version_tracker` keyed by validator id, first-vote-wins per producer (`chain/epoch-manager/src/epoch_info_aggregator.rs:187-191`).

Tally (`chain/epoch-manager/src/lib.rs:544-580`):
1. Sum stake per voted version into `versions` (`lib.rs:547-552`).
2. Take the version with the max stake (`lib.rs:567-568`).
3. Compute threshold = `total_block_producer_stake * protocol_upgrade_stake_threshold` where the fraction comes from the config for the *current* next-epoch version (`lib.rs:562-576`). Default threshold is `8/10` = 80% (`core/chain-configs/src/genesis_config.rs:63-65` — `default_protocol_upgrade_stake_threshold`).
4. If the top version's stake `> threshold`, the network moves to it (`next_next_epoch_version = version`), else it stays (`lib.rs:577`). Because the client votes only one scheduled step at a time (§3) and the schedule increments by 1, this advances at most one version per epoch.
5. Validators still voting for a version `< next_next_epoch_version` are kicked out with `ValidatorKickoutReason::ProtocolVersionTooOld` (`lib.rs:588-600`). Nodes that never upgrade thus lose their seat once the network moves on.

The chosen version applies to the epoch after next (the block-N+2 rule): it becomes `next_next_epoch_version` in the summary, consistent with the Upgradability NEP (voting in the last block of epoch X ⇒ active in first block of X+2).

## Interactions

- **Consumes**: wall-clock time (`self.clock.now_utc()`) and `next_epoch_protocol_version` from the epoch manager, when producing a block's vote (`chain/client/src/client.rs:1174`).
- **Produces**: (a) the `enabled(version)` predicate consumed by *every* protocol component to gate behavior; (b) `RuntimeConfig` per version, consumed by [runtime-execution](runtime-execution.md) and fee/gas logic; (c) the `latest_protocol_version` vote embedded in block headers.
- **Vote tally / kickouts / next-epoch version** are owned by [epoch-validators-staking](epoch-validators-staking.md) — this spec links to `collect_blocks_info` but does not re-document epoch-summary construction.
- **Parameter → config materialization** feeds [state-storage](state-storage.md) and gas accounting via `RuntimeConfig` sub-structs (fees, wasm limits, storage costs).
- Docs `docs/practices/protocol_upgrade.md` and `docs/ChainSpec/Upgradability.md` describe this process; both are accurate on the 80%/X+2 mechanics as of this tree. `Upgradability.md` still describes `ProtocolVersion` as a bare `u32` type alias, which matches reality.

## Protocol-version-gated behavior

This component *is* the gating mechanism; it does not itself branch on features for mainnet consensus. The registry (`ProtocolFeature`) and the `protocol_version()` map (`core/primitives-core/src/version.rs:458`) are the authoritative source; individual features are documented by their owning components. Notable version anchors on this 2.13.0 release:

| Version | Meaning | Anchor |
|---|---|---|
| 29 | Prod genesis version | `version.rs:597` |
| 83 | `MIN_SUPPORTED_PROTOCOL_VERSION`; batch incl. `ExcludeExistingCodeFromWitnessForCodeLen`, `EthImplicitGlobalContract` | `version.rs:549-557,600` |
| 84 | `Wasmtime` | `version.rs:558` |
| 85 | Large batch: `GasKeys`, `DynamicResharding`, `DelegateV2`, `StrictNonce`, `PostQuantumSignatures`, `ExecutionMetadataV4`, … | `version.rs:559-575` |
| 86 | `STABLE_PROTOCOL_VERSION`; sole named feature `EnforcePerReceiptStorageProofLimit` | `version.rs:576,628` |
| 129/143/152/180 | Nightly/spice-only: `FixContractLoadingCost`, `ShuffleShardAssignments`, `EarlyKickout`, `Spice` | `version.rs:579-586` |

Note (2.13.0-specific): there is **no** `FixContractLoadingError` variant on this tree; the loading-cost feature present is nightly `FixContractLoadingCost` (v129). The PV-86 feature is `EnforcePerReceiptStorageProofLimit`, not any loading fix.

## Invariants & failure modes

- **Monotonic activation**: a feature enabled at version V is enabled at all `>= V` (`enabled` uses `>=`, `version.rs:591`). There is no de-activation mechanism; deprecated features remain enabled forever.
- **Config lookup must have a floor**: `get_config` panics `"Not found RuntimeConfig for protocol version {}"` if no stored key `<=` version exists (`config_store.rs:244`). Key `0` always exists, so any `version >= 0` is safe.
- **Diff chain integrity**: `apply_diff` errors (`WrongOldValue` etc., `parameter_table.rs:545`) if a diff's declared `old` value disagrees with the accumulated table — construction panics at startup rather than serving a wrong config (`config_store.rs:124-129`). A test asserts every `CONFIG_DIFFS` version has a matching yaml file and vice-versa (`all_configs_are_specified`, `config_store.rs:272`).
- **Schedule well-formedness**: the schedule must end at the client version and be strictly monotone in time and +1 in version, else `new_from_env_or_schedule` returns an error that `get_protocol_upgrade_schedule` `.unwrap()`s into a panic at startup (`version.rs:80-85`, `upgrade_schedule.rs:95-120`).
- **Version support floor**: `assert_supported_protocol_version` panics `"protocol version {v} is below minimum supported {MIN}"` for `< 83` (`core/primitives-core/src/version.rs:620-625`); view-only paths instead clamp up via `clamp_to_supported_protocol_version` (`version.rs:610-614`).
- **Under-voting ⇒ kickout**: validators announcing a version below the network's next version are kicked (`ProtocolVersionTooOld`, `chain/epoch-manager/src/lib.rs:595`). A minority (≤20% stake) voting for a new version cannot advance it — threshold is strict `>` (`lib.rs:577`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives-core/src/version.rs:11` | `ProtocolFeature` | Enum registry of all gating features |
| `core/primitives-core/src/version.rs:458` | `ProtocolFeature::protocol_version` | Maps each feature to its activation version |
| `core/primitives-core/src/version.rs:576` | match arm | `EnforcePerReceiptStorageProofLimit => 86` (this release's PV) |
| `core/primitives-core/src/version.rs:591` | `ProtocolFeature::enabled` | Runtime gate: `version >= activation` (authoritative) |
| `core/primitives-core/src/version.rs:597` | `PROD_GENESIS_PROTOCOL_VERSION` | `= 29` |
| `core/primitives-core/src/version.rs:600` | `MIN_SUPPORTED_PROTOCOL_VERSION` | `= 83` |
| `core/primitives-core/src/version.rs:610` | `clamp_to_supported_protocol_version` | Floors view-call version to MIN |
| `core/primitives-core/src/version.rs:620` | `assert_supported_protocol_version` | Panics below MIN |
| `core/primitives-core/src/version.rs:628` | `STABLE_PROTOCOL_VERSION` | `= 86` |
| `core/primitives-core/src/version.rs:640` | `PROTOCOL_VERSION` | Compile-time binary ceiling (86/155/200) |
| `core/primitives/src/version.rs:37` | `get_protocol_upgrade_schedule` | Per-chain vote schedule; mainnet PV86 @2026-07-20 |
| `core/primitives/src/upgrade_schedule.rs:31` | `ProtocolUpgradeVotingSchedule` | Sorted (time,version) ballot schedule |
| `core/primitives/src/upgrade_schedule.rs:73` | `new_from_env_or_schedule` | Builds + validates schedule; env override for tests |
| `core/primitives/src/upgrade_schedule.rs:130` | `protocol_version_to_vote_for_at_date` | Picks the version to vote for now |
| `core/parameters/src/config_store.rs:26` | `CONFIG_DIFFS` | Ordered version→diff-file table |
| `core/parameters/src/config_store.rs:88` | `RuntimeConfigStore::new` | Base + ordered-diff config assembly |
| `core/parameters/src/config_store.rs:240` | `RuntimeConfigStore::get_config` | Floor lookup (greatest key ≤ version) |
| `core/parameters/src/parameter_table.rs:409` | `RuntimeConfig::try_from(&ParameterTable)` | Materialize typed config |
| `core/parameters/src/parameter_table.rs:526` | `ParameterTable::apply_diff` | Strict old-value-checked diff application |
| `chain/client/src/client.rs:1174` | block production | Emits the vote via schedule |
| `chain/epoch-manager/src/epoch_info_aggregator.rs:187` | `update_tail` (step 3) | Records per-producer vote in `version_tracker` |
| `chain/epoch-manager/src/lib.rs:544` | `collect_blocks_info` | Stake-weighted tally → next-next-epoch version |
| `chain/epoch-manager/src/lib.rs:588` | kickout loop | Kicks under-voting validators |
| `core/chain-configs/src/genesis_config.rs:63` | `default_protocol_upgrade_stake_threshold` | `8/10` (80%) default |

## Open questions

- None.
</content>
