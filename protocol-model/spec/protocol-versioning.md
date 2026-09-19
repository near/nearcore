# Protocol versioning & upgrades

> Protocol version: 87 (stable) · Release: 2.14.0-rc.1 · Derived from commit: 233252e · Generated: 2026-09-14
> Primary crates/files: `core/primitives-core/src/version.rs`, `core/primitives-core/src/types.rs`, `core/primitives/src/version.rs`, `core/primitives/src/upgrade_schedule.rs`, `core/parameters/src/config_store.rs`, `core/parameters/src/parameter_table.rs`, `core/parameters/res/runtime_configs/`, `chain/epoch-manager/src/lib.rs`, `chain/epoch-manager/src/epoch_info_aggregator.rs`, `chain/client/src/client.rs`, `chain/network/src/peer/peer_actor.rs`

## Role

This component defines the *protocol version* — a single `u32` that names the exact set of consensus rules the whole network agrees to run at a given epoch — and the machinery that (a) gates individual behavioral changes on that number, (b) selects a `RuntimeConfig` for it, and (c) drives the on-chain vote that advances it at epoch boundaries. Every other component's "Protocol-version-gated behavior" section ultimately resolves against the `ProtocolFeature` registry and the `enabled(version)` predicate defined here (`core/primitives-core/src/version.rs:643` — `ProtocolFeature::enabled`). The vote tally that turns validator ballots into the next epoch's version lives in the epoch manager — see [epoch-validators-staking](epoch-validators-staking.md); this spec covers the version/feature/config mechanism and the schedule that decides *what* a node votes for.

## Key data structures

- **`ProtocolVersion`** — `core/primitives-core/src/types.rs:50` — `pub type ProtocolVersion = u32` (re-exported at `core/primitives/src/version.rs:17`). There is no enum wrapping; a raw integer *is* the protocol version. Ordering is the whole semantics: a feature is active iff `current_version >= feature_activation_version`.

- **`ProtocolFeature`** — `core/primitives-core/src/version.rs:11` — a `#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]` enum whose variants are the registry of every protocol change that has ever gated behavior. Once a feature's activation version drops below `MIN_SUPPORTED_PROTOCOL_VERSION` the variant is renamed to `_Deprecated*` and marked `#[deprecated]`, its call sites are deleted, and the behavior becomes unconditional; the variant itself is kept so the historical version→feature map stays intact. Live named variants are the ones code still branches on. Each variant maps to its activation version via `protocol_version()` (`core/primitives-core/src/version.rs:500`).

- **`STABLE_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:680` — `= 87`. The version a stable (non-nightly) binary runs and ultimately votes for. (Was 86 on 2.13.0.)
- **`NIGHTLY_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:683` — `= 157` (was 155). Chosen big enough to enable every nightly-only feature.
- **`SPICE_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:689` — `= 200`. Note this is the *binary ceiling* for a spice build; the `Spice` feature itself activates at 180 (`version.rs:638`).
- **`PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:692` — the largest version this binary supports, chosen at compile time by `cfg!`: spice ⇒ 200, else nightly ⇒ 157, else stable ⇒ 87.
- **`MIN_SUPPORTED_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:652` — `= 84` (was 83). The oldest version the current binary can still process; behavior gated below this has been folded into the baseline and the corresponding code paths deleted.
- **`PROD_GENESIS_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:649` — `= 29`, the mainnet/testnet genesis version.

- **`ProtocolUpgradeVotingSchedule`** — `core/primitives/src/upgrade_schedule.rs:31` — holds the `client_protocol_version` and a sorted `Vec<(DateTime<Utc>, ProtocolVersion)>`. Decides which version a node *votes* for at a given wall-clock time. An empty schedule means "always vote for the client version" (`new_immediate`, `upgrade_schedule.rs:45`).

- **`RuntimeConfigStore`** — `core/parameters/src/config_store.rs:70` — a `BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>` mapping each version *at which parameters changed* to the fully-materialized config. Lookups floor to the greatest key `<=` the requested version.

- **`ParameterTable` / `ParameterTableDiff`** — `core/parameters/src/parameter_table.rs:346` / `:362` — a `BTreeMap<Parameter, ParameterValue>` and a keyed set of `(old, new)` edits. Both are `pub(crate)`; diffs are applied in version order onto a base table to build each version's config.

## Behavior

### 1. Feature gating: compile-time membership vs. runtime activation

Two distinct mechanisms, and only one is authoritative for mainnet consensus:

1. **Compile-time (`cfg!(feature = "nightly"/"protocol_feature_spice")`)** only picks the *ceiling* — `PROTOCOL_VERSION` — that this binary will ever run/vote for (`core/primitives-core/src/version.rs:692`). Nightly-only features get activation versions above the stable ceiling (`FixContractLoadingCost => 129`, `ShuffleShardAssignments => 143`, `Spice => 180`; `version.rs:632-638`) so a stable binary, capped at 87, can never reach them.
2. **Runtime (`ProtocolFeature::enabled(current_version)`)** is authoritative: `protocol_version >= self.protocol_version()` (`core/primitives-core/src/version.rs:643-645`). Consensus code branches on the *epoch's* protocol version, not on cargo features. So the same stable binary changes behavior purely as a function of the number it observes on-chain.

`protocol_version()` (`core/primitives-core/src/version.rs:500`) is a `const fn` giant match returning each variant's activation version. The tail of the stable range on this tree:

- v83: only `_Deprecated*` variants (`_DeprecatedInvalidTxGenerateOutcomes`, `_DeprecatedExcludeExistingCodeFromWitnessForCodeLen`, `_DeprecatedFixAccessKeyAllowanceCharging`, `_DeprecatedIncludeDeployGlobalContractOutcomeBurntStorage`, `_DeprecatedGlobalContractDistributionNonce`, `_DeprecatedInstantPromiseYield`, `_DeprecatedYieldResumeImprovements`, `_DeprecatedEthImplicitGlobalContract`, `_DeprecatedInstantDeleteAccount`) — `version.rs:591-599`. Six of these nine were still live-named on 2.13.0 and were renamed this release; `_DeprecatedInstantPromiseYield`, `_DeprecatedYieldResumeImprovements` and `_DeprecatedInstantDeleteAccount` were already `_Deprecated*` on 2.13.0 (they were unreachable under the old `MIN_SUPPORTED = 83` too, since `enabled` is `>=`).
- v84 (the new `MIN_SUPPORTED`): `_DeprecatedWasmtime` — `version.rs:600`. Wasmtime is now the only VM backend; the NearVM engine crates were deleted outright, so there is nothing left to gate.
- v85: a large live batch — `FixDelegateActionDepositWithFunctionCallError`, `FixDeleteAccountGlobalContractStorageUsage`, `FixDelegatedDeterministicStateInit`, `GasKeys`, `ContinuousEpochSync`, `DynamicResharding`, `StickyReshardingValidatorAssignment`, `StrictNonce`, `PostQuantumSignatures`, `UniqueChunkTransactions`, `ValidateBlockOrdinalAndEpochSyncDataHash`, `YieldWithId`, `ExecutionMetadataV4`, `SignedContractCodeResponse`, `ClampOutgoingGasAdmission`, `AccountCostIncrease`, `DelegateV2` — `version.rs:601-617`.
- v86: `EnforcePerReceiptStorageProofLimit` — `version.rs:618`.
- v87 (the PV this release votes in): eleven features, listed one arm per line at `version.rs:619-629` — see the table in *Protocol-version-gated behavior*.

Note the v85 `DelegateV2` feature is *not* removed at 87; it is neutralized by a second feature, `RejectDelegateV2` (`version.rs:621`), which rejects `Action::DelegateV2` while leaving the wire variant in place for a future delegate version (`version.rs:453-460`).

### 2. Assembling the `RuntimeConfig` for a version (base + ordered diffs)

`RuntimeConfigStore::new` (`core/parameters/src/config_store.rs:85`):

1. Parse the embedded base file `res/runtime_configs/parameters.yaml` into a `ParameterTable` (`config_store.rs:22,86`).
2. Materialize it into a `RuntimeConfig` and store it under key `0` (`config_store.rs:92-98`). `RuntimeConfig::new` is a thin wrapper over `TryFrom<&ParameterTable>` (`core/parameters/src/config.rs:50`).
3. Iterate `CONFIG_DIFFS` — a hard-coded, version-ordered list of `(ProtocolVersion, yaml)` diff files (`config_store.rs:26-63`) — and for each: parse the diff, `params.apply_diff(diff)` to mutate the running table in place, then snapshot a fresh `RuntimeConfig` under that version (`config_store.rs:114-150`). Because diffs mutate a single accumulating table, each stored config is the base plus *all* diffs up to and including its version.
4. `apply_diff` (`parameter_table.rs:530`) is strict: for each parameter it checks the recorded `old` value matches the table's current value, erroring with `NoOldValueExists` / `OldValueExists` / `WrongOldValue` otherwise (`parameter_table.rs:536-554`); a `new` of `None` deletes the parameter (`parameter_table.rs:556-560`). This makes the diff chain self-verifying against drift.
5. The materialization `RuntimeConfig::try_from(&ParameterTable)` (`parameter_table.rs:409`) reads typed parameters into fees, wasm/VM config, congestion-control, witness, and bandwidth-scheduler sub-configs.

**Diff table on this release.** `CONFIG_DIFFS` now starts at `53.yaml`: the entries for protocol versions 46, 48, 49, 50 and 52 were deleted along with their yaml files, consistent with `MIN_SUPPORTED = 84` (they only ever mattered for replaying pre-53 history). The stable tail is `83.yaml`, `84.yaml`, `85.yaml`, **`87.yaml`** (new); `129.yaml`, `155.yaml` and **`157.yaml`** (new) are nightly-only (`config_store.rs:55-62`). There is still no `86.yaml` — PV 86 introduced no parameter changes and reuses the v85 config.

`87.yaml` carries the parameter half of the v87 features — nine entries in all (`core/parameters/res/runtime_configs/87.yaml`): `burnt_gas_reward` 3/10 → 0/1 (`RemoveGasRewards`), `max_receipt_total_input_size` 4_294_967_295 → 4_194_944 (`ReceiptPromiseInputSizeLimit` — 4 MiB `max_length_returned_data` plus 640 bytes of per-input framing), `max_state_init_entries` 4_294_967_295 → 1_500, `min_contract_size_per_local` *newly added* with value 2 (no `old` key — one local per two bytes of code), and the five feature flags `ml_dsa_verify_host_fn`, `sha3_host_fns`, `fix_contract_loading_error`, `fix_ml_dsa_cost_charging`, `universal_accounts` flipped false → true. Note the SHA3-256/384/512 and ML-DSA-65 host functions are gated *only* by these config flags, not by a `ProtocolFeature` variant.

Lookup: `get_config(version)` does a `BTreeMap::range((Unbounded, Included(version))).next_back()` — the greatest key `<=` version — and panics if none exists (`config_store.rs:237-245`). So versions *between* two diff entries reuse the lower entry's config.

`for_chain_id` (`config_store.rs:167`) layers chain-specific overrides on top: testnet overrides the genesis (v0) config for historical compatibility (`config_store.rs:169-172`); benchmarknet and congestion-control-test mutate the `PROTOCOL_VERSION` entry (`config_store.rs:173-203`).

### 3. What a node votes for (client side)

Each block a producer emits carries a `latest_protocol_version` field set from the schedule. In `Client` block production the next epoch's version is read from the epoch manager (`chain/client/src/client.rs:1139` — `get_epoch_protocol_version`), and the value passed to `Block::produce` is `self.upgrade_schedule.protocol_version_to_vote_for(now_utc, next_epoch_protocol_version)` (`chain/client/src/client.rs:1173-1174`). This is the only production call site of the schedule.

`protocol_version_to_vote_for_at_date` (`core/primitives/src/upgrade_schedule.rs:130`, unchanged this release):

1. If the next epoch's version already `>=` the client's own ceiling, vote for the client version (nothing higher to offer) — `upgrade_schedule.rs:136`.
2. If the schedule is empty, vote for the client version immediately — `upgrade_schedule.rs:140`.
3. Otherwise walk the sorted schedule; for each `(time, version)` where `now >= time`, adopt it, stopping once a version exceeds the next-epoch version (`upgrade_schedule.rs:149-158`). Net effect: the node votes for the highest scheduled version whose date has passed, but never jumps more than one scheduled step past the current network version at a time.

### 4. The mainnet/testnet schedule on this release

`get_protocol_upgrade_schedule(chain_id)` (`core/primitives/src/version.rs:37`):

- **TESTNET**: single entry `(2026-09-21 00:00:00 UTC, 87)` (`version.rs:63-70`). This is the 2.14.0-rc.1 testnet release, so testnet nodes begin voting for PV 87 on 2026-09-21; the upgrade lands 1–2 epochs later.
- **MAINNET**: **empty** on this tree (`version.rs:59-62`). An empty schedule short-circuits to "vote for the client version immediately" (`upgrade_schedule.rs:140`), so a mainnet-chain-id node built from this rc would vote for 87 at once. This is a release-staging artifact: the mainnet `(datetime, 87)` entry is expected to be filled in for the final 2.14.0 mainnet release. (On 2.13.0 mainnet carried `(2026-07-20, 86)`.)
- Unknown chains: empty schedule ⇒ vote immediately for the client version (`version.rs:71-74`).

The schedule is validated by `new_from_env_or_schedule` (`upgrade_schedule.rs:73`): the final entry's version must equal `client_protocol_version` (`PROTOCOL_VERSION`) or it returns `InvalidFinalUpgrade` (`upgrade_schedule.rs:95-102`); entries must be strictly increasing in datetime (`InvalidDateTimeOrder`, `:104-111`) and increase by exactly 1 in version (`InvalidProtocolVersionOrder`, `:113-120`). Env var `NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE` can replace the schedule with `now` (empty ⇒ immediate) or `sequential` (one bump per epoch from `MIN_SUPPORTED + 1` to the client version) — tests only (`upgrade_schedule.rs:78-90,200-209`).

### 5. Tallying votes into the next epoch's version (epoch side)

Per-validator votes are collected during aggregation: `EpochInfoAggregator::update_tail` step 3 records each sampled block producer's `latest_protocol_version` into `version_tracker` keyed by validator id, first-vote-wins per producer (`chain/epoch-manager/src/epoch_info_aggregator.rs:194-198`).

At the epoch's last block, `collect_blocks_info` (`chain/epoch-manager/src/lib.rs:843`) tallies:

1. Sum stake per voted version into `versions` (`lib.rs:873-879`); `total_block_producer_stake` is the deduplicated block-producer settlement stake (`lib.rs:861-869`).
2. Take the version with the max stake (`lib.rs:894-895`). Iteration order is non-deterministic but safe, because at most one version can clear the threshold.
3. Compute `threshold = total_block_producer_stake * protocol_upgrade_stake_threshold`, where the fraction comes from the epoch config for the *current* next-epoch version (`lib.rs:891,897-903`). Default threshold is `8/10` = 80% (`core/chain-configs/src/genesis_config.rs:63-65` — `default_protocol_upgrade_stake_threshold`).
4. If the top version's stake `> threshold`, the network moves to it, else it stays at `next_epoch_info.protocol_version()` (`lib.rs:904-907`). Because the client votes only one scheduled step at a time (§3) and the schedule increments by 1, this advances at most one version per epoch.
5. Validators still voting for a version `< next_next_epoch_version` are kicked out with `ValidatorKickoutReason::ProtocolVersionTooOld` (`lib.rs:914-927`). Nodes that never upgrade lose their seat once the network moves on.

The chosen version applies to the epoch after next (`next_next_epoch_version` in the `EpochSummary`, `lib.rs:990`), consistent with the Upgradability NEP: voting in the last block of epoch X ⇒ active in the first block of X+2.

### 6. Version compatibility at the network layer

`PROTOCOL_VERSION` / `MIN_SUPPORTED_PROTOCOL_VERSION` also bound *peer* compatibility, independent of consensus. An inbound handshake outside `[MIN_SUPPORTED, PROTOCOL_VERSION]` is answered with `HandshakeFailureReason::ProtocolVersionMismatch` (`chain/network/src/peer/peer_actor.rs:564-579`); an outbound peer receiving that reply retries at `min(their_version, PROTOCOL_VERSION)` and gives up (`ClosingReason::HandshakeFailed`) if the common version falls below either side's floor (`peer_actor.rs:913-936`). Raising `MIN_SUPPORTED` from 83 to 84 therefore also drops connectivity to binaries whose ceiling is below 84.

## Interactions

- **Consumes**: wall-clock time (`self.clock.now_utc()`) and `next_epoch_protocol_version` from the epoch manager when producing a block's vote (`chain/client/src/client.rs:1139,1173`).
- **Produces**: (a) the `enabled(version)` predicate consumed by *every* protocol component to gate behavior; (b) a `RuntimeConfig` per version, consumed by [runtime-execution](runtime-execution.md) and fee/gas logic; (c) the `latest_protocol_version` vote embedded in block headers.
- **Vote tally / kickouts / next-epoch version** are owned by [epoch-validators-staking](epoch-validators-staking.md) — this spec links to `collect_blocks_info` but does not re-document epoch-summary construction. The per-version *epoch* config (as distinct from the runtime config) is resolved by `EpochConfigStore::for_protocol_version` (`core/primitives/src/epoch_manager.rs:342`), called from the tally at `chain/epoch-manager/src/lib.rs:891`.
- **Parameter → config materialization** feeds [state-storage](state-storage.md) and gas accounting via `RuntimeConfig` sub-structs (fees, wasm limits, storage costs).
- **View/archival paths** clamp rather than gate: `clamp_to_supported_protocol_version` is applied before building a view runtime (`chain/chain/src/runtime/mod.rs:1864,1893`) and `assert_supported_protocol_version` enforces the clamp at the callee (`runtime/runtime/src/state_viewer/mod.rs:137,416`).
- Docs `docs/practices/protocol_upgrade.md` and `docs/ChainSpec/Upgradability.md` describe this process; both remain accurate on the 80% / X+2 mechanics and on `ProtocolVersion` being a bare `u32` type alias. Neither was updated this release.

## Protocol-version-gated behavior

This component *is* the gating mechanism; it does not itself branch on features for mainnet consensus. The registry (`ProtocolFeature`) and the `protocol_version()` map (`core/primitives-core/src/version.rs:500`) are the authoritative source; individual features are documented by their owning components.

Version anchors on this 2.14.0-rc.1 release:

| Version | Meaning | Anchor |
|---|---|---|
| 29 | Prod genesis version | `version.rs:649` |
| 84 | `MIN_SUPPORTED_PROTOCOL_VERSION`; only `_DeprecatedWasmtime` maps here | `version.rs:600,652` |
| 85 | Large live batch: `GasKeys`, `DynamicResharding`, `DelegateV2`, `StrictNonce`, `PostQuantumSignatures`, `ExecutionMetadataV4`, … | `version.rs:601-617` |
| 86 | `EnforcePerReceiptStorageProofLimit` | `version.rs:618` |
| 87 | `STABLE_PROTOCOL_VERSION`; the eleven features below | `version.rs:619-629,680` |
| 129 / 143 / 180 | Nightly/spice-only: `FixContractLoadingCost`, `ShuffleShardAssignments`, `Spice` | `version.rs:632-638` |
| 157 / 200 | `NIGHTLY_PROTOCOL_VERSION` / `SPICE_PROTOCOL_VERSION` binary ceilings | `version.rs:683,689` |

The eleven features newly activating at 87 (`version.rs:619-629`), each owned by another component:

| Feature | Variant decl | What it changes (see owning spec) |
|---|---|---|
| `FixContractLoadingError` | `version.rs:448` | Charge the contract-loading fee and finalize as a gas-bearing abort when `Module::deserialize` fails, instead of a zero-gas nop |
| `ReceiptPromiseInputSizeLimit` | `version.rs:450` | Bound the combined size of one receipt's resolved promise inputs (`max_receipt_total_input_size`) |
| `RejectEmptyMethodName` | `version.rs:452` | Reject `FunctionCall` with empty `method_name` in action validation |
| `RejectDelegateV2` | `version.rs:460` | Reject `Action::DelegateV2` (disables meta-tx from gas keys); variant kept for a future delegate version |
| `RejectWithdrawFromGasKeyInDelegate` | `version.rs:465` | Reject `WithdrawFromGasKey` nested inside a delegate action |
| `EnforceStorageProofLimitForAllActions` | `version.rs:473` | Extend the per-receipt storage-proof limit past `FunctionCall` to every action kind; fails with `ActionErrorKind::ReceiptStorageProofSizeExceeded` |
| `RemoveGasRewards` | `version.rs:477` | `burnt_gas_reward` 3/10 → 0 (paired with `87.yaml`) |
| `EarlyKickout` | `version.rs:395` | Persist chunk-producer assignments in `DBCol::ChunkProducers`; early chunk-producer kickout. **Moved from nightly 152 to stable 87 this release** |
| `FixMlDsaCostCharging` | `version.rs:487` | Gas keys: exec fee on `trie_id_len()`, send fee on `len()`; meta-tx: inner signature-verification compute metered on the receiver shard |
| `GlobalContractSameChunkCallFix` | `version.rs:413` | Record the deploy so a same-chunk call to a just-distributed global contract resolves |
| `UniversalAccounts` | `version.rs:491` | The `0u` account scheme and the `UniversalStateInit` action |

Two groups of host functions stabilized at 87 have **no** `ProtocolFeature` variant and are gated purely by `87.yaml` config flags: SHA3-256/384/512 (`sha3_host_fns`) and ML-DSA-65 verification (`ml_dsa_verify_host_fn`).

Features **deprecated** this release (activation version now below `MIN_SUPPORTED = 84`, so the behavior is unconditional and the gate is gone): `ExcludeExistingCodeFromWitnessForCodeLen`, `InvalidTxGenerateOutcomes`, `FixAccessKeyAllowanceCharging`, `IncludeDeployGlobalContractOutcomeBurntStorage`, `GlobalContractDistributionNonce`, `EthImplicitGlobalContract` (all → `_Deprecated*` at 83, `version.rs:591-599`) and `Wasmtime` (→ `_DeprecatedWasmtime` at 84, `version.rs:600`). Sibling specs must not describe these as conditional.

## Invariants & failure modes

- **Monotonic activation**: a feature enabled at version V is enabled at all `>= V` (`enabled` uses `>=`, `version.rs:643`). There is no de-activation mechanism; deprecated features remain enabled forever. Disabling a shipped behavior requires a *new* feature that rejects it (e.g. `DelegateV2` @85 neutralized by `RejectDelegateV2` @87).
- **Config lookup must have a floor**: `get_config` panics `"Not found RuntimeConfig for protocol version {}"` if no stored key `<=` version exists (`config_store.rs:242`). Key `0` always exists, so any `version >= 0` is safe.
- **Diff chain integrity**: `apply_diff` errors (`WrongOldValue` / `OldValueExists` / `NoOldValueExists`, `parameter_table.rs:536-552`) if a diff's declared `old` value disagrees with the accumulated table — construction panics at startup rather than serving a wrong config (`config_store.rs:121-126`). A test asserts every `CONFIG_DIFFS` version has a matching yaml file and vice-versa (`all_configs_are_specified`, `config_store.rs:269`).
- **Schedule well-formedness**: the schedule must end at the client version and be strictly monotone in time and +1 in version, else `new_from_env_or_schedule` returns an error that `get_protocol_upgrade_schedule` `.unwrap()`s into a panic at startup (`core/primitives/src/version.rs:76-81`, `upgrade_schedule.rs:95-120`).
- **Version support floor**: `assert_supported_protocol_version` panics `"protocol version {v} is below minimum supported {MIN}"` for `< 84` (`core/primitives-core/src/version.rs:672-677`); view-only paths instead clamp up via `clamp_to_supported_protocol_version` (`version.rs:662-666`). Archival nodes therefore answer view calls about pre-84 blocks using the v84 config, not the historically correct one.
- **Under-voting ⇒ kickout**: validators announcing a version below the network's next version are kicked (`ProtocolVersionTooOld`, `chain/epoch-manager/src/lib.rs:922`). A minority (≤ 80% stake) voting for a new version cannot advance it — the threshold test is a strict `>` (`lib.rs:904`).
- **Network floor**: peers whose supported range does not intersect `[84, PROTOCOL_VERSION]` cannot handshake at all (`chain/network/src/peer/peer_actor.rs:564`), so an un-upgraded node is disconnected before it can vote.

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives-core/src/types.rs:50` | `ProtocolVersion` | `type ProtocolVersion = u32` |
| `core/primitives-core/src/version.rs:11` | `ProtocolFeature` | Enum registry of all gating features |
| `core/primitives-core/src/version.rs:500` | `ProtocolFeature::protocol_version` | Maps each feature to its activation version |
| `core/primitives-core/src/version.rs:600` | match arm | `_DeprecatedWasmtime => 84` (the new floor) |
| `core/primitives-core/src/version.rs:618` | match arm | `EnforcePerReceiptStorageProofLimit => 86` |
| `core/primitives-core/src/version.rs:619-629` | match arms | The eleven `=> 87` features of this release |
| `core/primitives-core/src/version.rs:632-638` | match arms | Nightly/spice: 129 / 143 / 180 |
| `core/primitives-core/src/version.rs:643` | `ProtocolFeature::enabled` | Runtime gate: `version >= activation` (authoritative) |
| `core/primitives-core/src/version.rs:649` | `PROD_GENESIS_PROTOCOL_VERSION` | `= 29` |
| `core/primitives-core/src/version.rs:652` | `MIN_SUPPORTED_PROTOCOL_VERSION` | `= 84` |
| `core/primitives-core/src/version.rs:662` | `clamp_to_supported_protocol_version` | Floors view-call version to MIN |
| `core/primitives-core/src/version.rs:672` | `assert_supported_protocol_version` | Panics below MIN |
| `core/primitives-core/src/version.rs:680` | `STABLE_PROTOCOL_VERSION` | `= 87` |
| `core/primitives-core/src/version.rs:683` | `NIGHTLY_PROTOCOL_VERSION` | `= 157` |
| `core/primitives-core/src/version.rs:689` | `SPICE_PROTOCOL_VERSION` | `= 200` |
| `core/primitives-core/src/version.rs:692` | `PROTOCOL_VERSION` | Compile-time binary ceiling (87 / 157 / 200) |
| `core/primitives/src/version.rs:37` | `get_protocol_upgrade_schedule` | Per-chain vote schedule; testnet PV 87 @ 2026-09-21, mainnet empty |
| `core/primitives/src/upgrade_schedule.rs:31` | `ProtocolUpgradeVotingSchedule` | Sorted (time, version) ballot schedule |
| `core/primitives/src/upgrade_schedule.rs:73` | `new_from_env_or_schedule` | Builds + validates schedule; env override for tests |
| `core/primitives/src/upgrade_schedule.rs:130` | `protocol_version_to_vote_for_at_date` | Picks the version to vote for now |
| `core/parameters/src/config_store.rs:26` | `CONFIG_DIFFS` | Ordered version→diff-file table (now starts at 53; adds 87 and 157) |
| `core/parameters/src/config_store.rs:85` | `RuntimeConfigStore::new` | Base + ordered-diff config assembly |
| `core/parameters/src/config_store.rs:167` | `RuntimeConfigStore::for_chain_id` | Chain-specific config overrides |
| `core/parameters/src/config_store.rs:237` | `RuntimeConfigStore::get_config` | Floor lookup (greatest key ≤ version) |
| `core/parameters/res/runtime_configs/87.yaml` | v87 diff | `burnt_gas_reward`→0, `max_receipt_total_input_size`, `max_state_init_entries`, sha3 / ML-DSA / universal-accounts flags |
| `core/parameters/src/config.rs:50` | `RuntimeConfig::new` | Wrapper over `TryFrom<&ParameterTable>` |
| `core/parameters/src/parameter_table.rs:409` | `TryFrom<&ParameterTable> for RuntimeConfig` | Materialize typed config |
| `core/parameters/src/parameter_table.rs:530` | `ParameterTable::apply_diff` | Strict old-value-checked diff application |
| `chain/client/src/client.rs:1139` | `get_epoch_protocol_version` | Reads next epoch's version for the vote |
| `chain/client/src/client.rs:1173` | block production | Emits the vote via the schedule |
| `chain/epoch-manager/src/epoch_info_aggregator.rs:194` | `update_tail` step 3 | Records per-producer vote in `version_tracker` |
| `chain/epoch-manager/src/lib.rs:843` | `collect_blocks_info` | Stake-weighted tally → next-next-epoch version |
| `chain/epoch-manager/src/lib.rs:894` | max-stake version pick | `versions.into_iter().max_by_key(stake)` |
| `chain/epoch-manager/src/lib.rs:904` | threshold check | `stake > threshold` decides the upgrade |
| `chain/epoch-manager/src/lib.rs:914` | kickout loop | Kicks under-voting validators |
| `chain/network/src/peer/peer_actor.rs:564` | inbound handshake check | Rejects peers outside `[MIN_SUPPORTED, PROTOCOL_VERSION]` with `ProtocolVersionMismatch` |
| `chain/network/src/peer/peer_actor.rs:913` | outbound `HandshakeFailure` handling | Retries at `min(their, ours)`; closes if below either floor |
| `core/primitives/src/epoch_manager.rs:342` | `EpochConfigStore::for_protocol_version` | Per-version *epoch* config (distinct from `RuntimeConfig`) |
| `core/chain-configs/src/genesis_config.rs:63` | `default_protocol_upgrade_stake_threshold` | `8/10` (80%) default |

## Open questions

- The mainnet entry of `get_protocol_upgrade_schedule` is empty on this rc tree (`core/primitives/src/version.rs:59-62`), which makes a mainnet-chain-id build vote for 87 immediately. Whether that is intentional for the rc or a placeholder awaiting the mainnet date cannot be determined from code alone; the 2.13.0 tree carried an explicit mainnet entry.
- `SPICE_PROTOCOL_VERSION` is 200 while the `Spice` feature activates at 180 and `NIGHTLY_PROTOCOL_VERSION` is 157. The comment at `version.rs:685-688` explains spice is not yet part of nightly, but the code does not say what occupies 158–179 or 181–199, i.e. whether those ranges are reserved for anything.
