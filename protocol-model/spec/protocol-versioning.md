# Protocol versioning & upgrades

> Protocol version: 88 (stable) · Release: 2.14.0 · Derived from commit: bc4e9e8 · Generated: 2026-11-03
> Primary crates/files: `core/primitives-core/src/version.rs`, `core/primitives-core/src/types.rs`, `core/primitives/src/version.rs`, `core/primitives/src/upgrade_schedule.rs`, `core/parameters/src/config_store.rs`, `core/parameters/src/parameter_table.rs`, `core/parameters/res/runtime_configs/`, `chain/epoch-manager/src/lib.rs`, `chain/epoch-manager/src/epoch_info_aggregator.rs`, `chain/client/src/client.rs`, `chain/network/src/peer/peer_actor.rs`

## Role

This component defines the *protocol version* — a single `u32` that names the exact set of consensus rules the whole network agrees to run at a given epoch — and the machinery that (a) gates individual behavioral changes on that number, (b) selects a `RuntimeConfig` for it, and (c) drives the on-chain vote that advances it at epoch boundaries. Every other component's "Protocol-version-gated behavior" section ultimately resolves against the `ProtocolFeature` registry and the `enabled(version)` predicate defined here (`core/primitives-core/src/version.rs:661` — `ProtocolFeature::enabled`). The vote tally that turns validator ballots into the next epoch's version lives in the epoch manager — see [epoch-validators-staking](epoch-validators-staking.md); this spec covers the version/feature/config mechanism and the schedule that decides *what* a node votes for.

## Key data structures

- **`ProtocolVersion`** — `core/primitives-core/src/types.rs:50` — `pub type ProtocolVersion = u32` (re-exported at `core/primitives/src/version.rs:17`). There is no enum wrapping; a raw integer *is* the protocol version. Ordering is the whole semantics: a feature is active iff `current_version >= feature_activation_version`.

- **`ProtocolFeature`** — `core/primitives-core/src/version.rs:11` — a `#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]` enum whose variants are the registry of every protocol change that has ever gated behavior. Once a feature's activation version drops below `MIN_SUPPORTED_PROTOCOL_VERSION` the variant is renamed to `_Deprecated*` and marked `#[deprecated]`, its call sites are deleted, and the behavior becomes unconditional; the variant itself is kept so the historical version→feature map stays intact. Live named variants are the ones code still branches on. Each variant maps to its activation version via `protocol_version()` (`core/primitives-core/src/version.rs:514`). The *declaration order* of variants carries no meaning — only the `protocol_version()` match does. Two variants declared among the stable-era features, `ReceiptPromiseInputSizeLimit` (`version.rs:450`) and `FixMlDsaCostCharging` (`version.rs:487`), now map to the nightly version 157 (see §1).

- **`STABLE_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:698` — `= 88` (was 87 on 2.14.0-rc.1, 86 on 2.13.0). The version a stable (non-nightly) binary runs and ultimately votes for.
- **`NIGHTLY_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:701` — `= 157`, unchanged since rc.1. Chosen big enough to enable every nightly-only feature.
- **`SPICE_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:707` — `= 200`. Note this is the *binary ceiling* for a spice build; the `Spice` feature itself activates at 180 (`version.rs:656`).
- **`PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:710` — the largest version this binary supports, chosen at compile time by `cfg!`: spice ⇒ 200, else nightly ⇒ 157, else stable ⇒ 88.
- **`MIN_SUPPORTED_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:670` — `= 84`, unchanged since rc.1. The oldest version the current binary can still process; behavior gated below this has been folded into the baseline and the corresponding code paths deleted.
- **`PROD_GENESIS_PROTOCOL_VERSION`** — `core/primitives-core/src/version.rs:667` — `= 29`, the mainnet/testnet genesis version.

- **`ProtocolUpgradeVotingSchedule`** — `core/primitives/src/upgrade_schedule.rs:31` — holds the `client_protocol_version` and a sorted `Vec<(DateTime<Utc>, ProtocolVersion)>`. Decides which version a node *votes* for at a given wall-clock time. An empty schedule means "always vote for the client version" (`new_immediate`, `upgrade_schedule.rs:45`).

- **`RuntimeConfigStore`** — `core/parameters/src/config_store.rs:71` — a `BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>` mapping each version *at which parameters changed* to the fully-materialized config. Lookups floor to the greatest key `<=` the requested version.

- **`ParameterTable` / `ParameterTableDiff`** — `core/parameters/src/parameter_table.rs:346` / `:362` — a `BTreeMap<Parameter, ParameterValue>` and a keyed set of `(old, new)` edits. Both are `pub(crate)`; diffs are applied in version order onto a base table to build each version's config.

## Behavior

### 1. Feature gating: compile-time membership vs. runtime activation

Two distinct mechanisms, and only one is authoritative for mainnet consensus:

1. **Compile-time (`cfg!(feature = "nightly"/"protocol_feature_spice")`)** only picks the *ceiling* — `PROTOCOL_VERSION` — that this binary will ever run/vote for (`core/primitives-core/src/version.rs:710`). Nightly-only features get activation versions above the stable ceiling (`FixContractLoadingCost => 129`, `ShuffleShardAssignments => 143`, `FixMlDsaCostCharging => 157`, `ReceiptPromiseInputSizeLimit => 157`, `Spice => 180`; `version.rs:648-656`) so a stable binary, capped at 88, can never reach them.
2. **Runtime (`ProtocolFeature::enabled(current_version)`)** is authoritative: `protocol_version >= self.protocol_version()` (`core/primitives-core/src/version.rs:661-663`). Consensus code branches on the *epoch's* protocol version, not on cargo features. So the same stable binary changes behavior purely as a function of the number it observes on-chain.

`protocol_version()` (`core/primitives-core/src/version.rs:514`) is a `const fn` giant match returning each variant's activation version. The tail of the stable range on this tree:

- v83: only `_Deprecated*` variants (`_DeprecatedInvalidTxGenerateOutcomes`, `_DeprecatedExcludeExistingCodeFromWitnessForCodeLen`, `_DeprecatedFixAccessKeyAllowanceCharging`, `_DeprecatedIncludeDeployGlobalContractOutcomeBurntStorage`, `_DeprecatedGlobalContractDistributionNonce`, `_DeprecatedInstantPromiseYield`, `_DeprecatedYieldResumeImprovements`, `_DeprecatedEthImplicitGlobalContract`, `_DeprecatedInstantDeleteAccount`) — `version.rs:605-613`.
- v84 (`MIN_SUPPORTED`): `_DeprecatedWasmtime` — `version.rs:614`. Wasmtime is the only VM backend, so there is nothing left to gate.
- v85: a large live batch — `FixDelegateActionDepositWithFunctionCallError`, `FixDeleteAccountGlobalContractStorageUsage`, `FixDelegatedDeterministicStateInit`, `GasKeys`, `ContinuousEpochSync`, `DynamicResharding`, `StickyReshardingValidatorAssignment`, `StrictNonce`, `PostQuantumSignatures`, `UniqueChunkTransactions`, `ValidateBlockOrdinalAndEpochSyncDataHash`, `YieldWithId`, `ExecutionMetadataV4`, `SignedContractCodeResponse`, `ClampOutgoingGasAdmission`, `AccountCostIncrease`, `DelegateV2` — `version.rs:615-631`.
- v86: `EnforcePerReceiptStorageProofLimit` — `version.rs:632`.
- v87: ten features, one arm per line at `version.rs:633-642`.
- v88 (`STABLE_PROTOCOL_VERSION`, what this release ultimately votes in): `UniversalAccounts`, `RefundRepeatedUniversalStateInitDeposit`, `CanonicalUniversalStateInit` — `version.rs:643-645`.

See the tables in *Protocol-version-gated behavior* for what each feature does.

Note the v85 `DelegateV2` feature is *not* removed at 87; it is neutralized by a second feature, `RejectDelegateV2` (`version.rs:635`), which rejects `Action::DelegateV2` while leaving the wire variant in place for a future delegate version (`version.rs:453-460`).

**De-stabilization between rc.1 and the final release.** Two features that 2.14.0-rc.1 activated at 87 were moved out of the stable range and now activate only at the nightly version 157:

| Feature | rc.1 | 2.14.0 | Moved by |
|---|---|---|---|
| `ReceiptPromiseInputSizeLimit` | 87 | 157 | #16557, then #16561 put it back at 87 and lifted it at 88, then #16575 settled on 157 |
| `FixMlDsaCostCharging` | 87 | 157 | #16527 |

The move has two halves and **both** must happen, because these two features are each half-gated by a runtime-config flag:

1. the `protocol_version()` match arm moved from the `=> 87` group into the nightly group (`version.rs:652-653`) — the enum variant declaration itself did not move (`version.rs:450`, `:487`);
2. the yaml parameter moved from `87.yaml` into `157.yaml` — `max_receipt_total_input_size: {old: 4_294_967_295, new: 4_194_944}` and `fix_ml_dsa_cost_charging: {old: false, new: true}` (`core/parameters/res/runtime_configs/157.yaml:8,17`).

Consequence at stable 87/88: the promise-input bound is not active (the parameter keeps its unlimited base value `4_294_967_295`, so `TotalPromiseInputSizeExceeded` is unreachable), and the ML-DSA gas-key exec/send length fix and the meta-transaction receiver-shard compute metering are not active. `CHANGELOG.md` under `## [2.14.0]` → `### Protocol Changes` still carries the ML-DSA cost-charging bullet (#16064); that bullet is stale — the code above is authoritative.

### 2. Assembling the `RuntimeConfig` for a version (base + ordered diffs)

`RuntimeConfigStore::new` (`core/parameters/src/config_store.rs:86`):

1. Parse the embedded base file `res/runtime_configs/parameters.yaml` into a `ParameterTable` (`config_store.rs:22,87-88`).
2. Materialize it into a `RuntimeConfig` and store it under key `0` (`config_store.rs:93-99`). `RuntimeConfig::new` is a thin wrapper over `TryFrom<&ParameterTable>` (`core/parameters/src/config.rs:50`).
3. Iterate `CONFIG_DIFFS` — a hard-coded, version-ordered list of `(ProtocolVersion, yaml)` diff files (`config_store.rs:26-64`) — and for each: parse the diff, `params.apply_diff(diff)` to mutate the running table in place, then snapshot a fresh `RuntimeConfig` under that version (`config_store.rs:115-151`). Because diffs mutate a single accumulating table, each stored config is the base plus *all* diffs up to and including its version.
4. `apply_diff` (`parameter_table.rs:532`) is strict: for each parameter it checks the recorded `old` value matches the table's current value, erroring with `NoOldValueExists` / `OldValueExists` / `WrongOldValue` otherwise (`parameter_table.rs:538-554`); a `new` of `None` deletes the parameter. This makes the diff chain self-verifying against drift *within one binary*.
5. The materialization `RuntimeConfig::try_from(&ParameterTable)` (`parameter_table.rs:409`) reads typed parameters into fees, wasm/VM config, congestion-control, witness, and bandwidth-scheduler sub-configs.

**Diff table on this release.** The stable tail of `CONFIG_DIFFS` is `83.yaml`, `84.yaml`, `85.yaml`, `87.yaml`, **`88.yaml`** (new this release, `config_store.rs:59`); `129.yaml`, `155.yaml`, `157.yaml` are nightly-only (`config_store.rs:60-63`). There is still no `86.yaml` — PV 86 introduced no parameter changes and reuses the v85 config. The list starts at `53.yaml` (`config_store.rs:29`), consistent with `MIN_SUPPORTED = 84`.

`87.yaml` now carries **six** entries (it lost three relative to rc.1 — see §1 and §4): `burnt_gas_reward` 3/10 → 0/1 (`RemoveGasRewards`), `min_contract_size_per_local` newly added with value 2 (no `old` key — one local per two bytes of code), `max_state_init_entries` 4_294_967_295 → 1_500 (its comment was rewritten from "a state-init action" to "the state-init actions in one receipt … in total", i.e. the cap is **per receipt**; the count itself recurses into delegate actions — `core/primitives/src/action/mod.rs:518` — `Action::state_init_counts`, enforced by `validate_state_init_counts`, `runtime/runtime/src/action_validation.rs:48`), and the three feature flags `ml_dsa_verify_host_fn`, `fix_contract_loading_error`, `sha3_host_fns` flipped false → true. The SHA3-256/384/512 and ML-DSA-65 *host functions* are gated only by these config flags, not by a `ProtocolFeature` variant.

`88.yaml` (`core/parameters/res/runtime_configs/88.yaml`) carries the parameter half of the v88 universal-account features — six entries:

| Parameter | old → new | Why |
|---|---|---|
| `universal_accounts` | `false` → `true` | the `0u` scheme (moved here from `87.yaml`) |
| `canonical_universal_state_init` | `false` → `true` | pairs with `CanonicalUniversalStateInit`; new base-table key (`parameters.yaml:330`, `parameters_testnet.yaml:325`, both `false`) |
| `action_universal_state_init_per_entry` | send 0 → 3_100_000_000 (exec unchanged at 200 Ggas) | the send side now decodes and canonicality-checks the payload per entry |
| `action_universal_state_init_per_byte` | send 72M → 78M, exec 70M → 74M | the canonical decode/re-encode pass |
| `wasm_universal_state_init_to_account_id_base` | 8_520_000_000 → 8_900_000_000 | host fn now decodes + checks canonical before hashing |
| `wasm_universal_state_init_to_account_id_byte` | 21_471_105 → 83_100_000 | same |

`157.yaml` already existed at rc.1 with a single entry (`max_globals_per_contract: { new: 100_000 }`, previously unset, so the per-contract globals cap is real on nightly); it **gained** `fix_ml_dsa_cost_charging` and `max_receipt_total_input_size` when those two features were de-stabilized (§1).

Lookup: `get_config(version)` does a `BTreeMap::range((Unbounded, Included(version))).next_back()` — the greatest key `<=` version — and panics if none exists (`config_store.rs:238-246`). So versions *between* two diff entries reuse the lower entry's config.

`for_chain_id` (`config_store.rs:168`) layers chain-specific overrides on top: testnet overrides the genesis (v0) config for historical compatibility (`config_store.rs:170-173`); benchmarknet and congestion-control-test mutate the `PROTOCOL_VERSION` entry (`config_store.rs:174-204`).

**A version's config is a property of the binary, not of the chain.** `CONFIG_DIFFS` and the `protocol_version()` match are compile-time constants, and nothing on-chain commits to either. Re-cutting a diff file for a version that some network has already activated therefore changes the rules of that version *retroactively*, for every node that installs the new binary; `apply_diff`'s old-value chain only checks that one binary's diffs are internally consistent, so it does not catch this. The insta snapshots under `core/parameters/src/snapshots/` (one per `CONFIG_DIFFS` version, plus `testnet_*` variants) make such a change visible in review but do not prevent it. See *Invariants & failure modes* and *Open questions* for the instance of this in 2.14.0.

### 3. What a node votes for (client side)

Each block a producer emits carries a `latest_protocol_version` field set from the schedule. In `Client` block production the next epoch's version is read from the epoch manager (`chain/client/src/client.rs:1140` — `get_epoch_protocol_version`), and the value passed to `Block::produce` is `self.upgrade_schedule.protocol_version_to_vote_for(now_utc, next_epoch_protocol_version)` (`chain/client/src/client.rs:1174`). This is the only production call site of the schedule.

`protocol_version_to_vote_for_at_date` (`core/primitives/src/upgrade_schedule.rs:130`, unchanged this release):

1. If the next epoch's version already `>=` the client's own ceiling, vote for the client version (nothing higher to offer) — `upgrade_schedule.rs:136-138`.
2. If the schedule is empty, vote for the client version immediately — `upgrade_schedule.rs:140-142`.
3. Otherwise walk the sorted schedule; for each `(time, version)` where `now >= time`, adopt it, and stop as soon as an adopted version exceeds the next-epoch version (`upgrade_schedule.rs:149-158`). Net effect: the node votes for the highest scheduled version whose date has passed, but never more than one scheduled step past the version the network is already going to run.

### 4. The mainnet/testnet schedule on this release — a two-step 86 → 87 → 88 activation

`get_protocol_upgrade_schedule(chain_id)` (`core/primitives/src/version.rs:37`) now returns a **two-entry** schedule for both production chains (it was a single entry on testnet and *empty* on mainnet at rc.1):

| Chain | Entry 1 | Entry 2 | Anchor |
|---|---|---|---|
| MAINNET | 87 @ 2026-11-04 00:00 UTC | 88 @ 2026-11-06 00:00 UTC | `core/primitives/src/version.rs:59-71` |
| TESTNET | 87 @ 2026-09-23 00:00 UTC | 88 @ 2026-10-21 00:00 UTC | `core/primitives/src/version.rs:72-84` |
| anything else | — (empty ⇒ vote for the client version immediately) | | `core/primitives/src/version.rs:85-88` |

At the generation date (2026-11-03) both testnet dates have passed and **neither** mainnet date has, so a mainnet node running this release votes for whatever the network is already on (step 3 of §3 breaks on the first entry, leaving `result = next_epoch_protocol_version`).

**Why two steps and not one.** PV 87 is a transitional version whose job is to arm two guards before `UniversalAccounts` turns on at 88:

- `RejectUniversalAccountTransfers` is scoped to the window `[87, 88)` by construction — `RejectUniversalAccountTransfers.enabled(v) && !UniversalAccounts.enabled(v)` (`runtime/runtime/src/action_validation.rs:241-243`). During that window a *new* receipt carrying a `Transfer` to a `0u` account id is refused with `ActionsValidationError::TransferToUniversalAccountNotAllowed`; receipts already in flight keep executing (`mode == ValidateReceiptMode::NewReceipt`). This drains the pipeline of transfers that were funded without the account-creation fee before that fee starts applying at 88.
- `RejectBlockHeaderV7BeforeSpice` starts rejecting `BlockHeaderV7` in non-spice epochs at 87 (`chain/chain/src/chain.rs:941`). Before it, `validate_header` accepted V7 there and treated it as `BlockHeaderV6`; the rejection is gated precisely so that upgraded and not-yet-upgraded nodes agree.

Skipping 87 is structurally impossible anyway: `new_from_env_or_schedule` requires schedule versions to increase by exactly 1 (`upgrade_schedule.rs:113-120`) and the vote walk stops one step past the network version (`upgrade_schedule.rs:155-157`), so the tally can only advance one version per epoch. The two-day gap is operational pacing — it leaves room for the 1–2 epochs between the vote and the activation of 87 before the 88 vote begins, which is exactly what the comment on `get_protocol_upgrade_schedule` asks release engineers to allow ("the protocol upgrade will happen 1-2 epochs (15h-30h) after the set date… At most one protocol version upgrade vote can happen per epoch", `core/primitives/src/version.rs:38-49`).

The schedule is validated by `new_from_env_or_schedule` (`upgrade_schedule.rs:73`): the final entry's version must equal `client_protocol_version` (`PROTOCOL_VERSION` = 88) or it returns `InvalidFinalUpgrade` (`upgrade_schedule.rs:95-102`); entries must be strictly increasing in datetime (`InvalidDateTimeOrder`, `:104-111`) and increase by exactly 1 in version (`InvalidProtocolVersionOrder`, `:113-120`). Env var `NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE` can replace the schedule with `now` (empty ⇒ immediate) or `sequential` (one bump per epoch from `MIN_SUPPORTED + 1` to the client version) — tests only (`upgrade_schedule.rs:78-90,195-211`).

### 5. Tallying votes into the next epoch's version (epoch side)

Per-validator votes are collected during aggregation: `EpochInfoAggregator::update_tail` step 3 records each sampled block producer's `latest_protocol_version` into `version_tracker` keyed by validator id, first-vote-wins per producer (`chain/epoch-manager/src/epoch_info_aggregator.rs:194-198`).

At the epoch's last block, `collect_blocks_info` (`chain/epoch-manager/src/lib.rs:852`) tallies:

1. Sum stake per voted version into `versions` (`lib.rs:882-888`); `total_block_producer_stake` is the deduplicated block-producer settlement stake (`lib.rs:870-878`).
2. Take the version with the max stake (`lib.rs:903-904`). Iteration order is non-deterministic but safe, because at most one version can clear the threshold.
3. Compute `threshold = total_block_producer_stake * protocol_upgrade_stake_threshold`, where the fraction comes from the epoch config for the *current* next-epoch version (`lib.rs:898-912`). Default threshold is `8/10` = 80% (`core/chain-configs/src/genesis_config.rs:63` — `default_protocol_upgrade_stake_threshold`).
4. If the top version's stake `> threshold`, the network moves to it, else it stays at `next_epoch_info.protocol_version()` (`lib.rs:913-916`). Because the client votes only one scheduled step at a time (§3) and the schedule increments by 1, this advances at most one version per epoch.
5. Validators still voting for a version `< next_next_epoch_version` are kicked out with `ValidatorKickoutReason::ProtocolVersionTooOld` (`lib.rs:921-936`). Nodes that never upgrade lose their seat once the network moves on.

The chosen version applies to the epoch after next (`next_next_epoch_version` in the `EpochSummary`, `lib.rs:999`), consistent with the Upgradability NEP: voting in the last block of epoch X ⇒ active in the first block of X+2.

### 6. Version compatibility at the network layer

`PROTOCOL_VERSION` / `MIN_SUPPORTED_PROTOCOL_VERSION` also bound *peer* compatibility, independent of consensus. An inbound handshake outside `[MIN_SUPPORTED, PROTOCOL_VERSION]` is answered with `HandshakeFailureReason::ProtocolVersionMismatch` (`chain/network/src/peer/peer_actor.rs:564-579`); an outbound peer receiving that reply retries at `min(their_version, PROTOCOL_VERSION)` and gives up (`ClosingReason::HandshakeFailed`) if the common version falls below either side's floor (`peer_actor.rs:913-937`).

Raising `STABLE_PROTOCOL_VERSION` from 87 to 88 raises a stable binary's `PROTOCOL_VERSION` to 88, but `MIN_SUPPORTED` stays at 84, so the supported ranges of a 2.14.0-rc.1 node (`[84, 87]`) and a 2.14.0 node (`[84, 88]`) still overlap and the two handshake fine. The network layer therefore does **not** separate the two binaries during a rolling upgrade; only the consensus layer can (see *Invariants & failure modes*).

## Interactions

- **Consumes**: wall-clock time (`self.clock.now_utc()`) and `next_epoch_protocol_version` from the epoch manager when producing a block's vote (`chain/client/src/client.rs:1140,1174`).
- **Produces**: (a) the `enabled(version)` predicate consumed by *every* protocol component to gate behavior; (b) a `RuntimeConfig` per version, consumed by [runtime-execution](runtime-execution.md) and fee/gas logic; (c) the `latest_protocol_version` vote embedded in block headers.
- **Vote tally / kickouts / next-epoch version** are owned by [epoch-validators-staking](epoch-validators-staking.md) — this spec links to `collect_blocks_info` but does not re-document epoch-summary construction. The per-version *epoch* config (as distinct from the runtime config) is resolved by `EpochConfigStore::for_protocol_version` (`core/primitives/src/epoch_manager.rs:342`), called from the tally at `chain/epoch-manager/src/lib.rs:900`.
- **Parameter → config materialization** feeds [state-storage](state-storage.md) and gas accounting via `RuntimeConfig` sub-structs (fees, wasm limits, storage costs).
- The v87/v88 action-validation gates reach into [runtime-execution](runtime-execution.md) (`runtime/runtime/src/action_validation.rs`) and [accounts-keys](accounts-keys.md) (the `0u` scheme); `RejectBlockHeaderV7BeforeSpice` reaches into [chain-block-processing](chain-block-processing.md) and [data-structures-serialization](data-structures-serialization.md) (`BlockHeaderV8`, added in the same PR).
- **View/archival paths** clamp rather than gate: `clamp_to_supported_protocol_version` is applied before building a view runtime (`chain/chain/src/runtime/mod.rs:1864,1893`) and `assert_supported_protocol_version` enforces the clamp at the callee (`runtime/runtime/src/state_viewer/mod.rs:137,416`).
- Docs `docs/practices/protocol_upgrade.md` and `docs/ChainSpec/Upgradability.md` describe this process; both remain accurate on the 80% / X+2 mechanics and on `ProtocolVersion` being a bare `u32` type alias. Neither was updated this release, and neither states a policy for changing an already-activated version's rules.

## Protocol-version-gated behavior

This component *is* the gating mechanism; it does not itself branch on features for mainnet consensus. The registry (`ProtocolFeature`) and the `protocol_version()` map (`core/primitives-core/src/version.rs:514`) are the authoritative source; individual features are documented by their owning components.

Version anchors on this 2.14.0 release:

| Version | Meaning | Anchor |
|---|---|---|
| 29 | Prod genesis version | `version.rs:667` |
| 84 | `MIN_SUPPORTED_PROTOCOL_VERSION`; only `_DeprecatedWasmtime` maps here | `version.rs:614,670` |
| 85 | Large live batch: `GasKeys`, `DynamicResharding`, `DelegateV2`, `StrictNonce`, `PostQuantumSignatures`, `ExecutionMetadataV4`, … | `version.rs:615-631` |
| 86 | `EnforcePerReceiptStorageProofLimit` | `version.rs:632` |
| 87 | Transitional step of this release; the ten features below | `version.rs:633-642` |
| 88 | `STABLE_PROTOCOL_VERSION`; the three universal-account features below | `version.rs:643-645,698` |
| 129 / 143 / 157 / 180 | Nightly/spice-only: `FixContractLoadingCost`, `ShuffleShardAssignments`, `FixMlDsaCostCharging` + `ReceiptPromiseInputSizeLimit`, `Spice` | `version.rs:648-656` |
| 157 / 200 | `NIGHTLY_PROTOCOL_VERSION` / `SPICE_PROTOCOL_VERSION` binary ceilings | `version.rs:701,707` |

The ten features activating at 87 (`version.rs:633-642`), each owned by another component:

| Feature | Variant decl | What it changes (see owning spec) |
|---|---|---|
| `FixContractLoadingError` | `version.rs:448` | Charge the contract-loading fee and finalize as a gas-bearing abort when `Module::deserialize` fails, instead of a zero-gas nop |
| `RejectEmptyMethodName` | `version.rs:452` | Reject `FunctionCall` with empty `method_name` in action validation |
| `RejectDelegateV2` | `version.rs:460` | Reject `Action::DelegateV2` (disables meta-tx from gas keys); variant kept for a future delegate version |
| `RejectWithdrawFromGasKeyInDelegate` | `version.rs:465` | Reject `WithdrawFromGasKey` nested inside a delegate action |
| `EnforceStorageProofLimitForAllActions` | `version.rs:473` | Extend the per-receipt storage-proof limit past `FunctionCall` to every action kind; fails with `ActionErrorKind::ReceiptStorageProofSizeExceeded` |
| `RemoveGasRewards` | `version.rs:477` | `burnt_gas_reward` 3/10 → 0 (paired with `87.yaml`) |
| `EarlyKickout` | `version.rs:395` | Persist chunk-producer assignments in `DBCol::ChunkProducers`; early chunk-producer kickout |
| `GlobalContractSameChunkCallFix` | `version.rs:413` | Record the deploy so a same-chunk call to a just-distributed global contract resolves |
| `RejectUniversalAccountTransfers` | `version.rs:494` | **New this release.** In the window `[87, 88)` only, reject a new `Transfer` to a `0u` account id (`ActionsValidationError::TransferToUniversalAccountNotAllowed`) — `runtime/runtime/src/action_validation.rs:241-243` |
| `RejectBlockHeaderV7BeforeSpice` | `version.rs:505` | **New this release.** Reject `BlockHeaderV7` in epochs before Spice — `chain/chain/src/chain.rs:941` |

The three features activating at 88 (`version.rs:643-645`):

| Feature | Variant decl | What it changes (see owning spec) |
|---|---|---|
| `UniversalAccounts` | `version.rs:491` | The `0u` account scheme and the `UniversalStateInit` action. **Moved from 87 to 88 this release**; sibling specs that say "stable at 87" for universal accounts are wrong |
| `RefundRepeatedUniversalStateInitDeposit` | `version.rs:497` | **New.** A universal state init on an already-initialized account refunds the whole deposit; before, the deposit also topped up storage staking |
| `CanonicalUniversalStateInit` | `version.rs:502` | **New.** A `UniversalStateInit` is accepted only in its canonical borsh encoding, so each state init has exactly one encoding and one account id; also gates the status returned by the `universal_state_init_to_account_id` host function, and comes with the repriced fees in `88.yaml` |

Two groups of host functions stabilized at 87 have **no** `ProtocolFeature` variant and are gated purely by `87.yaml` config flags: SHA3-256/384/512 (`sha3_host_fns`) and ML-DSA-65 verification (`ml_dsa_verify_host_fn`).

**No longer stable (changed since rc.1).** `ReceiptPromiseInputSizeLimit` (`version.rs:450`) and `FixMlDsaCostCharging` (`version.rs:487`) now activate at 157 (`version.rs:652-653`). Sibling specs must describe both as **nightly-only**, not as stable-at-87: at 87 and 88 the promise-input bound is inert (`max_receipt_total_input_size` keeps its unlimited base value) and the ML-DSA cost-charging fixes do not apply.

Features **deprecated** (activation version below `MIN_SUPPORTED = 84`, so the behavior is unconditional and the gate is gone): the nine `_Deprecated*` variants at 83 (`version.rs:605-613`) and `_DeprecatedWasmtime` at 84 (`version.rs:614`). Sibling specs must not describe these as conditional.

## Invariants & failure modes

- **Monotonic activation of a single feature**: a feature enabled at version V is enabled at all `>= V` (`enabled` uses `>=`, `version.rs:661`). There is no de-activation mechanism for a single feature.
- **…but composite behavior can be windowed.** `RejectUniversalAccountTransfers` is the first live example in the registry of a gate whose predicate is a *conjunction with the negation of a later feature* — `enabled(87) && !UniversalAccounts.enabled(v)` (`runtime/runtime/src/action_validation.rs:241-243`) — so the transfer ban turns on at 87 and off again at 88. The weaker statement that still holds is: each individual `ProtocolFeature::enabled` is monotone; the behavior composed from several of them need not be. Disabling a shipped behavior still requires a *new* feature (e.g. `DelegateV2` @85 neutralized by `RejectDelegateV2` @87).
- **A released version's rules are not immutable.** Nothing in the code pins the meaning of a protocol version once some chain has activated it. The feature→version map and `CONFIG_DIFFS` are compile-time constants with no on-chain commitment; `apply_diff`'s old-value chain is a within-binary consistency check only; per-version insta snapshots (`core/parameters/src/snapshots/near_parameters__config_store__tests__{87,88,testnet_87,testnet_88}.json.snap`) surface such a change in review but do not block it. 2.14.0 exercises this. The final `87.yaml` sets none of `max_receipt_total_input_size`, `fix_ml_dsa_cost_charging`, `universal_accounts`; every release candidate's `87.yaml` sets `universal_accounts` and `max_receipt_total_input_size` (rc.1 … rc.7), and rc.1–rc.3 additionally set `fix_ml_dsa_cost_charging`, which #16527 moved out in rc.4. Testnet's schedule dated the PV-87 vote 2026-09-23 in every rc, so rc.1–rc.3 are the binaries current at that point. Sharper still: `UniversalAccounts` maps to 87 in rc.1 through rc.7 and only the final release moves it to 88, so a node installing 2.14.0 resolves a different PV-87 config than *any* binary testnet could have been running when it activated 87.
- **There is a precedent for the additive alternative, and it did not survive.** Instead of re-cutting an activated version's diff, one can leave that version alone and turn the behavior off at the *next* version. nearcore #13959 (`bace678e`, 2025-07-24) did exactly that for `ReducedGasRefunds`: it renamed the shipped PV-78 variant to `ReducedGasRefundsOriginal` (still ⇒ 78), added a new `ReducedGasRefundsRevert` ⇒ 79 whose parameters went into `79.yaml`, moved `ReducedGasRefunds` itself to nightly 144 with a new `144.yaml`, and added `ProtocolFeature::reduced_gas_refunds_enabled` to express the resulting non-monotone window — `78.yaml` was not touched. That is the shape §1's de-stabilization did *not* use. It is also not visible at TARGET: #13980 (`231dfc8e`, 2025-07-28) reverted #13959, so this tree has no `ReducedGasRefundsRevert`, no `144.yaml` and no `reduced_gas_refunds_enabled` — only `_DeprecatedReducedGasRefunds => 78` (`core/primitives-core/src/version.rs:601`). The same additive shape was the first fix attempted in this release — #16561, "to avoid an implicit protocol upgrade on testnet", kept `ReceiptPromiseInputSizeLimit` at 87 and restored the unlimited bound in `88.yaml` — and #16575 replaced it with the retroactive edit to `87.yaml`.
- **Two binaries claiming the same version can disagree.** Because of the above, any 2.14.0-rc node and a 2.14.0 node both running PV 87 apply different runtime configs (and, for rc.1–rc.7, a different `UniversalAccounts` activation). The divergence is observable only for a receipt that actually touches one of the changed parameters; when it happens it surfaces as a chunk/state-root mismatch (block rejection), not as a version error, because no layer compares config contents across peers. For mainnet, this release schedules the 87 vote at 2026-11-04, which has not passed (§4) — but that only constrains nodes running *this* binary: the rc binaries carried an **empty** mainnet schedule, which makes a node vote for its ceiling immediately (§3 step 2), so a mainnet validator on an rc would have voted 87. What mainnet actually ran is not determinable here (*Open questions*).
- **Config lookup must have a floor**: `get_config` panics `"Not found RuntimeConfig for protocol version {}"` if no stored key `<=` version exists (`config_store.rs:243`). Key `0` always exists, so any `version >= 0` is safe.
- **Diff chain integrity**: `apply_diff` errors (`WrongOldValue` / `OldValueExists` / `NoOldValueExists`, `parameter_table.rs:538-554`) if a diff's declared `old` value disagrees with the accumulated table — construction panics at startup rather than serving a wrong config (`config_store.rs:122-127`). A test asserts every `CONFIG_DIFFS` version has a matching yaml file and vice-versa (`all_configs_are_specified`, `config_store.rs:270`), and `test_canonical_universal_state_init_enabled` (`config_store.rs:471`) asserts the `canonical_universal_state_init` flag and the repriced `universal_state_init_entry` send fee both switch on exactly at `CanonicalUniversalStateInit.protocol_version()`.
- **Schedule well-formedness**: the schedule must end at the client version and be strictly monotone in time and +1 in version, else `new_from_env_or_schedule` returns an error that `get_protocol_upgrade_schedule` `.unwrap()`s into a panic at startup (`core/primitives/src/version.rs:90-95`, `upgrade_schedule.rs:95-120`). The mainnet/testnet two-entry schedules satisfy this: `87, 88` with 88 == `PROTOCOL_VERSION`.
- **Version support floor**: `assert_supported_protocol_version` panics `"protocol version {v} is below minimum supported {MIN}"` for `< 84` (`core/primitives-core/src/version.rs:690-695`); view-only paths instead clamp up via `clamp_to_supported_protocol_version` (`version.rs:680-684`). Archival nodes therefore answer view calls about pre-84 blocks using the v84 config, not the historically correct one.
- **Under-voting ⇒ kickout**: validators announcing a version below the network's next version are kicked (`ProtocolVersionTooOld`, `chain/epoch-manager/src/lib.rs:931`). A minority (≤ 80% stake) voting for a new version cannot advance it — the threshold test is a strict `>` (`lib.rs:913`).
- **Network floor**: peers whose supported range does not intersect `[84, PROTOCOL_VERSION]` cannot handshake at all (`chain/network/src/peer/peer_actor.rs:564`). An rc.1–rc.3 node (ceiling 87; rc.4 onwards already carry `STABLE_PROTOCOL_VERSION = 88`) and a 2.14.0 node (ceiling 88) do intersect, so they stay connected through the rolling upgrade; an rc.1–rc.3 validator simply caps its vote at 87 and is kicked with `ProtocolVersionTooOld` once the network reaches 88.

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives-core/src/types.rs:50` | `ProtocolVersion` | `type ProtocolVersion = u32` |
| `core/primitives-core/src/version.rs:11` | `ProtocolFeature` | Enum registry of all gating features |
| `core/primitives-core/src/version.rs:450` | `ReceiptPromiseInputSizeLimit` | Variant decl; now maps to 157, not 87 |
| `core/primitives-core/src/version.rs:487` | `FixMlDsaCostCharging` | Variant decl; now maps to 157, not 87 |
| `core/primitives-core/src/version.rs:491-505` | variant decls | `UniversalAccounts`, `RejectUniversalAccountTransfers`, `RefundRepeatedUniversalStateInitDeposit`, `CanonicalUniversalStateInit`, `RejectBlockHeaderV7BeforeSpice` |
| `core/primitives-core/src/version.rs:514` | `ProtocolFeature::protocol_version` | Maps each feature to its activation version |
| `core/primitives-core/src/version.rs:601` | match arm | `_DeprecatedReducedGasRefunds => 78` — all that is left of the #13959/#13980 revert pair |
| `core/primitives-core/src/version.rs:614` | match arm | `_DeprecatedWasmtime => 84` (the floor) |
| `core/primitives-core/src/version.rs:632` | match arm | `EnforcePerReceiptStorageProofLimit => 86` |
| `core/primitives-core/src/version.rs:633-642` | match arms | The ten `=> 87` features |
| `core/primitives-core/src/version.rs:643-645` | match arms | The three `=> 88` universal-account features |
| `core/primitives-core/src/version.rs:648-656` | match arms | Nightly/spice: 129 / 143 / 157 / 157 / 180 |
| `core/primitives-core/src/version.rs:661` | `ProtocolFeature::enabled` | Runtime gate: `version >= activation` (authoritative) |
| `core/primitives-core/src/version.rs:667` | `PROD_GENESIS_PROTOCOL_VERSION` | `= 29` |
| `core/primitives-core/src/version.rs:670` | `MIN_SUPPORTED_PROTOCOL_VERSION` | `= 84` |
| `core/primitives-core/src/version.rs:680` | `clamp_to_supported_protocol_version` | Floors view-call version to MIN |
| `core/primitives-core/src/version.rs:690` | `assert_supported_protocol_version` | Panics below MIN |
| `core/primitives-core/src/version.rs:698` | `STABLE_PROTOCOL_VERSION` | `= 88` |
| `core/primitives-core/src/version.rs:701` | `NIGHTLY_PROTOCOL_VERSION` | `= 157` |
| `core/primitives-core/src/version.rs:707` | `SPICE_PROTOCOL_VERSION` | `= 200` |
| `core/primitives-core/src/version.rs:710` | `PROTOCOL_VERSION` | Compile-time binary ceiling (88 / 157 / 200) |
| `core/primitives/src/version.rs:37` | `get_protocol_upgrade_schedule` | Per-chain vote schedule |
| `core/primitives/src/version.rs:59-71` | MAINNET arm | 87 @ 2026-11-04, 88 @ 2026-11-06 |
| `core/primitives/src/version.rs:72-84` | TESTNET arm | 87 @ 2026-09-23, 88 @ 2026-10-21 |
| `core/primitives/src/upgrade_schedule.rs:31` | `ProtocolUpgradeVotingSchedule` | Sorted (time, version) ballot schedule |
| `core/primitives/src/upgrade_schedule.rs:73` | `new_from_env_or_schedule` | Builds + validates schedule; env override for tests |
| `core/primitives/src/upgrade_schedule.rs:113` | +1 check | `InvalidProtocolVersionOrder` — no version may be skipped |
| `core/primitives/src/upgrade_schedule.rs:130` | `protocol_version_to_vote_for_at_date` | Picks the version to vote for now |
| `core/parameters/src/config_store.rs:26` | `CONFIG_DIFFS` | Ordered version→diff-file table |
| `core/parameters/src/config_store.rs:59` | `(88, include_config!("88.yaml"))` | New diff entry this release |
| `core/parameters/src/config_store.rs:86` | `RuntimeConfigStore::new` | Base + ordered-diff config assembly |
| `core/parameters/src/config_store.rs:168` | `RuntimeConfigStore::for_chain_id` | Chain-specific config overrides |
| `core/parameters/src/config_store.rs:238` | `RuntimeConfigStore::get_config` | Floor lookup (greatest key ≤ version) |
| `core/parameters/res/runtime_configs/87.yaml` | v87 diff | `burnt_gas_reward`→0, `min_contract_size_per_local`, `max_state_init_entries`, sha3 / ML-DSA / contract-loading flags |
| `core/parameters/res/runtime_configs/88.yaml` | v88 diff | `universal_accounts`, `canonical_universal_state_init`, repriced state-init entry/byte fees and `universal_state_init_to_account_id` host-fn costs |
| `core/parameters/res/runtime_configs/157.yaml` | nightly diff | `max_globals_per_contract`, plus the de-stabilized `fix_ml_dsa_cost_charging` and `max_receipt_total_input_size` |
| `core/parameters/src/config.rs:50` | `RuntimeConfig::new` | Wrapper over `TryFrom<&ParameterTable>` |
| `core/parameters/src/parameter_table.rs:409` | `TryFrom<&ParameterTable> for RuntimeConfig` | Materialize typed config |
| `core/parameters/src/parameter_table.rs:532` | `ParameterTable::apply_diff` | Strict old-value-checked diff application |
| `chain/client/src/client.rs:1140` | `get_epoch_protocol_version` | Reads next epoch's version for the vote |
| `chain/client/src/client.rs:1174` | block production | Emits the vote via the schedule |
| `chain/epoch-manager/src/epoch_info_aggregator.rs:194` | `update_tail` step 3 | Records per-producer vote in `version_tracker` |
| `chain/epoch-manager/src/lib.rs:852` | `collect_blocks_info` | Stake-weighted tally → next-next-epoch version |
| `chain/epoch-manager/src/lib.rs:903` | max-stake version pick | `versions.into_iter().max_by_key(stake)` |
| `chain/epoch-manager/src/lib.rs:913` | threshold check | `stake > threshold` decides the upgrade |
| `chain/epoch-manager/src/lib.rs:921` | kickout loop | Kicks under-voting validators |
| `chain/network/src/peer/peer_actor.rs:564` | inbound handshake check | Rejects peers outside `[MIN_SUPPORTED, PROTOCOL_VERSION]` with `ProtocolVersionMismatch` |
| `chain/network/src/peer/peer_actor.rs:913` | outbound `HandshakeFailure` handling | Retries at `min(their, ours)`; closes if below either floor |
| `runtime/runtime/src/action_validation.rs:48` | `validate_state_init_counts` | Enforces `max_state_init_entries` (87.yaml) per receipt |
| `core/primitives/src/action/mod.rs:518` | `Action::state_init_counts` | Per-receipt entry/key count; recurses into `Delegate`/`DelegateV2` |
| `runtime/runtime/src/action_validation.rs:241` | `validate_transfer_action` | The `[87, 88)` universal-account transfer ban window |
| `chain/chain/src/chain.rs:941` | `validate_header` | `RejectBlockHeaderV7BeforeSpice` gate |
| `core/primitives/src/epoch_manager.rs:342` | `EpochConfigStore::for_protocol_version` | Per-version *epoch* config (distinct from `RuntimeConfig`) |
| `core/chain-configs/src/genesis_config.rs:63` | `default_protocol_upgrade_stake_threshold` | `8/10` (80%) default |

## Open questions

- **Resolved this release**: the rc.1 open question about the empty mainnet entry of `get_protocol_upgrade_schedule`. It was a release-staging placeholder; the final 2.14.0 tree fills it with the two-step `87 @ 2026-11-04`, `88 @ 2026-11-06` schedule (`core/primitives/src/version.rs:59-71`). Until a date passes, a mainnet node keeps voting for the version the network is already on, instead of voting for its ceiling immediately as an empty schedule would have made it do.
- The PV-87 rules differ between every 2.14.0 release candidate (rc.1 … rc.7) and the final release: the rcs enable `universal_accounts` at 87 (both the feature arm and the parameter), the rcs through rc.3 also enable `fix_ml_dsa_cost_charging` there, and all seven set `max_receipt_total_input_size` there (§2, *Invariants*). Testnet's own schedule dated the 87 vote 2026-09-23 and the 88 vote 2026-10-21. What cannot be determined from this repository: which binaries testnet validators actually ran at either date, whether any receipt in those epochs touched one of those parameters or created a `0u` account under PV 87 (and so whether a replay of that range actually diverges), whether testnet was reset, whether mainnet validators ran an rc binary — whose empty mainnet schedule would have made them vote 87 immediately — and whether a limited-replayability policy covers this — no `docs/` page states one, and the only trace of such a policy in code is a `TODO(limited_replayability)` comment (`core/parameters/src/config_store.rs:192`). Treat this as a reviewed risk, not a confirmed defect.
- `SPICE_PROTOCOL_VERSION` is 200 while the `Spice` feature activates at 180 and `NIGHTLY_PROTOCOL_VERSION` is 157. The comment at `version.rs:703-706` explains spice is not yet part of nightly, but the code does not say what occupies 158–179 or 181–199, i.e. whether those ranges are reserved for anything.
