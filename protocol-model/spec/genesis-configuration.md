# Genesis & node configuration

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `core/chain-configs/src/genesis_config.rs`, `core/chain-configs/src/genesis_validate.rs`, `core/chain-configs/src/client_config.rs`, `core/parameters/src/config.rs`, `core/parameters/src/config_store.rs`, `core/parameters/src/parameter_table.rs`, `nearcore/src/config.rs`, `nearcore/src/config_validate.rs`, `nearcore/src/dyn_config.rs`, `core/store/src/genesis/initialization.rs`

## Role

This component bootstraps a NEAR network's and a node's initial conditions. It defines three separable configuration surfaces: (1) the **genesis config** — the immutable, network-wide chain parameters (chain id, genesis height/time, initial protocol version, initial validators, shard layout, economics) plus the initial state records that are hashed into the genesis state root; (2) the **runtime config** — the versioned schema of fees, wasm limits, account-creation, congestion, witness, and bandwidth parameters that feeds [runtime-execution](runtime-execution.md), [contract-vm](contract-vm.md), and [economics](economics.md), selected per protocol version by machinery documented in [protocol-versioning](protocol-versioning.md); and (3) the **node config** (`config.json`) — per-node operational knobs (tracked shards, archival, gc, state-sync source, network) that have no consensus effect except through what state the node keeps and serves. Genesis feeds the genesis block/state root consumed by [chain-block-processing](chain-block-processing.md); the runtime config feeds every execution component; the node config's tracked-shards/gc choices interact with [sync](sync.md).

## Key data structures

- **`GenesisConfig`** — `core/chain-configs/src/genesis_config.rs:110` — the network-wide, consensus-critical parameters. Fields include `protocol_version` (:112, the version genesis runs at), `genesis_time` (:115), `chain_id` (:118), `genesis_height` (:120), `num_block_producer_seats` (:122), `dynamic_resharding` (:124), `protocol_upgrade_stake_threshold` (:129, default 8/10), `epoch_length` (:131), `gas_limit` (:133), `min_gas_price`/`max_gas_price` (:135/:137), kickout thresholds (:139-:144), seat/mandate counts (:148, :223, :226), online thresholds (:153/:158), `validators: Vec<AccountInfo>` (:164), `total_supply` (:176), `protocol_treasury_account` (:181), `shard_layout` (:191), and `minimum_stake_ratio` (:205). It derives `SmartDefault`; many fields carry `#[serde(default = "...")]` so older/newer JSON round-trips. Not versioned as an enum — it is a flat struct evolved by adding defaulted fields.
- **`Genesis`** — `core/chain-configs/src/genesis_config.rs:334` — `{ config: GenesisConfig (#[serde(flatten)]), contents: GenesisContents }`. The invariant that `total_supply` equals the supply implied by records is documented but enforced only at validation time (:329).
- **`GenesisContents`** — `core/chain-configs/src/genesis_config.rs:304` — an `#[serde(untagged)]` enum: `Records { records }` (inline), `RecordsFile { records_file }` (streamed from a side file to bound memory), or `StateRoots { state_roots }` (testing/mock-fork only; cannot recompute a consistent genesis hash — :314).
- **`GenesisRecords`** — `core/chain-configs/src/genesis_config.rs:284` — `Vec<StateRecord>`; the initial accounts, access keys, contracts and data that become the genesis trie. See [state-storage](state-storage.md) for `StateRecord` semantics.
- **`ProtocolConfigView` / `ProtocolConfig`** — `core/chain-configs/src/genesis_config.rs:793` / `:870` — the RPC-facing projection combining `GenesisConfig` fields with a `RuntimeConfigView`; assembled by `From<ProtocolConfig>` (:875).
- **`RuntimeConfig`** — `core/parameters/src/config.rs:17` — the fully-materialized per-version runtime parameters: `fees: Arc<RuntimeFeesConfig>`, `wasm_config: Arc<vm::Config>`, `account_creation_config`, `congestion_control_config`, `witness_config`, `bandwidth_scheduler_config`, plus `use_state_stored_receipt`, `min_gas_purchase_price`, `account_creation_charge` (:22-:46). Sub-structs `CongestionControlConfig` (:135), `WitnessConfig` (:261), `BandwidthSchedulerConfig` (:288), `AccountCreationConfig` (:116).
- **`RuntimeConfigStore`** — `core/parameters/src/config_store.rs:73` — `BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>`. Lookup floors to the greatest key `<=` requested version (`get_config`, :240). See [protocol-versioning](protocol-versioning.md).
- **`ParameterTable` / `ParameterTableDiff`** — `core/parameters/src/parameter_table.rs:346` / `:362` — `BTreeMap<Parameter, ParameterValue>` and a keyed `(old, new)` diff set.
- **`Config`** (node/`config.json`) — `nearcore/src/config.rs:227` — the per-node struct: `genesis_file`, key files, `network`, `consensus`, `tracked_shards_config` (:248) with deprecated fallbacks (:252-:264), `archive` (:267), `save_trie_changes`/`save_tx_outcomes` (:283/:292), `gc: GCConfig` (:334, `#[serde(flatten)]`), `store`/`cold_store`/`split_storage`, `state_sync` (:365).
- **`NearConfig`** — `nearcore/src/config.rs:697` — the assembled runtime bundle: `config`, `client_config: ClientConfig`, `network_config`, `genesis`, `validator_signer`.
- **`TrackedShardsConfig`** — `core/chain-configs/src/client_config.rs:49` — `NoShards | Shards(Vec<ShardUId>) | AllShards | ShadowValidator(AccountId) | Schedule(Vec<Vec<ShardId>>) | Accounts(Vec<AccountId>)`.
- **`GCConfig`** — `core/chain-configs/src/client_config.rs:135` — `gc_blocks_limit` (default 2), `gc_fork_clean_step` (100), `gc_num_epochs_to_keep` (default 5, floored to `MIN_GC_NUM_EPOCHS_TO_KEEP = 3`), `gc_step_period` (500ms) — `client_config.rs:158-163`, `:28`, `:31`.

## Behavior

### 1. Loading and parsing genesis

1. `Genesis::from_file` reads the JSON, strips `//`/`/* */` comments, and deserializes into `Genesis`, then runs `validate` — `core/chain-configs/src/genesis_config.rs:574` — `Genesis::from_file`. `from_files` (:610) parses config and a separate records file.
2. `GenesisConfig` itself parses via `from_json`/`from_file`; `from_json` **panics** (not returns `Result`) on malformed JSON — `genesis_config.rs:352` — `GenesisConfig::from_json`.
3. Records may be streamed rather than fully loaded: `for_each_record` dispatches on `GenesisContents`, iterating the in-memory `Vec` or streaming a records file via `stream_records_from_file` — `genesis_config.rs:687` — `Genesis::for_each_record`. The streaming deserializer reads only the `records` field and ignores all others, erroring on a missing or duplicate `records` field — `genesis_config.rs:462` — `RecordsProcessor::visit_map`.

### 2. Genesis validation (`GenesisValidationMode::Full`)

`validate` routes `Full` to `validate_genesis`, while `UnsafeFast` logs "skipped genesis validation" and returns Ok — `core/chain-configs/src/genesis_config.rs:655` — `Genesis::validate`. `validate_genesis` short-circuits to Ok for `StateRoots` contents (records cannot be validated) — `genesis_validate.rs:10` — `validate_genesis`. Otherwise it streams every record through `GenesisValidator::process_record` (`genesis_validate.rs:53`) accumulating totals, then `validate_processed_records` (`genesis_validate.rs:85`) enforces, among others:
- every validator's public key `is_valid_staking_key`, no duplicate validators, non-empty validator set;
- `sum(account.amount + account.locked)` over records equals `genesis_config.total_supply` (else "wrong total supply.");
- the set of accounts with `locked > 0` matches the validator set;
- every access-key/contract account exists as an `Account` record;
- `online_min < online_max <= 1`, numerator/denominator bounds `< 10_000_000`, `gas_price_adjustment_rate < 1`, `epoch_length > 0`.
All violations are collected (not fail-fast) and returned as one `ValidationError::GenesisSemanticsError` — `genesis_validate.rs:85` / `:195` — `GenesisValidator::validate_processed_records` / `result_with_full_error`.

### 3. Producing the genesis state root

At node startup `initialize_sharded_genesis_state` is the entry point — `core/store/src/genesis/initialization.rs:29`. Order:
1. If the store already holds genesis state roots, reuse them; it re-asserts the stored genesis height equals the config's (`initialization.rs:38-49`).
2. Else, if a `state_dump` file exists in the home dir, load roots from it (`genesis_state_from_dump`, :82); records in the config are then ignored with a warning.
3. Else `genesis_state_from_genesis` (:95) computes roots: for `StateRoots` contents it returns them directly; otherwise it picks the runtime config via `RuntimeConfigStore::for_chain_id(chain_id).get_config(protocol_version)` (:119-120) to obtain `storage_usage_config`, distributes each record to a shard via `state_record_to_shard_id` over the genesis `shard_layout` (:133), then applies records into per-shard tries and commits, producing one `StateRoot` per shard.
4. `GenesisStateApplier::apply` (`core/store/src/genesis/state_applier.rs:345`) writes each record with the matching setter (`set_account`, `set_access_key_by_handle`, contract code, `Data`, `GasKeyNonce`, etc.) into an `AutoFlushingTrieUpdate` whose `flush` periodically commits and folds `trie_changes` into the running `state_root` — `state_applier.rs:140`.
5. The computed roots are persisted (`set_genesis_state_roots`, `set_genesis_height`) and, for mainnet/testnet, hard-asserted against pinned constants — `initialization.rs:70` asserts mainnet `[8EhZRfDTYujfZoUZtZ3eSMB9gJyFo5zjscR12dEcaxGU]` and `:74` testnet `[7EAgMRCrBWcb3ZS6SZJ7Dm71VZ1jaBpgGiewAEvFqPT1]`.

The `Genesis::json_hash` (a SHA-256 over pretty-serialized config + records via `GenesisJsonHasher`) is a separate identity used for the genesis hash; for `StateRoots` contents it hashes the roots and is explicitly noted as **incorrect**/testing-only — `genesis_config.rs:526` — `GenesisJsonHasher::process_state_roots`.

### 4. Building the versioned runtime config

`RuntimeConfigStore::new(genesis_runtime_config)` — `core/parameters/src/config_store.rs:88`:
1. Parse `BASE_CONFIG` (`res/runtime_configs/parameters.yaml`) into a `ParameterTable`; materialize it as the config for version 0 (`config_store.rs:88-101`).
2. For each `(version, diff)` in `CONFIG_DIFFS` (`config_store.rs:26-66`, versions 46…85 plus nightly 129/155), apply the YAML diff onto the running table **in ascending version order** and snapshot the resulting `RuntimeConfig` under that version (:117-153). `apply_diff` verifies each edit's declared `old` value matches the current table, erroring with `WrongOldValue`/`NoOldValueExists`/`OldValueExists` otherwise — `parameter_table.rs:526` — `ParameterTable::apply_diff`.
3. `RuntimeConfig::try_from(&ParameterTable)` assembles every sub-config by reading typed parameters — `parameter_table.rs:409`. Notably `wasm_config.limit_config` is built by re-serializing the vm-limit parameters to YAML (`parameter_table.rs:457`), and flags like `eth_implicit_global_contract`, `gas_key_host_fns`, `use_state_stored_receipt`, `account_creation_charge`, `min_gas_purchase_price` are read directly from parameters (:465, :467, :494-:496) — i.e. version-gated *behavior* is baked into the config values, not read from `ProtocolFeature` at execution time.
4. `for_chain_id` overrides version-0 for testnet with `INITIAL_TESTNET_CONFIG` (historically divergent), and builds special configs for benchmarknet (disables congestion/witness/bandwidth limits) and congestion-control-test — `config_store.rs:170`.

`get_config(version)` floors to the greatest stored key `<= version` (`config_store.rs:240`). Config selection semantics are owned by [protocol-versioning](protocol-versioning.md).

### 5. Assembling `NearConfig` from node config

`NearConfig::new(config, genesis, ...)` — `nearcore/src/config.rs:718` — folds `Config` + `Genesis` into a `ClientConfig`. Derived/inferred fields:
- `is_archive_or_rpc = config.archive || tracked_shards_config().is_rpc()` (:724).
- `save_trie_changes = config.save_trie_changes.unwrap_or(!config.archive)` (:787) — non-archival nodes save trie changes for gc.
- `save_tx_outcomes = config.save_tx_outcomes.unwrap_or(is_archive_or_rpc)` (:788); `save_receipt_to_tx` defaults to the same (:789).
- `epoch_length` and `num_block_producer_seats` are copied from **genesis**, not node config (:773-:774).
- `tracked_shards_config()` resolves the new field or falls back to the deprecated `tracked_shards`/`tracked_shard_schedule`/`tracked_shadow_validator`/`tracked_accounts` in that priority order, where any non-empty `tracked_shards` means AllShards — `config.rs:659` / `client_config.rs:103` — `TrackedShardsConfig::from_deprecated_config_values`. `is_rpc()` is true for every variant except `NoShards`/`ShadowValidator` (`client_config.rs:93`).
- `state_sync_config()` derives a dump config from cloud-archival settings when a cloud archival writer is configured, else uses `config.state_sync` or default — `config.rs:682`.

### 6. Node-config validation and dynamic reload

- `validate_config` collects errors over many conditions: `min_block_production_delay <= max_block_production_delay` and `<= max_block_wait_delay`; all three gc values `> 0`; `cold_store` requires `save_trie_changes == Some(true)`; `archive=false` forbids `save_trie_changes=false`, `cold_store`, `split_storage`, and `cloud_archival`; cloud-archival-writer shard-tracking constraints; and mutual exclusion of `tracked_shards` and `tracked_shards_config` — `nearcore/src/config_validate.rs:10` / `:33` — `ConfigValidator::validate_all_conditions` / `validate_tracked_shards_config` (:323).
- A subset of client config is **reloadable without restart**: `read_updatable_configs` re-reads `config.json`, log config, and validator key; `get_updatable_client_config` whitelists exactly `expected_shutdown`, `resharding_config`, `produce_chunk_add_transactions_time_limit`, and several consensus timing knobs — `nearcore/src/dyn_config.rs:62` — `get_updatable_client_config`. Everything else requires a restart.

### 7. Chain differences (mainnet / testnet / localnet)

- `GenesisConfig::use_production_config()` is true for `chain_id == mainnet | testnet` (or an explicit `use_production_config` flag), routing to the hardcoded production epoch-config overrides — `genesis_config.rs:236`.
- Runtime-config genesis (version-0) differs only for testnet, which is patched with `INITIAL_TESTNET_CONFIG` for historical compatibility — `config_store.rs:170`.
- Mainnet/testnet genesis state roots are hard-asserted at init (Behavior §3.5); localnet/other chains compute freely.
- Mainnet/testnet genesis protocol version is `PROD_GENESIS_PROTOCOL_VERSION = 29` (`core/primitives-core/src/version.rs:597`); the protocol upgrade vote schedule is chain-specific and lives in [protocol-versioning](protocol-versioning.md).

## Interactions

- **Produces**: genesis `GenesisConfig` + per-shard genesis `StateRoot`s consumed by [chain-block-processing](chain-block-processing.md) and [sharding-chunks](sharding-chunks.md); per-version `RuntimeConfig` consumed by [runtime-execution](runtime-execution.md), [contract-vm](contract-vm.md) (`wasm_config`), [economics](economics.md) (fees/inflation), [cross-shard-congestion](cross-shard-congestion.md) (`congestion_control_config`, `bandwidth_scheduler_config`), and [stateless-validation](stateless-validation.md) (`witness_config`).
- **Consumes**: `ProtocolVersion` and `ProtocolFeature` activation from [protocol-versioning](protocol-versioning.md) (config diffs are keyed by the same version numbers).
- **Node config edges**: `tracked_shards_config` and `gc` drive which shards/history a node keeps, feeding [sync](sync.md) and gc in [chain-block-processing](chain-block-processing.md); `state_sync` selects the state-sync source (see [sync](sync.md)).
- The shard-distribution of genesis records uses the shard layout from the genesis-derived `EpochConfig` — see [epoch-validators-staking](epoch-validators-staking.md) and [sharding-chunks](sharding-chunks.md).

## Protocol-version-gated behavior

Genesis config is *set once* at genesis and is not itself version-gated; what evolves per version is the **runtime config**, expressed as parameter diffs rather than `if feature.enabled()` branches. The relevant features (verified against `core/primitives-core/src/version.rs` in this 2.13.0 tree):

| Feature | Activation version | Effect on config |
| --- | --- | --- |
| `ExcludeExistingCodeFromWitnessForCodeLen` | 83 (`version.rs:550`) | active named v83 feature; witness code-size handling |
| `InvalidTxGenerateOutcomes` | 83 (`version.rs:549`) | active named v83 feature |
| `FixAccessKeyAllowanceCharging` | 83 (`version.rs:551`) | active named v83 feature |
| `IncludeDeployGlobalContractOutcomeBurntStorage` | 83 (`version.rs:552`) | active named v83 feature |
| `GlobalContractDistributionNonce` | 83 (`version.rs:553`) | active named v83 feature |
| `EthImplicitGlobalContract` | 83 (`version.rs:556`) | drives `wasm_config.eth_implicit_global_contract` (`parameter_table.rs:465`); corresponds to `83.yaml` diff |
| `Wasmtime` | 84 (`version.rs:558`) | VM kind selection; `84.yaml` |
| `EnforcePerReceiptStorageProofLimit` | **86** (`version.rs:576`) | the PV-86 feature on this release |

There is **no** `FixContractLoadingError` variant on 2.13.0; `FixContractLoadingCost` exists but is a *nightly* feature at version 129 (`version.rs:579`), unreachable on a stable binary capped at 86. Config-diff files exist for every version where a parameter changed (`config_store.rs:26-66`: …82, 83, 84, 85, then nightly 129, 155). Consequently v85 and v86 share the v85 runtime config (there is no `85`→`86` diff), because `get_config(86)` floors to key 85. `MIN_SUPPORTED_PROTOCOL_VERSION = 83` (`version.rs:600`) bounds the oldest config a running binary produces state under. `STABLE_PROTOCOL_VERSION = 86` (`version.rs:628`) is what a stable binary votes for; the mainnet vote for PV 86 is scheduled for 2026-07-20 (`core/primitives/src/version.rs:60-64`).

## Invariants & failure modes

- **Total supply consistency**: `sum(amount + locked)` over `Account` records must equal `genesis_config.total_supply`; else `GenesisSemanticsError` "wrong total supply." — `genesis_validate.rs:114` — `validate_processed_records`.
- **Validator/stake consistency**: staked accounts (`locked > 0`) must exactly equal the validator set; keys must be valid staking keys; set non-empty — `genesis_validate.rs:85` (same fn).
- **Referential integrity**: every access-key/contract account must have an `Account` record — `genesis_validate.rs:127` / `:134`.
- **No duplicate accounts / no double contract deploy** per genesis records — `genesis_validate.rs:53` — `GenesisValidator::process_record`.
- **Threshold bounds**: `epoch_length > 0`, `online_min < online_max <= 1`, `gas_price_adjustment_rate < 1`, numerator/denominator `< 10_000_000` — `genesis_validate.rs:141-192` (same fn).
- **Mainnet/testnet genesis roots are frozen**: any drift from the pinned state-root constants panics at startup — `core/store/src/genesis/initialization.rs:70` / `:74`.
- **Genesis height agreement**: stored genesis height must equal config's, else assert panic — `initialization.rs:46`.
- **Config-diff soundness**: a diff whose declared `old` value disagrees with the accumulated table fails store construction (panics at startup) with `WrongOldValue`/`NoOldValueExists`/`OldValueExists` — `parameter_table.rs:526` / `config_store.rs:117-129`.
- **Malformed genesis/params panic rather than error**: `GenesisConfig::from_json` panics (`genesis_config.rs:352`); `RuntimeConfigStore::new` panics on unparseable base/diff files (`config_store.rs:90`, `:118`, `:124`).
- **Node-config validation** returns `ValidationError` (does not panic) and aggregates all failures — `config_validate.rs:33`.
- **StateRoots genesis cannot recompute a consistent genesis hash** — testing-only; documented invariant at `genesis_config.rs:314` and `:526`.

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `core/chain-configs/src/genesis_config.rs:110` | `GenesisConfig` | network-wide genesis parameters struct |
| `core/chain-configs/src/genesis_config.rs:304` | `GenesisContents` | Records / RecordsFile / StateRoots variants |
| `core/chain-configs/src/genesis_config.rs:334` | `Genesis` | config + contents wrapper |
| `core/chain-configs/src/genesis_config.rs:352` | `GenesisConfig::from_json` | parse (panics on error) |
| `core/chain-configs/src/genesis_config.rs:574` | `Genesis::from_file` | load + validate genesis |
| `core/chain-configs/src/genesis_config.rs:655` | `Genesis::validate` | Full vs UnsafeFast dispatch |
| `core/chain-configs/src/genesis_config.rs:687` | `Genesis::for_each_record` | stream/iterate records |
| `core/chain-configs/src/genesis_config.rs:462` | `RecordsProcessor::visit_map` | streaming record deserializer |
| `core/chain-configs/src/genesis_config.rs:236` | `GenesisConfig::use_production_config` | mainnet/testnet production path |
| `core/chain-configs/src/genesis_config.rs:526` | `GenesisJsonHasher::process_state_roots` | StateRoots genesis-hash caveat |
| `core/chain-configs/src/genesis_validate.rs:85` | `GenesisValidator::validate_processed_records` | semantic genesis invariants |
| `core/store/src/genesis/initialization.rs:29` | `initialize_sharded_genesis_state` | genesis state-root init entry |
| `core/store/src/genesis/initialization.rs:70` | (mainnet/testnet asserts) | frozen genesis roots |
| `core/store/src/genesis/state_applier.rs:345` | `GenesisStateApplier::apply` | records → trie → state root |
| `core/parameters/src/config.rs:17` | `RuntimeConfig` | materialized per-version runtime params |
| `core/parameters/src/config_store.rs:88` | `RuntimeConfigStore::new` | build versioned config map from base + diffs |
| `core/parameters/src/config_store.rs:170` | `RuntimeConfigStore::for_chain_id` | chain-specific overrides (testnet/benchmarknet) |
| `core/parameters/src/config_store.rs:240` | `RuntimeConfigStore::get_config` | floor lookup by version |
| `core/parameters/src/config_store.rs:26` | `CONFIG_DIFFS` | ordered (version, diff-file) list |
| `core/parameters/src/parameter_table.rs:409` | `TryFrom<&ParameterTable> for RuntimeConfig` | assemble config from typed params |
| `core/parameters/src/parameter_table.rs:526` | `ParameterTable::apply_diff` | validated diff application |
| `nearcore/src/config.rs:227` | `Config` | node `config.json` struct |
| `nearcore/src/config.rs:659` | `Config::tracked_shards_config` | resolve tracked-shards (new + deprecated) |
| `nearcore/src/config.rs:682` | `Config::state_sync_config` | derive state-sync source |
| `nearcore/src/config.rs:718` | `NearConfig::new` | assemble ClientConfig with inferred fields |
| `nearcore/src/config_validate.rs:33` | `ConfigValidator::validate_all_conditions` | node-config validation |
| `nearcore/src/dyn_config.rs:62` | `get_updatable_client_config` | whitelist of hot-reloadable fields |
| `core/chain-configs/src/client_config.rs:49` | `TrackedShardsConfig` | shard-tracking variants |
| `core/chain-configs/src/client_config.rs:135` | `GCConfig` | gc knobs + defaults |
| `core/primitives-core/src/version.rs:576` | `ProtocolFeature::EnforcePerReceiptStorageProofLimit` | PV-86 feature (this release) |
| `core/primitives-core/src/version.rs:597` | `PROD_GENESIS_PROTOCOL_VERSION` | =29 mainnet/testnet genesis version |

## Open questions

- The nomicon page `docs/GenesisConfig/GenesisConfig.md` documents genesis fields but is field-oriented and does not reflect all defaulted 2.13.0 fields (e.g. `num_chunk_producer_seats`, `chunk_producer_assignment_changes_limit`); treat the struct at `genesis_config.rs:110` as authoritative. Not broken, only incomplete.
- The base runtime parameter values and each per-version diff live in `core/parameters/res/runtime_configs/*.yaml`; this spec describes the *mechanism* that consumes them but does not enumerate individual parameter values (owned by the consuming components).
