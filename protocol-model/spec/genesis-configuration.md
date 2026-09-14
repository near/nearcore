# Genesis & node configuration

> Protocol version: 87 (stable) · Release: 2.14.0-rc.1 · Derived from commit: 233252e · Generated: 2026-09-14
> Primary crates/files: `core/chain-configs/src/genesis_config.rs`, `core/chain-configs/src/genesis_validate.rs`, `core/chain-configs/src/client_config.rs`, `core/parameters/src/config.rs`, `core/parameters/src/config_store.rs`, `core/parameters/src/parameter_table.rs`, `nearcore/src/config.rs`, `nearcore/src/config_validate.rs`, `nearcore/src/dyn_config.rs`, `nearcore/src/lib.rs`, `neard/src/cli.rs`, `core/store/src/genesis/initialization.rs`, `core/store/src/archive/cloud_storage/config.rs`, `chain/network/src/config_json.rs`, `chain/network/src/config.rs`

## Role

This component bootstraps a NEAR network's and a node's initial conditions. It defines three separable configuration surfaces: (1) the **genesis config** — the immutable, network-wide chain parameters (chain id, genesis height/time, initial protocol version, initial validators, shard layout, economics) plus the initial state records that are hashed into the genesis state root; (2) the **runtime config** — the versioned schema of fees, wasm limits, account-creation, congestion, witness, and bandwidth parameters that feeds [runtime-execution](runtime-execution.md), [contract-vm](contract-vm.md), and [economics](economics.md), selected per protocol version by machinery documented in [protocol-versioning](protocol-versioning.md); and (3) the **node config** (`config.json`) — per-node operational knobs (tracked shards, archival, gc, state-sync source, network limits) that have no consensus effect except through what state the node keeps and serves. Genesis feeds the genesis block/state root consumed by [chain-block-processing](chain-block-processing.md); the runtime config feeds every execution component; the node config's tracked-shards/gc choices interact with [sync](sync.md).

## Key data structures

- **`GenesisConfig`** — `core/chain-configs/src/genesis_config.rs:110` — the network-wide, consensus-critical parameters. Fields include `protocol_version` (:112, the version genesis runs at), `genesis_time` (:115), `chain_id` (:118), `genesis_height` (:120), `num_block_producer_seats` (:122), `dynamic_resharding` (:124), `protocol_upgrade_stake_threshold` (:129, default 8/10), `epoch_length` (:131), `gas_limit` (:133), `min_gas_price`/`max_gas_price` (:135/:137), kickout thresholds (:139/:141/:144), `target_validator_mandates_per_shard` (:148), online thresholds (:153/:158), `validators: Vec<AccountInfo>` (:164), `total_supply` (:176), `protocol_treasury_account` (:181), `shard_layout` (:191), `minimum_stake_ratio` (:205), `num_chunk_producer_seats` (:223), `num_chunk_validator_seats` (:226). It derives `SmartDefault`; many fields carry `#[serde(default = "...")]` so older/newer JSON round-trips. Not versioned as an enum — it is a flat struct evolved by adding defaulted fields.
- **`Genesis`** — `core/chain-configs/src/genesis_config.rs:334` — `{ config: GenesisConfig (#[serde(flatten)]), contents: GenesisContents }`. The invariant that `total_supply` equals the supply implied by records is documented but enforced only at validation time (:329).
- **`GenesisContents`** — `core/chain-configs/src/genesis_config.rs:304` — an `#[serde(untagged)]` enum: `Records { records }` (inline), `RecordsFile { records_file }` (streamed from a side file to bound memory), or `StateRoots { state_roots }` (testing/mock-fork/forknet only; cannot recompute a consistent genesis hash — :314).
- **`GenesisRecords`** — `core/chain-configs/src/genesis_config.rs:284` — `Vec<StateRecord>`; the initial accounts, access keys, contracts and data that become the genesis trie. See [state-storage](state-storage.md) for `StateRecord` semantics.
- **`ProtocolConfigView` / `ProtocolConfig`** — `core/chain-configs/src/genesis_config.rs:793` / `:870` — the RPC-facing projection combining `GenesisConfig` fields with a `RuntimeConfigView`; assembled by `From<ProtocolConfig> for ProtocolConfigView` (:875).
- **`RuntimeConfig`** — `core/parameters/src/config.rs:17` — the fully-materialized per-version runtime parameters: `fees: Arc<RuntimeFeesConfig>` (:22), `wasm_config: Arc<vm::Config>` (:27), `account_creation_config` (:29), `congestion_control_config` (:31), `witness_config` (:33), `bandwidth_scheduler_config` (:35), plus `use_state_stored_receipt` (:38), `min_gas_purchase_price` (:42), `account_creation_charge` (:46). Sub-structs `AccountCreationConfig` (:116), `CongestionControlConfig` (:135), `WitnessConfig` (:261), `BandwidthSchedulerConfig` (:288).
- **`RuntimeConfigStore`** — `core/parameters/src/config_store.rs:70` — `BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>`. Lookup floors to the greatest key `<=` requested version (`get_config`, :237). See [protocol-versioning](protocol-versioning.md).
- **`ParameterTable` / `ParameterTableDiff`** — `core/parameters/src/parameter_table.rs:346` / `:362` — `BTreeMap<Parameter, ParameterValue>` and a keyed `(old, new)` diff set.
- **`Config`** (node/`config.json`) — `nearcore/src/config.rs:226` — the per-node struct: `genesis_file` (:227), key files, `network`, `consensus` (:243), `tracked_shards_config` (:247) with deprecated fallbacks (:250-:262), `archive` (:266), `cloud_archival: Option<CloudArchivalConfig>` (:269), `save_trie_changes`/`save_tx_outcomes`/`save_state_changes` (:278/:287/:316), `view_access_keys_limit` (:346), `gc: GCConfig` (:329, `#[serde(flatten)]`), `store`/`cold_store`/`split_storage` (:351/:354/:357), `state_sync` (:364).
- **`Consensus`** — `nearcore/src/config.rs:119` — consensus/sync timing knobs, including the renamed `block_request_timeout` (`#[serde(alias = "state_sync_external_timeout")]`).
- **`NearConfig`** — `nearcore/src/config.rs:694` — the assembled runtime bundle: `config`, `client_config: ClientConfig`, `network_config`, `genesis`, `validator_signer`.
- **`TrackedShardsConfig`** — `core/chain-configs/src/client_config.rs:46` — `NoShards | Shards(Vec<ShardUId>) | AllShards | ShadowValidator(AccountId) | Schedule(Vec<Vec<ShardId>>) | Accounts(Vec<AccountId>)`.
- **`GCConfig`** — `core/chain-configs/src/client_config.rs:132` — `gc_blocks_limit` (default 2), `gc_fork_clean_step` (100), `gc_num_epochs_to_keep` (default `DEFAULT_GC_NUM_EPOCHS_TO_KEEP = 5`, floored to `MIN_GC_NUM_EPOCHS_TO_KEEP = 3` by the accessor at :165), `gc_step_period` (500 ms) — `client_config.rs:150-161`, `:33`, `:36`.
- **`SyncConfig`** — `core/chain-configs/src/client_config.rs:328` — now a **single-variant** enum (`Peers`). The `ExternalStorage(ExternalStorageConfig)` variant and the `ExternalStorageConfig` struct were deleted this release.
- **`CloudArchivalConfig`** — `core/store/src/archive/cloud_storage/config.rs:12` — `{ location, credentials_file, writer: Option<CloudArchivalWriterConfig>, reader: Option<CloudArchivalReaderConfig> }`; `into_default_dump_config` (:28) derives the state-dump config for a writer node. `CloudArchivalWriterConfig` — `core/chain-configs/src/client_config.rs:233` (`archive_block_data`, `polling_interval`, `catch_up_throttle`, `snapshot_every_n_epochs`); `CloudArchivalReaderConfig` — `client_config.rs:214` (`polling_interval`).

## Behavior

### 1. Loading and parsing genesis

1. `Genesis::from_file` reads the JSON, strips `//`/`/* */` comments, and deserializes into `Genesis`, then runs `validate` — `core/chain-configs/src/genesis_config.rs:574` — `Genesis::from_file`. `from_files` (:610) parses config and a separate records file.
2. `GenesisConfig` itself parses via `from_json`/`from_file`; `from_json` **panics** (does not return `Result`) on malformed JSON — `genesis_config.rs:352` — `GenesisConfig::from_json`.
3. Records may be streamed rather than fully loaded: `for_each_record` dispatches on `GenesisContents`, iterating the in-memory `Vec` or streaming a records file via `stream_records_from_file` — `genesis_config.rs:687` — `Genesis::for_each_record`. The streaming deserializer reads only the `records` field and ignores all others, erroring on a missing or duplicate `records` field — `genesis_config.rs:462` — `RecordsProcessor::visit_map`.

### 2. Genesis validation (`GenesisValidationMode::Full`)

`validate` routes `Full` to `validate_genesis`, while `UnsafeFast` logs "skipped genesis validation" and returns Ok — `core/chain-configs/src/genesis_config.rs:655` — `Genesis::validate`. `validate_genesis` short-circuits to Ok for `StateRoots` contents (records cannot be validated) — `genesis_validate.rs:10` — `validate_genesis`. Otherwise it streams every record through `GenesisValidator::process_record` (`genesis_validate.rs:57`) accumulating totals, then `validate_processed_records` (`genesis_validate.rs:101`) enforces, among others:
- every validator's public key `is_valid_staking_key`, no duplicate validators, non-empty validator set;
- `sum(account.amount + account.locked)` over records equals `genesis_config.total_supply` (else "wrong total supply.") — `genesis_validate.rs:132`;
- the set of accounts with `locked > 0` matches the validator set;
- every access-key/contract account exists as an `Account` record — `genesis_validate.rs:143` / `:150`;
- `online_min < online_max <= 1` (:178), numerator/denominator bounds `< 10_000_000` (:194-:212), `gas_price_adjustment_rate < 1` (:218), `epoch_length > 0` (:226).

**New in 2.14 — uninitialized accounts are accepted (#16398).** `process_record` no longer rejects an `Account` record whose `Account::is_initialized()` is false (`core/primitives-core/src/account.rs:321`); such records are collected into `uninitialized_account_ids` — `genesis_validate.rs:68`. These are the `Uninitialized` representation used by universal accounts (see [runtime-execution](runtime-execution.md) for `UniversalStateInit`); they cannot occur in a hand-written production genesis but do occur in a genesis produced by a state dump of a running chain. The validator instead enforces that such an account carries **no state of its own**, because a `Contract` record for one trips a code-hash assertion inside the genesis state applier — `genesis_validate.rs:157-176`:

| Condition | Error |
| --- | --- |
| uninitialized account has an access key | "uninitialized account {id} must not have an access key" |
| uninitialized account has a `Contract` record | "uninitialized account {id} must not have code" |
| uninitialized account has a `Data` or `GasKeyNonce` record | "uninitialized account {id} must not have data" |

`Data`/`GasKeyNonce` records are now tracked in `data_account_ids`; `PostponedReceipt`/`ReceivedData`/`DelayedReceipt` are explicitly ignored ("receipts in flight are not part of an account's own state") — `genesis_validate.rs:91-97`.

All violations are collected (not fail-fast) and returned as one `ValidationError::GenesisSemanticsError` — `genesis_validate.rs:232` — `GenesisValidator::result_with_full_error`.

### 3. Resolving the genesis shard layout

`initialize_sharded_genesis_state` now takes a `&ShardLayout` directly rather than an `&EpochConfig` — `core/store/src/genesis/initialization.rs:29`. Two callers resolve it differently, and the difference is the point of change #16148 (**dynamic shard layout allowed in genesis**):

1. The running node takes the layout the `EpochManager` already recorded in the genesis `EpochInfo`, so the two can never disagree — `nearcore/src/lib.rs:420` — `epoch_manager.get_shard_layout(&EpochId::default())`.
2. The standalone `initialize_genesis_state` helper resolves it the same way the `EpochManager` would: the epoch config's static layout if it declares one, else the layout declared in `genesis.config.shard_layout` — `initialization.rs:109` — `initialize_genesis_state`.

The epoch manager side mirrors this: `AllEpochConfig` now carries `genesis_shard_layout` alongside `genesis_protocol_version` (`core/primitives/src/epoch_manager.rs:322`), populated from `genesis_config.shard_layout` (`chain/epoch-manager/src/lib.rs:558` — `AllEpochConfig::from_epoch_config_store`), and `AllEpochConfig::genesis_shard_layout()` (`epoch_manager.rs:364`) returns the epoch config's `static_shard_layout()` when there is one and otherwise falls back to the genesis-declared layout. When the genesis protocol version has `ShardLayoutConfig::Dynamic` (i.e. `ProtocolFeature::DynamicResharding` is enabled at that version — it activates at **85**, `core/primitives-core/src/version.rs:606`), the epoch config declares no layout and the genesis one is authoritative; the shipped mainnet/testnet epoch configs from 85 on carry `dynamic_resharding_config` and no `shard_layout` key (`core/primitives/res/epoch_configs/{mainnet,testnet}/85.json`), so for any genesis created at protocol version >= 85 `genesis.config.shard_layout` is the source of truth; when it is **not** enabled and the epoch config carries no static layout, genesis epoch construction fails with `EpochError::ShardingError` "static shard layout expected for genesis when dynamic resharding is disabled" — `chain/epoch-manager/src/genesis.rs:68`.

`EpochConfig::from(&GenesisConfig)` (`genesis_config.rs:243`) still projects genesis fields onto a static-layout epoch config for the non-EpochManager path. Deprecated `EpochConfig` fields `num_block_producer_seats_per_shard`, `avg_hidden_validator_seats_per_shard` and `num_chunk_only_producer_seats` were **removed** this release (#16098). An epoch-config JSON that still carries those keys keeps loading: `EpochConfig` has no `#[serde(deny_unknown_fields)]`, and the keys fall through the `#[serde(flatten)] shard_layout_config` buffer into the untagged `ShardLayoutConfig`, whose struct variants ignore unknown keys — `core/primitives/src/epoch_manager.rs:100-151`, `:63-72`.

### 4. Producing the genesis state root

At node startup `initialize_sharded_genesis_state` is the entry point — `core/store/src/genesis/initialization.rs:29`. Order:
1. If the store already holds genesis state roots, cross-check them against the layout and reuse them; it also re-asserts the stored genesis height equals the config's (`initialization.rs:47`).
2. Else, if a `state_dump` file exists in the home dir, load roots from it (`genesis_state_from_dump`, :116); records in the config are then ignored with a warning.
3. Else `genesis_state_from_genesis` (:129) computes roots: for `StateRoots` contents it returns them directly; otherwise it picks the runtime config via `RuntimeConfigStore::for_chain_id(chain_id).get_config(protocol_version)` (:153-154) to obtain `storage_usage_config`, distributes each record to a shard via `state_record_to_shard_id` over the resolved `shard_layout` (:167), then applies records into per-shard tries and commits, producing one `StateRoot` per shard.
4. **Before** anything is committed, `check_state_roots_match_layout` asserts `state_roots.len() == shard_layout.num_shards()` — `initialization.rs:92`. This is checked on every path (already-stored, state dump, computed), because `genesis_chunks` otherwise silently replicates a single root across every shard and yields a chain that starts and is wrong. Doing it before `set_genesis_state_roots` keeps a failed attempt from leaving a half-initialized DB (`initialization.rs:62-68`). The guard exists because `genesis.config.shard_layout` is `#[serde(default)]` and a genesis file omitting it silently claims a single shard.
5. `GenesisStateApplier::apply` (`core/store/src/genesis/state_applier.rs:345`) writes each record with the matching setter (`set_account`, `set_access_key_by_handle`, contract code, `Data`, `set_gas_key_nonce_by_handle`, etc.) into an `AutoFlushingTrieUpdate` (:100) whose `flush` periodically commits and folds `trie_changes` into the running `state_root` — `state_applier.rs:140`.
6. The computed roots are persisted (`set_genesis_state_roots`, `set_genesis_height`) and, for mainnet/testnet, hard-asserted against pinned constants — `initialization.rs:75` asserts mainnet `[8EhZRfDTYujfZoUZtZ3eSMB9gJyFo5zjscR12dEcaxGU]` and `:79` testnet `[7EAgMRCrBWcb3ZS6SZJ7Dm71VZ1jaBpgGiewAEvFqPT1]`.

`Genesis::json_hash` (a SHA-256 over pretty-serialized config + records via `GenesisJsonHasher`) is a separate identity used for the genesis hash; for `StateRoots` contents it hashes the roots and is explicitly noted as **incorrect**/testing-only — `genesis_config.rs:526` — `GenesisJsonHasher::process_state_roots`.

### 5. Building the versioned runtime config

`RuntimeConfigStore::new(genesis_runtime_config)` — `core/parameters/src/config_store.rs:85`:
1. Parse `BASE_CONFIG` (`res/runtime_configs/parameters.yaml`, `config_store.rs:22`) into a `ParameterTable`; materialize it as the config for version 0 (`config_store.rs:86-98`).
2. For each `(version, diff)` in `CONFIG_DIFFS` (`config_store.rs:26-63`) apply the YAML diff onto the running table **in ascending version order** and snapshot the resulting `RuntimeConfig` under that version (:114-150). `apply_diff` verifies each edit's declared `old` value matches the current table, erroring with `WrongOldValue`/`NoOldValueExists`/`OldValueExists` otherwise — `parameter_table.rs:530` — `ParameterTable::apply_diff`.
3. `RuntimeConfig::new(&ParameterTable)` (`core/parameters/src/config.rs:50`, delegating to `TryFrom<&ParameterTable>`, `parameter_table.rs:409`) assembles every sub-config by reading typed parameters. Notably `wasm_config.limit_config` is built by re-serializing the vm-limit parameters to YAML (`parameter_table.rs:457`), and flags like `universal_accounts` (:466), `gas_key_host_fns` (:468), `fix_contract_loading_error` (:460), `fix_ml_dsa_cost_charging` (:469), `sha3_host_fns` (:473), `use_state_stored_receipt` (:498), `min_gas_purchase_price` (:499) and `account_creation_charge` (:500) are read directly from parameters — i.e. version-gated *behavior* is baked into the config values, not read from `ProtocolFeature` at execution time.
4. `for_chain_id` overrides version 0 for testnet with `RuntimeConfig::initial_testnet_config()` (from `INITIAL_TESTNET_CONFIG`, `config_store.rs:66`), and builds special configs for benchmarknet (disables congestion/witness/bandwidth limits) and congestion-control-test — `config_store.rs:167`.

`get_config(version)` floors to the greatest stored key `<= version` (`config_store.rs:237`). Config selection semantics are owned by [protocol-versioning](protocol-versioning.md).

**Diff-set changes this release.** The `46`, `48`, `49`, `50` and `52` diff files were deleted and their values folded into the base `parameters.yaml` (#15952, #15993, #16245) — e.g. `wasm_regular_op_cost: 822_756` (`parameters.yaml:175`), `max_gas_burnt: 300_000_000_000_000` (:291), `max_functions_number_per_contract: 10_000` (:318) are now base values. Because `get_config` floors and those diffs contained no parameter that any later diff re-declares with an `old` guard, the materialized config for every version `>= 53` is unchanged; the configs materialized *for* versions 46–52 now equal the (updated) base config. `eth_implicit_global_contract` was dropped from `83.yaml` and from the `Parameter` enum entirely — the behavior is unconditional now that `MIN_SUPPORTED_PROTOCOL_VERSION` is past it. New diffs: `87.yaml` (stable) and `157.yaml` (nightly).

### 6. Assembling `NearConfig` from node config

`NearConfig::new(config, genesis, ...)` — `nearcore/src/config.rs:715` — folds `Config` + `Genesis` into a `ClientConfig`. Derived/inferred fields:
- `is_archive_or_rpc = config.archive || tracked_shards_config().is_rpc()` (:721).
- `save_trie_changes = config.save_trie_changes.unwrap_or(!config.archive)` (:786) — non-archival nodes save trie changes for gc.
- `save_tx_outcomes = config.save_tx_outcomes.unwrap_or(is_archive_or_rpc)` (:787); `save_receipt_to_tx` defaults to the same (:788).
- `save_state_changes = config.save_state_changes.unwrap_or(true)` (:796) and `save_untracked_partial_chunks_parts` likewise (:797) — unlike `save_trie_changes`/`save_tx_outcomes` these do not depend on `archive`.
- `epoch_length` and `num_block_producer_seats` are copied from **genesis**, not node config (:769-:770).
- `cloud_archival_writer` is now read out of the nested `cloud_archival.writer` (:782-:785), not a top-level `cloud_archival_writer` field.
- `view_access_keys_limit = config.view_access_keys_limit.unwrap_or(default_view_access_keys_limit())` = 100 (:810, `client_config.rs:592`).
- `tracked_shards_config()` resolves the new field or falls back to the deprecated `tracked_shards`/`tracked_shard_schedule`/`tracked_shadow_validator`/`tracked_accounts` in that priority order, where any non-empty `tracked_shards` means AllShards — `config.rs:658` / `client_config.rs:100` — `TrackedShardsConfig::from_deprecated_config_values`. `is_rpc()` is true for every variant except `NoShards`/`ShadowValidator` (`client_config.rs:90`).
- `state_sync_config()` derives a dump config from `cloud_archival.into_default_dump_config()` when a cloud-archival **writer** is configured, else uses `config.state_sync` or default — `config.rs:681`.

### 7. Node-config validation and dynamic reload

`validate_config` aggregates all failures into one `ValidationError::ConfigSemanticsError` (it does not panic) — `nearcore/src/config_validate.rs:15` / `:38` — `validate_config` / `ConfigValidator::validate_all_conditions`. Checks, in call order:

1. `validate_cloud_archival_config` (:264) — `cloud_archival` must set **exactly one** of `writer`/`reader` ("`cloud_archival` sets both `writer` and `reader`; a node is one or the other." / "`cloud_archival` sets neither `writer` nor `reader`; one is required."); `state_sync` must not also be set; the location must be supported; `archive` must be true. For a writer: it must track a non-empty subset of shards unless `archive_block_data`; `snapshot_every_n_epochs > 0`; and when it archives shards, `save_tx_outcomes` (:322), `save_state_changes` (:332, new this release) and the resolved `save_receipt_to_tx` (:340-:348) must not be explicitly `false`. All three are `Option<bool>` and the validator resolves each to `true` when omitted, so the checks only fire on an explicit `false`. (The validator resolves `save_receipt_to_tx` locally as `save_receipt_to_tx.or(save_tx_outcomes).unwrap_or(true)`, while `NearConfig::new` uses `is_archive_or_rpc` as the fallback — they agree here because `cloud_archival` already requires `archive == true`.)
2. `validate_cold_store_config` (:235) — `cold_store` requires `save_trie_changes == Some(true)`; `archive == false` forbids `save_trie_changes == Some(false)`, `cold_store` and `split_storage`.
3. `validate_state_sync_config` (:219) — only the dump config and `parts_compression_lvl ∈ [-22, 22]` remain; every `ExternalStorage`-location check was deleted along with the variant.
4. `validate_tracked_shards_config` (:352) — `tracked_shards_config` is mutually exclusive with each deprecated `tracked_*` field.
5. Consensus timing: `min_block_production_delay <= max_block_production_delay` and `<= max_block_wait_delay`; `header_sync_expected_height_per_second != 0`.
6. `validate_gc_config` (:91) — see below.
7. `tx_routing_height_horizon ∈ [2, 100]`.

Separately, `load_config` rejects a node that is **both a validator and a cloud-archival writer** (a validator key file is present *and* `cloud_archival.writer` is set): "a cloud archival writer must not produce chunks: it would archive its own chunk rows under an inclusion height the chain has not given them yet. Remove the validator key file, or unset cloud_archival.writer." — pushed as a `push_cross_file_semantics_error`, `nearcore/src/config.rs:1748-1759`.

**GC rate validation (new, #16158).** Garbage collection reclaims at most `gc_blocks_limit` blocks per `gc_step_period` tick and never catches up, so it must outpace block production. `validate_gc_config` — `config_validate.rs:91`:
1. All of `gc_blocks_limit`, `gc_fork_clean_step`, `gc_num_epochs_to_keep` must be non-zero and `gc_step_period > Duration::ZERO` (the last is newly required), else "gc config values should all be greater than 0, but gc_blocks_limit is …, gc_fork_clean_step is …, gc_num_epochs_to_keep is …, gc_step_period is ….".
2. If `gc_num_epochs_to_keep < MIN_GC_NUM_EPOCHS_TO_KEEP` (3) it is silently clamped by the accessor, so a warning is logged naming the value actually retained.
3. `min_block_production_delay == 0` (measured in nanoseconds, so sub-millisecond delays are not truncated) is a hard error: "consensus.min_block_production_delay is …, so blocks are produced at an unbounded rate and no gc config can keep up with them."
4. `required = ceil(gc_step_period / min_block_production_delay)` and `recommended = ceil(2 * gc_step_period / min_block_production_delay)` (`RECOMMENDED_GC_RATE_MULTIPLIER = 2`, `config_validate.rs:11`). If `gc_blocks_limit >= recommended`, pass. If `gc_blocks_limit < required`, **error**: "garbage collection cannot keep up with block production: gc_blocks_limit is … per gc_step_period of …, but a block can be produced every min_block_production_delay = …. The gc tail would fall permanently behind the chain head and storage would grow without bound. Raise gc_blocks_limit to at least {recommended}, or lower gc_step_period." In between (`required <= limit < recommended`) it only warns.

`set_block_production_delay(chain_id, fast, config)` (`config.rs:1063`) additionally raises `gc.gc_blocks_limit` for `--fast` localnet via `recommended_gc_blocks_limit_for_block_delay` (`config.rs:1096`), which returns `max(GCConfig::default().gc_blocks_limit, ceil(2 * gc_step_period / block_delay))`. The shipped defaults pass: 600 ms blocks (`MIN_BLOCK_PRODUCTION_DELAY`, `core/chain-configs/src/lib.rs:74`) against 2 blocks per 500 ms.

**Dynamic reload.** A subset of client config is reloadable without restart: `read_updatable_configs` re-reads `config.json`, log config, and validator key; `get_updatable_client_config` whitelists exactly `expected_shutdown`, `resharding_config`, `produce_chunk_add_transactions_time_limit`, `block_production_tracking_delay`, `min_block_production_delay`, `max_block_production_delay`, `max_block_wait_delay`, `chunk_wait_mult`, `doomslug_step_period` — `nearcore/src/dyn_config.rs:62` — `get_updatable_client_config`. Everything else requires a restart. Note the gc-rate validation above is only applied at startup, so a hot-reloaded `min_block_production_delay` is *not* re-checked against the gc config.

### 8. Startup preconditions and logging

- **Architecture gate (new, #15919).** `check_validator_arch` **panics** when a validator key is present and the chain is mainnet or testnet and `target_arch != "x86_64"`: "running a {chain} validator on the {arch} architecture is not supported; validators must run on x86_64" — `neard/src/cli.rs:392`, called from the run path at `neard/src/cli.rs:504` after `load_config`. Non-validators and other chains are unaffected. The rationale in the doc comment is divergence risk from a non-x86_64 execution environment.
- **Cloud-archive reader stores are refused.** Before any actor is spawned, a store that carries a cloud-archival reader head is rejected: "this store was written by a cloud-archive reader and cannot be used by a running node; point the cloud-archive tool at it instead" — `nearcore/src/lib.rs:399`.
- **Chain head is logged at startup (#16019).** After the client starts, the node logs `block_height`, `block_hash`, `epoch_id`, `epoch_height`, `epoch_start_height` and `protocol_version` of the chain head under the message "starting from chain head" — `nearcore/src/lib.rs:653-665`.
- `check_release_build` (`neard/src/cli.rs:368`) still warns on a debug build for mainnet/testnet.

### 9. Config surface changes an operator must apply when upgrading 2.13 → 2.14

**How an old `config.json` behaves.** `Config::from_file_skip_validation` deserializes through `serde_ignored` and no config struct sets `#[serde(deny_unknown_fields)]`, so a *removed or renamed* key is collected, logged once as "encountered unrecognized fields" and otherwise ignored — `nearcore/src/config.rs:555` / `:569-586`. A key that still exists but whose *value shape* changed is a hard deserialization failure returned as `ValidationError::ConfigFileError`, and the node exits. `check_for_deprecated_fields` additionally warns per known deprecated key before deserialization (`config.rs:622`).

| Field in 2.13 `config.json` | Status in 2.14 | Old value left in place → | Reference |
| --- | --- | --- | --- |
| `state_sync.sync = { "ExternalStorage": {...} }` | **removed**; `SyncConfig` is now a single unit variant `Peers` | **node fails to start** — `sync` still exists, so serde parses it and rejects the unknown variant (`unknown variant ExternalStorage, expected Peers`) before any validator runs. Delete the whole `state_sync.sync` block | `client_config.rs:328` — `SyncConfig`; `client_config.rs:388` — the `sync` field on `StateSyncConfig` (`:383`) |
| `state_sync.sync.ExternalStorage.{location,num_concurrent_requests,num_concurrent_requests_during_catchup,external_storage_fallback_threshold}` | **removed** with `ExternalStorageConfig` | same failure as the row above (they are inside it) | deleted from `client_config.rs` (#16009) |
| `consensus.state_sync_external_timeout` | **renamed** to `consensus.block_request_timeout`; default 60 s | **still loads with the value honoured** — the new field carries `#[serde(alias = "state_sync_external_timeout")]` | `nearcore/src/config.rs:160-163`; `client_config.rs:523` — `default_block_request_timeout` |
| `consensus.state_sync_external_backoff` | **removed** | loads, key ignored + warned (no alias, no replacement) | `nearcore/src/config.rs` `Consensus` (#16128) |
| `store.state_snapshot_config` / `StateSnapshotType::Disabled` | **removed**; snapshots are always enabled for a running node. Offline tools use `NightshadeRuntime::from_config_with_state_snapshot` | loads, key ignored + warned. Behavioural change: a node that had set `Disabled`/`ForReshardingOnly` now snapshots every epoch | `core/store/src/config.rs:11` — `StoreConfig` no longer has the field, and `StateSnapshotConfig`/`StateSnapshotType` are gone from that file; `nearcore/src/config.rs:921` — `NightshadeRuntime::from_config` (#16120) |
| `cloud_archival_writer` (top-level) | **moved** to `cloud_archival.writer`; a new `cloud_archival.reader` exists and is mutually exclusive with it | the stale top-level key is ignored, but the surviving `cloud_archival` block then has neither role, so **validation fails**: "`cloud_archival` sets neither `writer` nor `reader`; one is required." | `core/store/src/archive/cloud_storage/config.rs:12` (#16198, #16263); `config_validate.rs:275-280` |
| `cloud_archival.writer.polling_interval` default | 1 s → **5 s**; new `catch_up_throttle` (1100 ms) and `cloud_archival.reader.polling_interval` (5 s) | an explicit value still loads; only the omitted default moved | `client_config.rs:190`, `:200`, `:196` |
| `gc.*` | now **rejected** if it cannot keep up with block production; `gc_step_period > 0` newly required | loads, then **validation fails** if the rate check or the `> 0` check trips | `config_validate.rs:91` (#16158) |
| `EpochConfig.{num_block_producer_seats_per_shard, avg_hidden_validator_seats_per_shard, num_chunk_only_producer_seats}` | **removed** from the struct (epoch-config JSON, not `config.json`) | loads, keys ignored — no `deny_unknown_fields`, and they fall through the flattened untagged `ShardLayoutConfig` | `core/primitives/src/epoch_manager.rs:100-151` (#16098) |
| `neard init --state-sync-bucket` | **removed** CLI flag; `init_configs` lost its `state_sync_bucket` parameter | `neard init` errors on the unknown flag (clap) | `nearcore/src/config.rs:1119` — `init_configs` |
| — | **new** `view_access_keys_limit` (default 100) caps `view_access_key_list` results | n/a | `nearcore/src/config.rs:346`, `client_config.rs:592` |
| `chunks_cache_height_horizon` default | 128 → **2** | an explicit value still loads; only the omitted default moved | `client_config.rs:638` — `default_chunks_cache_height_horizon` |

**New network knobs** (`config.network`, `chain/network/src/config_json.rs`):

| Knob | Default | Reference |
| --- | --- | --- |
| `outgoing_queue_limiter_capacity_bytes` — semaphore bounding the total queued outgoing bytes across all connections. A message reserves its bytes with a **non-blocking** `try_acquire`; when there is no headroom the message is **dropped** (counted as `MessageDropped::OutgoingQueueLimitExceeded`, logged "dropping outgoing message: global outgoing-queue limit reached"), it does not wait | `DEFAULT_OUTGOING_QUEUE_LIMITER_CAPACITY_BYTES` = 3 GiB | `config_json.rs:218`; `chain/network/src/config.rs:42`; `chain/network/src/concurrency/outgoing_queue_limiter.rs:17` — `OutgoingQueueLimiter::try_acquire`; drop site `chain/network/src/peer/peer_actor.rs:442-455` |
| `max_write_buffer_capacity_bytes` — per-connection write-buffer cap; exceeding it **closes the connection** | `DEFAULT_MAX_WRITE_BUFFER_CAPACITY_BYTES` = 700 MiB | `config_json.rs:222`; `chain/network/src/config.rs:44`; enforced at `chain/network/src/peer/stream.rs:183` |
| `network.experimental.network_config_overrides.routing_graph_max_accounts_per_message` — cap on `AnnounceAccount` entries in one `SyncRoutingTable` | `DEFAULT_ROUTING_GRAPH_MAX_ACCOUNTS_PER_MESSAGE` = 10 000 | `config_json.rs:330` — `NetworkConfigOverrides`, field at `:352`; default `chain/network/src/config.rs:39`; applied at `config.rs:308` |

Both new byte knobs are `Option<usize>` directly on `config.network` and fall back to the constants in `NetworkConfig::new` (`chain/network/src/config.rs:464-469`); `routing_graph_max_accounts_per_message` is not a `config.network` field but an entry of the experimental override block. `routing_graph_max_accounts_per_message` must be `> 0` or `NetworkConfig::verify` fails (`chain/network/src/config.rs:630`). The other 2.14 network hardening — the **incoming** memory semaphore (fixed `INCOMING_SEMAPHORE_PERMITS = 1_000_000_000`, i.e. 1 GB, `chain/network/src/peer_manager/network_state/mod.rs:92`), the per-message-type size limit (`chain/network/src/network_protocol/mod.rs:501` — `max_size`), and the basic per-message rate limits (`chain/network/src/rate_limits/messages_limits.rs`) — is **not** configurable.

### 10. Chain differences (mainnet / testnet / localnet)

- `GenesisConfig::use_production_config()` is true for `chain_id == mainnet | testnet` (or an explicit `use_production_config` flag), routing to the hardcoded production epoch-config overrides — `genesis_config.rs:236`.
- Runtime-config genesis (version 0) differs only for testnet, which is patched with `INITIAL_TESTNET_CONFIG` for historical compatibility — `config_store.rs:167` — `RuntimeConfigStore::for_chain_id`.
- Mainnet/testnet genesis state roots are hard-asserted at init (Behavior §4.6); localnet/other chains compute freely. Mainnet/testnet genesis `EpochInfo` digests are likewise asserted — `chain/epoch-manager/src/genesis.rs:57` / `:60`.
- Mainnet/testnet genesis protocol version is `PROD_GENESIS_PROTOCOL_VERSION = 29` (`core/primitives-core/src/version.rs:649`), which takes the bespoke `prod_genesis` path (`chain/epoch-manager/src/genesis.rs:52`). Note 29 is far below `MIN_SUPPORTED_PROTOCOL_VERSION = 84` (`version.rs:652`): that is consistent because `get_config(29)` floors to the version-0 base (testnet: `INITIAL_TESTNET_CONFIG`) and no diff file below 53 exists any more, so removing the 46–52 diffs does not perturb the frozen genesis state roots. Genesis is *data*, not something a 2.14 binary re-executes.
- Block production delays are chain-specific: mainnet/testnet 600 ms (`config.rs:78`/`:79`), `--fast` localnet additionally bumps `gc_blocks_limit` (§7).
- The protocol upgrade vote schedule is chain-specific: mainnet has an empty schedule and testnet votes for PV 87 on 2026-09-21 (`core/primitives/src/version.rs:58-70`). Owned by [protocol-versioning](protocol-versioning.md).

## Interactions

- **Produces**: genesis `GenesisConfig` + per-shard genesis `StateRoot`s consumed by [chain-block-processing](chain-block-processing.md) and [sharding-chunks](sharding-chunks.md); per-version `RuntimeConfig` consumed by [runtime-execution](runtime-execution.md), [contract-vm](contract-vm.md) (`wasm_config`), [economics](economics.md) (fees/inflation), [cross-shard-congestion](cross-shard-congestion.md) (`congestion_control_config`, `bandwidth_scheduler_config`), and [stateless-validation](stateless-validation.md) (`witness_config`).
- **Consumes**: `ProtocolVersion` and `ProtocolFeature` activation from [protocol-versioning](protocol-versioning.md) (config diffs are keyed by the same version numbers); the genesis shard layout resolution goes through the `EpochManager`, see [epoch-validators-staking](epoch-validators-staking.md) and [dynamic resharding](../../docs/architecture/how/dynamic_resharding.md).
- **Node config edges**: `tracked_shards_config` and `gc` drive which shards/history a node keeps, feeding [sync](sync.md) and gc in [chain-block-processing](chain-block-processing.md); `state_sync` now only selects peer-based sync parameters (see [sync](sync.md)); the new network byte limits feed [networking-p2p](networking-p2p.md).

## Protocol-version-gated behavior

Genesis config is *set once* at genesis and is not itself version-gated; what evolves per version is the **runtime config**, expressed as parameter diffs rather than `if feature.enabled()` branches. The features whose activation is reflected as a config-parameter flip at or near this release (verified against `core/primitives-core/src/version.rs`, all activating at 87 per `ProtocolFeature::protocol_version`, `version.rs:619-629`):

| Feature | Activation | Parameter it flips (in `87.yaml`) |
| --- | --- | --- |
| `UniversalAccounts` (`version.rs:491`) | 87 | `universal_accounts: false → true`; read at `parameter_table.rs:466`. Also `max_state_init_entries: u32::MAX → 1_500` (#16408) |
| `FixContractLoadingError` (`version.rs:448`) | 87 | `fix_contract_loading_error: false → true` (`parameter_table.rs:460`) |
| `FixMlDsaCostCharging` (`version.rs:487`) | 87 | `fix_ml_dsa_cost_charging: false → true` (`parameter_table.rs:469`) |
| `RemoveGasRewards` (`version.rs:477`) | 87 | `burnt_gas_reward: 3/10 → 0/1` |
| `ReceiptPromiseInputSizeLimit` (`version.rs:450`) | 87 | `max_receipt_total_input_size: u32::MAX → 4_194_944` |
| (ML-DSA-65 host fn, #16104) | 87 | `ml_dsa_verify_host_fn: false → true` (`parameter_table.rs:472`) |
| (SHA3 host fns, #16339) | 87 | `sha3_host_fns: false → true` (`parameter_table.rs:473`) |
| — | 87 | `min_contract_size_per_local: 2` (new limit, no feature flag) |

Purely behavioral v87 features with no config-parameter counterpart — `RejectEmptyMethodName`, `RejectDelegateV2`, `RejectWithdrawFromGasKeyInDelegate`, `EnforceStorageProofLimitForAllActions`, `EarlyKickout`, `GlobalContractSameChunkCallFix` — are documented by their owning components.

`ProtocolFeature::DynamicResharding` is what decides whether the genesis epoch config declares a static shard layout at all, and therefore whether `genesis.config.shard_layout` is authoritative (§3) — `chain/epoch-manager/src/genesis.rs:68`.

Version bookkeeping: `CONFIG_DIFFS` is the sparse list 53, 55, 57, 59, 61, 62, 63, 64, 66, 67, 68, 69, 70, 72, 73, 74, 77, 78, 79, 82, 83, 84, 85, 87 (stable) then 129, 155, 157 (nightly) — `config_store.rs:26-63`. There is no `86` diff, so `get_config(86)` floors to key 85. `MIN_SUPPORTED_PROTOCOL_VERSION = 84` (`version.rs:652`) bounds the oldest config a running binary produces state under; `clamp_to_supported_protocol_version` (`version.rs:662`) raises older versions to 84 for read-only view calls on archival nodes, and `assert_supported_protocol_version` (`version.rs:672`) panics below it at callee boundaries. `STABLE_PROTOCOL_VERSION = 87` (`version.rs:680`) is what a stable binary votes for; `NIGHTLY_PROTOCOL_VERSION = 157` (`version.rs:683`), `SPICE_PROTOCOL_VERSION = 200` (`version.rs:689`).

## Invariants & failure modes

- **Total supply consistency**: `sum(amount + locked)` over `Account` records must equal `genesis_config.total_supply`; else `GenesisSemanticsError` "wrong total supply." — `genesis_validate.rs:132` — `validate_processed_records`.
- **Validator/stake consistency**: staked accounts (`locked > 0`) must exactly equal the validator set; keys must be valid staking keys; set non-empty — `genesis_validate.rs:101` (same fn).
- **Referential integrity**: every access-key/contract account must have an `Account` record — `genesis_validate.rs:143` / `:150`.
- **Uninitialized accounts carry no state**: an `Account` record with `is_initialized() == false` must have no access key, no contract and no `Data`/`GasKeyNonce` record — `genesis_validate.rs:157-176`. Without this the genesis state applier hits a code-hash assertion.
- **No duplicate accounts / no double contract deploy** per genesis records — `genesis_validate.rs:57` — `GenesisValidator::process_record`.
- **Threshold bounds**: `epoch_length > 0`, `online_min < online_max <= 1`, `gas_price_adjustment_rate < 1`, numerator/denominator `< 10_000_000` — `genesis_validate.rs:178-229` (same fn).
- **State roots agree with the shard layout**: `state_roots.len() == shard_layout.num_shards()` on every init path, asserted *before* committing anything — `core/store/src/genesis/initialization.rs:92` — `check_state_roots_match_layout`. A failed check leaves the store untouched, so a corrected config works on the next start.
- **Genesis epoch needs a layout**: below `DynamicResharding`, the genesis epoch config must declare a static shard layout or genesis epoch construction returns `EpochError::ShardingError` — `chain/epoch-manager/src/genesis.rs:68`.
- **Mainnet/testnet genesis roots are frozen**: any drift from the pinned state-root constants panics at startup — `core/store/src/genesis/initialization.rs:75` / `:79`; the genesis `EpochInfo` digests are asserted likewise — `chain/epoch-manager/src/genesis.rs:57` / `:60`.
- **Genesis height agreement**: stored genesis height must equal config's, else assert panic — `initialization.rs:47`.
- **GC must outpace block production**: otherwise config validation fails with "garbage collection cannot keep up with block production" — `nearcore/src/config_validate.rs:150`.
- **A validator must not be a cloud-archival writer** — cross-file semantics error at `nearcore/src/config.rs:1752`.
- **A mainnet/testnet validator must run on x86_64** — panic at `neard/src/cli.rs:397`.
- **A cloud-archive reader store cannot back a running node** — `anyhow::bail!` at `nearcore/src/lib.rs:399`.
- **Config-diff soundness**: a diff whose declared `old` value disagrees with the accumulated table fails store construction (panics at startup) with `WrongOldValue`/`NoOldValueExists`/`OldValueExists` — `parameter_table.rs:530` / `config_store.rs:121`.
- **Malformed genesis/params panic rather than error**: `GenesisConfig::from_json` panics (`genesis_config.rs:352`); `RuntimeConfigStore::new` panics on unparseable base/diff files (`config_store.rs:87`, `:115`, `:121`).
- **Node-config validation** returns `ValidationError` (does not panic) and aggregates all failures — `config_validate.rs:15`.
- **StateRoots genesis cannot recompute a consistent genesis hash** — testing/forknet-only; documented at `genesis_config.rs:314` and `:526`.

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `core/chain-configs/src/genesis_config.rs:110` | `GenesisConfig` | network-wide genesis parameters struct |
| `core/chain-configs/src/genesis_config.rs:236` | `GenesisConfig::use_production_config` | mainnet/testnet production path |
| `core/chain-configs/src/genesis_config.rs:243` | `From<&GenesisConfig> for EpochConfig` | genesis → static-layout epoch config |
| `core/chain-configs/src/genesis_config.rs:304` | `GenesisContents` | Records / RecordsFile / StateRoots variants |
| `core/chain-configs/src/genesis_config.rs:334` | `Genesis` | config + contents wrapper |
| `core/chain-configs/src/genesis_config.rs:352` | `GenesisConfig::from_json` | parse (panics on error) |
| `core/chain-configs/src/genesis_config.rs:462` | `RecordsProcessor::visit_map` | streaming record deserializer |
| `core/chain-configs/src/genesis_config.rs:526` | `GenesisJsonHasher::process_state_roots` | StateRoots genesis-hash caveat |
| `core/chain-configs/src/genesis_config.rs:574` | `Genesis::from_file` | load + validate genesis |
| `core/chain-configs/src/genesis_config.rs:655` | `Genesis::validate` | Full vs UnsafeFast dispatch |
| `core/chain-configs/src/genesis_config.rs:687` | `Genesis::for_each_record` | stream/iterate records |
| `core/chain-configs/src/genesis_validate.rs:10` | `validate_genesis` | entry; short-circuits for StateRoots |
| `core/chain-configs/src/genesis_validate.rs:57` | `GenesisValidator::process_record` | per-record accumulation; collects uninitialized accounts |
| `core/chain-configs/src/genesis_validate.rs:101` | `GenesisValidator::validate_processed_records` | semantic genesis invariants |
| `core/chain-configs/src/genesis_validate.rs:157` | (uninitialized-account loop) | no key/code/data for uninitialized accounts |
| `core/chain-configs/src/client_config.rs:46` | `TrackedShardsConfig` | shard-tracking variants |
| `core/chain-configs/src/client_config.rs:100` | `TrackedShardsConfig::from_deprecated_config_values` | deprecated `tracked_*` fallback order |
| `core/chain-configs/src/client_config.rs:132` | `GCConfig` | gc knobs + defaults |
| `core/chain-configs/src/client_config.rs:214` | `CloudArchivalReaderConfig` | reader polling interval |
| `core/chain-configs/src/client_config.rs:233` | `CloudArchivalWriterConfig` | writer polling/throttle/snapshot cadence |
| `core/chain-configs/src/client_config.rs:328` | `SyncConfig` | single `Peers` variant (ExternalStorage removed) |
| `core/chain-configs/src/client_config.rs:523` | `default_block_request_timeout` | renamed from `default_state_sync_external_timeout` |
| `core/store/src/archive/cloud_storage/config.rs:12` | `CloudArchivalConfig` | nested `writer`/`reader` sub-configs |
| `core/store/src/genesis/initialization.rs:29` | `initialize_sharded_genesis_state` | genesis state-root init entry (now takes `&ShardLayout`) |
| `core/store/src/genesis/initialization.rs:92` | `check_state_roots_match_layout` | roots-vs-layout cross-check before commit |
| `core/store/src/genesis/initialization.rs:109` | `initialize_genesis_state` | resolves layout: static epoch config else genesis |
| `core/store/src/genesis/state_applier.rs:345` | `GenesisStateApplier::apply` | records → trie → state root |
| `core/primitives/src/epoch_manager.rs:364` | `AllEpochConfig::genesis_shard_layout` | static layout, else genesis-declared |
| `chain/epoch-manager/src/genesis.rs:68` | (static-layout requirement) | ShardingError when no layout and no dynamic resharding |
| `core/parameters/src/config.rs:17` | `RuntimeConfig` | materialized per-version runtime params |
| `core/parameters/src/config_store.rs:26` | `CONFIG_DIFFS` | ordered (version, diff-file) list; 46–52 removed, 87/157 added |
| `core/parameters/src/config_store.rs:85` | `RuntimeConfigStore::new` | build versioned config map from base + diffs |
| `core/parameters/src/config_store.rs:167` | `RuntimeConfigStore::for_chain_id` | chain-specific overrides (testnet/benchmarknet) |
| `core/parameters/src/config_store.rs:237` | `RuntimeConfigStore::get_config` | floor lookup by version |
| `core/parameters/src/parameter_table.rs:409` | `TryFrom<&ParameterTable> for RuntimeConfig` | assemble config from typed params |
| `core/parameters/src/parameter_table.rs:530` | `ParameterTable::apply_diff` | validated diff application |
| `nearcore/src/config.rs:119` | `Consensus` | `block_request_timeout` (alias `state_sync_external_timeout`) |
| `nearcore/src/config.rs:226` | `Config` | node `config.json` struct |
| `nearcore/src/config.rs:658` | `Config::tracked_shards_config` | resolve tracked-shards (new + deprecated) |
| `nearcore/src/config.rs:681` | `Config::state_sync_config` | derive dump config from `cloud_archival.writer` |
| `nearcore/src/config.rs:715` | `NearConfig::new` | assemble ClientConfig with inferred fields |
| `nearcore/src/config.rs:921` | `NightshadeRuntime::from_config` | state snapshots unconditionally enabled |
| `nearcore/src/config.rs:1096` | `recommended_gc_blocks_limit_for_block_delay` | 2× gc rate over block production |
| `nearcore/src/config.rs:1752` | (validator + cloud writer check) | cross-file semantics error in `load_config` |
| `nearcore/src/config_validate.rs:11` | `RECOMMENDED_GC_RATE_MULTIPLIER` | = 2 |
| `nearcore/src/config_validate.rs:38` | `ConfigValidator::validate_all_conditions` | node-config validation order |
| `nearcore/src/config_validate.rs:91` | `ConfigValidator::validate_gc_config` | gc-vs-block-production rate check |
| `nearcore/src/config_validate.rs:264` | `ConfigValidator::validate_cloud_archival_config` | writer/reader exclusivity + data requirements |
| `nearcore/src/dyn_config.rs:62` | `get_updatable_client_config` | whitelist of hot-reloadable fields |
| `nearcore/src/lib.rs:399` | (reader-head store guard) | refuse a cloud-archive reader store |
| `nearcore/src/lib.rs:420` | (genesis shard layout) | layout taken from the genesis `EpochInfo` |
| `nearcore/src/lib.rs:653` | "starting from chain head" | startup chain-height/epoch log |
| `neard/src/cli.rs:392` | `check_validator_arch` | panic for non-x86_64 mainnet/testnet validators |
| `chain/network/src/config.rs:42` | `DEFAULT_OUTGOING_QUEUE_LIMITER_CAPACITY_BYTES` | 3 GiB outgoing semaphore |
| `chain/network/src/config.rs:44` | `DEFAULT_MAX_WRITE_BUFFER_CAPACITY_BYTES` | 700 MiB per-connection write buffer |
| `chain/network/src/config.rs:39` | `DEFAULT_ROUTING_GRAPH_MAX_ACCOUNTS_PER_MESSAGE` | 10 000 announce-accounts per SyncRoutingTable |
| `chain/network/src/concurrency/outgoing_queue_limiter.rs:17` | `OutgoingQueueLimiter::try_acquire` | non-blocking reservation; no headroom ⇒ message dropped |
| `nearcore/src/config.rs:555` | `Config::from_file_skip_validation` | `serde_ignored` parse: unknown keys warn, shape mismatches fail |
| `chain/network/src/config_json.rs:218` | `Config::outgoing_queue_limiter_capacity_bytes` | new `config.network` knob |
| `chain/network/src/peer_manager/network_state/mod.rs:92` | `INCOMING_SEMAPHORE_PERMITS` | 1 GB incoming semaphore (not configurable) |
| `core/primitives-core/src/version.rs:649` | `PROD_GENESIS_PROTOCOL_VERSION` | = 29, mainnet/testnet genesis version |
| `core/primitives-core/src/version.rs:652` | `MIN_SUPPORTED_PROTOCOL_VERSION` | = 84 |
| `core/primitives-core/src/version.rs:680` | `STABLE_PROTOCOL_VERSION` | = 87 |

## Open questions

- `docs/GenesisConfig/GenesisConfig.md` was not updated this release and is field-oriented; it does not reflect all defaulted fields (e.g. `num_chunk_producer_seats`, `chunk_producer_assignment_changes_limit`) nor the removed `EpochConfig` fields. Treat the struct at `genesis_config.rs:110` as authoritative. Not broken, only incomplete.
- The base runtime parameter values and each per-version diff live in `core/parameters/res/runtime_configs/*.yaml`; this spec describes the *mechanism* that consumes them but does not enumerate individual parameter values (owned by the consuming components).
- The 46–52 diff removal was verified to be behaviour-preserving for versions `>= 53` by inspection (values folded into `parameters.yaml`, no later diff re-declares them with an `old` guard) and by the unchanged semantic content of the `__53…__85` config snapshots. It was not re-derived by executing the store; a stronger check would be to diff materialized configs across the two commits.
- `validate_gc_config` runs only on the startup path. `min_block_production_delay` is in the hot-reload whitelist (`dyn_config.rs:62`), so a running node can be reloaded into a gc/block-rate combination that `validate_config` would have rejected. No code was found that re-checks it after reload; whether this is intentional is not determinable from code alone.

