# Genesis & node configuration

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `core/chain-configs/src/genesis_config.rs`, `core/chain-configs/src/genesis_validate.rs`, `core/parameters/src/config.rs`, `core/parameters/src/config_store.rs`, `core/parameters/src/parameter_table.rs`, `core/store/src/genesis/initialization.rs`, `core/store/src/genesis/state_applier.rs`, `nearcore/src/config.rs`, `nearcore/src/config_validate.rs`, `nearcore/src/dyn_config.rs`

## Role

This component bootstraps a network's initial conditions and a node's runtime behavior. It has three distinct pieces: (1) the **genesis** — chain parameters plus the initial state records that, applied to an empty trie, produce the genesis state roots and from which the genesis block is derived; (2) the **runtime config** — the versioned schema of fees, wasm limits, account-creation, congestion, witness, and bandwidth parameters, derived per protocol version by applying a chain of diffs to a base parameter table; and (3) the **node config** (`config.json`) — non-consensus operational settings (tracked shards, archival/gc, state-sync source, network). The genesis config seeds [epoch-validators-staking](epoch-validators-staking.md) (validators, seats, shard layout) and [economics](economics.md) (supply, inflation, reward rate). The runtime config feeds [runtime-execution](runtime-execution.md), [contract-vm](contract-vm.md), [cross-shard-congestion](cross-shard-congestion.md), and [state-storage](state-storage.md). Version selection of the runtime config is owned by [protocol-versioning](protocol-versioning.md); this spec documents only the store/derivation mechanism, not the per-block selection.

## Key data structures

- **`GenesisConfig`** — `core/chain-configs/src/genesis_config.rs:110` — the non-record portion of genesis. Chain identity (`protocol_version`, `chain_id`, `genesis_time`, `genesis_height`), economics (`total_supply`, `min_gas_price`, `max_gas_price`, `gas_price_adjustment_rate`, `max_inflation_rate`, `protocol_reward_rate`, `protocol_treasury_account`, `num_blocks_per_year`), validator/epoch params (`validators: Vec<AccountInfo>`, `epoch_length`, `num_block_producer_seats`, `num_chunk_producer_seats`, `num_chunk_validator_seats`, `shard_layout`, kickout thresholds, `minimum_stake_ratio`, `protocol_upgrade_stake_threshold`), and `transaction_validity_period`. Many fields carry `#[serde(default = …)]` + `SmartDefault` so old/short genesis files still deserialize.
- **`Genesis`** — `core/chain-configs/src/genesis_config.rs:334` — `config: GenesisConfig` (`#[serde(flatten)]`) plus `contents: GenesisContents`. Custom `no_value_and_null_as_default` deserializer (`:290`) is used because `serde(flatten)` does not compose with `serde(default)`.
- **`GenesisContents`** — `core/chain-configs/src/genesis_config.rs:304` — untagged enum: `Records { records: GenesisRecords }` (inline), `RecordsFile { records_file: PathBuf }` (streamed, for large genesis), or `StateRoots { state_roots }` (testing/mock-fork only — cannot compute a consistent genesis hash, warned at `:315`, variant at `:319`).
- **`GenesisRecords`** / **`StateRecord`** — `core/chain-configs/src/genesis_config.rs:284` / `core/primitives/src/state_record.rs:35` — the initial state. `StateRecord` variants: `Account`, `Data`, `Contract` (base64 code), `AccessKey`, `GasKeyNonce`, `PostponedReceipt`, `ReceivedData`, `DelayedReceipt`.
- **`RuntimeConfig`** — `core/parameters/src/config.rs:17` — the versioned runtime schema: `fees: Arc<RuntimeFeesConfig>`, `wasm_config: Arc<vm::Config>`, `account_creation_config`, `congestion_control_config`, `witness_config`, `bandwidth_scheduler_config`, `use_state_stored_receipt`, `min_gas_purchase_price`, `account_creation_charge`.
- **`RuntimeConfigStore`** — `core/parameters/src/config_store.rs:74` — `BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>`; `get_config(v)` returns the config of the greatest stored version `<= v`.
- **`ParameterTable` / `ParameterTableDiff`** — `core/parameters/src/parameter_table.rs:346` / `:362` — a `BTreeMap<Parameter, ParameterValue>` and a typed old→new diff over it. `RuntimeConfig` is built from a `ParameterTable` via `TryFrom` (`:409`).
- **`Config`** — `nearcore/src/config.rs:227` — the node's `config.json`: file paths (`genesis_file`, `genesis_records_file`, key files), `network`, `consensus`, `tracked_shards_config`, `archive`/`save_trie_changes`/`save_tx_outcomes`, `gc: GCConfig`, `state_sync`, cloud-archival, plus deprecated `tracked_*` fields.
- **`NearConfig`** — `nearcore/src/config.rs:697` — the fully-assembled in-memory node config: `config: Config`, `client_config: ClientConfig`, `network_config`, `genesis: Genesis`, `validator_signer`, telemetry/rpc.
- **`TrackedShardsConfig`** — `core/chain-configs/src/client_config.rs:49` — `NoShards`, `Shards(Vec<ShardUId>)`, `AllShards`, `ShadowValidator(AccountId)`, `Schedule(Vec<Vec<ShardId>>)`, `Accounts(Vec<AccountId>)`.

## Behavior

### 1. Loading genesis from disk

`Genesis::from_file` (`core/chain-configs/src/genesis_config.rs:574`) reads one JSON file (comments stripped via `strip_comments_from_json_str`), deserializes a `Genesis` (config flattened with records), then runs `validate(genesis_validation)`. `Genesis::from_files` (`:610`) instead reads `GenesisConfig` from one file and points `contents` at a separate `RecordsFile`. Large record sets are never fully loaded: `for_each_record` (`:687`) streams `RecordsFile` through `stream_records_from_file` (`:497`), whose `RecordsProcessor` visitor (`:441`) reads the `records` array element-by-element and ignores all other top-level fields.

### 2. Genesis validation

`Genesis::validate` (`:655`) dispatches on `GenesisValidationMode`: `Full` runs `validate_genesis`, `UnsafeFast` logs `skipped genesis validation` and returns Ok. `validate_genesis` (`core/chain-configs/src/genesis_validate.rs:10`) early-returns Ok for `StateRoots` contents (records cannot be checked), otherwise streams every record through `GenesisValidator::process_record` (`:53`) accumulating total supply, account-id set, staked accounts, access-key/contract account-ids, then `validate_processed_records` (`:85`) enforces:

1. Each validator's public key passes `is_valid_staking_key`; else "validator staking key is not valid".
2. No duplicate validator account-ids; at least one validator ("No validators in genesis").
3. Summed `account.amount + account.locked` over all account records equals `config.total_supply` ("wrong total supply") — the supply invariant.
4. The validator set (account → stake) exactly equals the set of accounts whose record carries non-zero `locked` ("Validator accounts do not match staked accounts").
5. Every `AccessKey` and `Contract` record's account exists as an `Account` record.
6. `online_min_threshold < online_max_threshold <= 1`, numerators/denominators `< 10_000_000`, `gas_price_adjustment_rate < 1`, and `epoch_length != 0`.

All errors are collected and returned together as `ValidationError::GenesisSemanticsError` (`:199`).

### 3. Computing genesis state roots

`initialize_genesis_state` (`core/store/src/genesis/initialization.rs:78`) builds the `EpochConfig` from the genesis config and calls `initialize_sharded_genesis_state` (`:29`):

1. If the store already holds genesis state roots, reuse them (and assert the stored genesis height matches config, `:46`).
2. Otherwise, if a `state_dump` file exists in `home_dir`, load roots from the dump (`genesis_state_from_dump`, `:82`), ignoring any inline records.
3. Otherwise compute from records via `genesis_state_from_genesis` (`:95`): for `StateRoots` contents, return the roots as-is; for `Records`/`RecordsFile`, pick the runtime config with `RuntimeConfigStore::for_chain_id(chain_id).get_config(protocol_version)` to get the `storage_usage_config`, then **distribute every record to its shard** by `state_record_to_shard_id` over the static shard layout (system accounts skipped), assert the protocol-treasury account is present, and apply each shard's records **in parallel** (`into_par_iter`, `:152`) via `GenesisStateApplier::apply`.
4. Persist roots + genesis height (`set_genesis_state_roots`, `:62`). For mainnet/testnet the resulting roots are asserted against hardcoded constants (`:69`, `:73`).

`GenesisStateApplier::apply` (`core/store/src/genesis/state_applier.rs:345`) writes one shard's trie in this order (ordering is load-bearing for the final root):

1. `apply_batch` (`:174`) iterates records for this shard's account-id set. It writes `Account`/`Data`/`Contract`/`AccessKey`/`GasKeyNonce`/`ReceivedData`/`DelayedReceipt` immediately; `Contract` recomputes the code hash and asserts it equals the account's stored hash; `PostponedReceipt`s are buffered.
2. After all records, `storage_usage` for each account is recomputed from `StorageComputer` (`:28`, fixed `num_bytes_account` per account plus per-record byte counts) and written back (`:278`) — so genesis `storage_usage` is derived, not taken from the record.
3. Buffered postponed receipts are inserted after all `ReceivedData` (so pending-data counts are correct, `:294`).
4. Each validator's `locked` balance is set to its genesis stake amount (`:320`).
5. `apply_delayed_receipts` writes `DelayedReceiptIndices` if non-empty (`:334`).

Writes are batched through `AutoFlushingTrieUpdate` (`:100`), which commits under `StateChangeCause::InitialState` and flushes to the trie + flat storage roughly every `TARGET_OUTSTANDING_WRITES / active_writers` modifications (`:127`). The final `flush()` (`:369`) returns the shard's state root.

### 4. Deriving the runtime config for a version

`RuntimeConfigStore::new` (`core/parameters/src/config_store.rs:89`):

1. Parse `BASE_CONFIG` (`res/runtime_configs/parameters.yaml`) into a `ParameterTable`; build `RuntimeConfig` from it and store it at key `0`.
2. For each `(version, diff_file)` in the ordered `CONFIG_DIFFS` table (`:26`), parse the diff, `apply_diff` it onto the running `ParameterTable`, rebuild `RuntimeConfig`, and store it at that version. Configs are cumulative: each version's table is the base plus every diff up to and including it.
3. If a `genesis_runtime_config` override is supplied, it replaces the key-`0` config (testnet compatibility).

`apply_diff` (`core/parameters/src/parameter_table.rs:526`) requires the diff's stated `old` value to exactly match the current value, else it errors (`NoOldValueExists`/`OldValueExists`/`WrongOldValue`); a `new: None` removes the parameter. `RuntimeConfig::try_from(&ParameterTable)` (`:409`) reads each typed `Parameter` into the structured config — action fees (`get_fee`), wasm `ext_costs`/limits, congestion (`get_congestion_control_config`, `:501`), witness, bandwidth, account-creation, `min_gas_purchase_price`, `account_creation_charge`. `get_config(v)` (`config_store.rs:241`) returns the entry for the greatest version `<= v`, so a config diff at version `N` takes effect for all versions in `[N, next_diff)`.

`for_chain_id` (`config_store.rs:171`) overrides per chain: `testnet` seeds key-`0` with `RuntimeConfig::initial_testnet_config()` (the historically-different testnet genesis config), `benchmarknet`/`congestion_control_test` mutate the latest config for benchmarking, everything else uses the unmodified base.

### 5. Loading and assembling the node config

`load_config` (`nearcore/src/config.rs:1690`):

1. `Config::from_file_skip_validation` reads `config.json` (comments stripped; `serde_ignored` warns on unrecognized fields, `:570`); then `config.validate()` (deferred so the genesis/key files can still be inspected before any panic).
2. Load validator key and node key; missing/invalid files accumulate into `ValidationErrors`.
3. Load genesis with `from_files` (if `genesis_records_file` is set) or `from_file`, always with `UnsafeFast` first (so the chain id is known for tracked-shards checks), then re-validate with the caller's `genesis_validation` mode.
4. If any validation error accumulated, return it; otherwise assemble `NearConfig::new`.

`NearConfig::new` (`:717`) projects `Config` + `Genesis` into a `ClientConfig`. Notable derivations: `is_archive_or_rpc = archive || tracked_shards_config().is_rpc()` (`:724`); `save_trie_changes` defaults to `!archive` (`:787`) — non-archival nodes need trie changes for gc; `save_tx_outcomes` defaults to `is_archive_or_rpc` (`:788`); `epoch_length`/`num_block_producer_seats` are copied from genesis (`:773`). `tracked_shards_config()` (`:659`) prefers the new field, else maps the deprecated `tracked_*` fields via `from_deprecated_config_values` (`client_config.rs:103`) — note a non-empty deprecated `tracked_shards` list historically means `AllShards` regardless of contents (`:110`).

### 6. Node config validation

`validate_config` (`nearcore/src/config_validate.rs:10`) runs semantic checks (collected, not fail-fast): `min_block_production_delay <= max_block_production_delay` and `<= max_block_wait_delay`; `header_sync_expected_height_per_second != 0`; all three `gc.*` fields `> 0` (`:68`); plus cloud-archival, cold-store, state-sync, and tracked-shards consistency checks.

### 7. Dynamic (reloadable) config

`read_updatable_configs` (`nearcore/src/dyn_config.rs:13`) is called at startup and on reload (SIGHUP). Only a small allowlist can change without restart, enumerated in `get_updatable_client_config` (`:62`): `expected_shutdown`, `resharding_config`, `produce_chunk_add_transactions_time_limit`, and the consensus timing fields (`block_production_tracking_delay`, `min`/`max_block_production_delay`, `max_block_wait_delay`, `chunk_wait_mult`, `doomslug_step_period`), plus the log config and the validator key (hot-loaded, `:118`). Everything else — genesis, runtime config, tracked shards, archival/gc, network — requires a restart. A missing `log_config.json` resets logging to defaults (`:104`); a malformed updatable file is rejected and the node keeps its current values.

## Interactions

- **Consumes**: `config.json`, `genesis.json` (+ optional records file), validator/node key files, and the embedded `res/runtime_configs/*.yaml` parameter files.
- **Produces**: genesis state roots + genesis height in the store (read back by [chain-block-processing](chain-block-processing.md) via `get_genesis_state_roots`), the per-version `RuntimeConfig` consumed by [runtime-execution](runtime-execution.md) / [contract-vm](contract-vm.md) / [cross-shard-congestion](cross-shard-congestion.md), the `EpochConfig` (`genesis_config.rs:243`) consumed by [epoch-validators-staking](epoch-validators-staking.md), and the `ClientConfig` (tracked shards, gc, state-sync) consumed by [chain-block-processing](chain-block-processing.md) and [sync](sync.md).
- **Version selection** of the runtime config per block is owned by [protocol-versioning](protocol-versioning.md); this component supplies only the store and the `get_config(version)` lookup.

## Protocol-version-gated behavior

The runtime config schema itself is versioned through `CONFIG_DIFFS` (`config_store.rs:26`), not through `ProtocolFeature` flags directly: each protocol bump that changes parameter values ships a `res/runtime_configs/<version>.yaml` diff, and `get_config(version)` resolves it. At version 86 the relevant stored diff versions include `84`, `85`, and `86`. ProtocolFeatures that change *which* config fields exist or how genesis/config is interpreted, verified against `core/primitives-core/src/version.rs:453`:

| Feature | Activates | Effect on this component |
|---------|-----------|--------------------------|
| `FixContractLoadingError` | 86 | The 86.yaml runtime-config diff; charges the contract-loading fee and finalizes as a gas-bearing abort on `Module::deserialize` failure. |
| `AccountCostIncrease` | 85 | Raises `account_creation_charge` (the `RuntimeConfig` field at `config.rs:46`) via the 85.yaml diff. |
| `DynamicResharding` | 85 | Interacts with the genesis `dynamic_resharding` / `shard_layout` fields; see [epoch-validators-staking](epoch-validators-staking.md). |
| `ShuffleShardAssignments` | 143 (nightly) | Genesis `shuffle_shard_assignment_for_chunk_producers` flag (`genesis_config.rs:212`). |
| `FixContractLoadingCost` | 129 (nightly) | `wasm_config.fix_contract_loading_cost` parameter. |

`MIN_SUPPORTED_PROTOCOL_VERSION` is 83 and `STABLE_PROTOCOL_VERSION`/`PROTOCOL_VERSION` is 86 (`version.rs:596`, `:624`, `:636`). Genesis on mainnet/testnet was created at `PROD_GENESIS_PROTOCOL_VERSION = 29` (`version.rs:593`); the `RuntimeConfigStore` therefore carries diffs all the way back so an archival node can reconstruct any historical config.

## Invariants & failure modes

- **Supply invariant**: genesis `total_supply` must equal the sum of `amount + locked` over account records, and the validator set must equal the staked-account set; violations are `GenesisSemanticsError` (`genesis_validate.rs:114`, `:122`). The invariant is documented as un-enforceable at construction time (`genesis_config.rs:329`) and is checked only by `validate_genesis`.
- **Protocol-treasury presence**: `genesis_state_from_genesis` asserts the protocol-treasury account appears among records, else panics "Genesis spec doesn't have protocol treasury account" (`initialization.rs:142`).
- **Mainnet/testnet root pinning**: computed genesis roots are asserted equal to hardcoded hashes for mainnet/testnet (`initialization.rs:69`, `:73`); a mismatch panics at startup.
- **`StateRoots` genesis is unsound**: its genesis hash cannot be made consistent with a records-based genesis; flagged for testing only (`genesis_config.rs:315`, `GenesisJsonHasher::process_state_roots:526`).
- **Diff-application strictness**: a runtime-config diff whose `old` value disagrees with the current parameter table aborts store construction with a panic (`config_store.rs:125`, where the failed `apply_diff` is unwrapped); this is a build-time guard against mis-authored diffs.
- **Parse failures**: `GenesisConfig::from_json`/`from_file` panic on malformed JSON (`genesis_config.rs:352`, `:364`); `load_config` collects key/genesis errors and returns them, but panics if genesis or network signer is somehow still `None` after error checks (`config.rs:1751`).
- **Config compatibility**: unrecognized `config.json` fields are warned, not rejected (`config.rs:578`); deprecated fields are mapped or warned (`:598`, `:622`).

## Code anchors

| Location | Symbol | What happens here |
|----------|--------|-------------------|
| `core/chain-configs/src/genesis_config.rs:110` | `GenesisConfig` | Genesis chain/economic/validator params with serde defaults. |
| `core/chain-configs/src/genesis_config.rs:243` | `From<&GenesisConfig> for EpochConfig` | Projects genesis into the epoch config. |
| `core/chain-configs/src/genesis_config.rs:304` | `GenesisContents` | Records / RecordsFile / StateRoots. |
| `core/chain-configs/src/genesis_config.rs:497` | `stream_records_from_file` | Streams records without loading them all. |
| `core/chain-configs/src/genesis_config.rs:655` | `Genesis::validate` | Dispatch Full vs UnsafeFast. |
| `core/chain-configs/src/genesis_config.rs:917` | `get_initial_supply` | Sums amount+locked over account records. |
| `core/chain-configs/src/genesis_validate.rs:85` | `validate_processed_records` | Supply / validator / threshold invariants. |
| `core/store/src/genesis/initialization.rs:29` | `initialize_sharded_genesis_state` | Reuse / dump / compute genesis roots, persist. |
| `core/store/src/genesis/initialization.rs:95` | `genesis_state_from_genesis` | Distribute records to shards, apply in parallel. |
| `core/store/src/genesis/state_applier.rs:345` | `GenesisStateApplier::apply` | Per-shard ordered trie write → state root. |
| `core/store/src/genesis/state_applier.rs:28` | `StorageComputer` | Derives genesis `storage_usage` per account. |
| `core/parameters/src/config.rs:17` | `RuntimeConfig` | Versioned runtime schema struct. |
| `core/parameters/src/config_store.rs:89` | `RuntimeConfigStore::new` | Base + cumulative diffs → per-version map. |
| `core/parameters/src/config_store.rs:26` | `CONFIG_DIFFS` | Ordered (version, yaml-diff) table. |
| `core/parameters/src/config_store.rs:241` | `get_config` | Greatest version `<= v` lookup. |
| `core/parameters/src/parameter_table.rs:409` | `TryFrom<&ParameterTable> for RuntimeConfig` | Typed parameter → structured config. |
| `core/parameters/src/parameter_table.rs:526` | `apply_diff` | Strict old→new diff application. |
| `nearcore/src/config.rs:227` | `Config` | The node's `config.json`. |
| `nearcore/src/config.rs:717` | `NearConfig::new` | Assemble ClientConfig from Config+Genesis. |
| `nearcore/src/config.rs:659` | `tracked_shards_config` | New field or deprecated-field fallback. |
| `nearcore/src/config.rs:1690` | `load_config` | End-to-end node config load + validate. |
| `nearcore/src/config_validate.rs:10` | `validate_config` | Node-config semantic checks. |
| `nearcore/src/dyn_config.rs:62` | `get_updatable_client_config` | Allowlist of hot-reloadable fields. |
| `core/chain-configs/src/client_config.rs:49` | `TrackedShardsConfig` | Shard-tracking modes. |
| `core/primitives-core/src/version.rs:453` | `ProtocolFeature::protocol_version` | Feature → activation version. |

## Open questions

- The nomicon pages under `docs/GenesisConfig/` (e.g. `GenesisConfig.md`) describe individual fields but predate several stable fields (`num_chunk_producer_seats`, `num_chunk_validator_seats`, `chunk_validator_only_kickout_threshold`, `target_validator_mandates_per_shard`) and the `tracked_shards_config` migration; they are stale relative to the structs cited here. Treated as informational only.
- The exact set of parameter-value changes inside each `res/runtime_configs/{84,85,86}.yaml` diff was not enumerated field-by-field here (out of scope; the owning components — fees/economics/runtime — document their own values). The mechanism by which they are applied is documented above.
