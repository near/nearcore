# Protocol versioning & upgrades

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `core/primitives-core/src/version.rs`, `core/primitives/src/version.rs`, `core/primitives/src/upgrade_schedule.rs`, `core/parameters/src/config_store.rs`, `core/parameters/src/parameter_table.rs`, `chain/epoch-manager/src/lib.rs`

## Role

This component defines the single linearly-increasing integer (`ProtocolVersion`) that names the consensus rules in force, the registry that maps every feature to the version at which it activates, the per-version `RuntimeConfig` assembly, and the on-chain voting mechanism by which a network steps from one version to the next. It is upstream of *every* other component: each component's "Protocol-version-gated behavior" section asks `ProtocolFeature::X.enabled(version)` against the version this component resolves. The vote tally itself runs inside [epoch management](epoch-validators-staking.md) at each epoch boundary; this spec documents the mechanism and links there for the surrounding epoch machinery.

## Key data structures

- **`ProtocolVersion`** — `core/primitives-core/src/types.rs` (re-exported `core/primitives/src/version.rs:17` — `pub use … ProtocolVersion`) — a `u32`. A higher number is a strict superset of rules; comparison is the only operation features use (`>=`).
- **`ProtocolFeature`** — `core/primitives-core/src/version.rs:11` — a `#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]` enum that is the authoritative registry of every protocol-affecting change. Most historical variants are `#[deprecated]` and named `_Deprecated…`; live variants at v86 include e.g. `GasKeys`, `DelegateV2`, `StrictNonce`, `PostQuantumSignatures`, `ExecutionMetadataV4`, `FixContractLoadingError`. Each variant carries no data; its activation version is a pure function of the variant (see `protocol_version` below).
- **`RuntimeConfigStore`** — `core/parameters/src/config_store.rs:73` — `BTreeMap<ProtocolVersion, Arc<RuntimeConfig>>`. Holds one fully-materialized `RuntimeConfig` per protocol version at which any parameter changed. Lookup is "latest entry ≤ requested version" (`get_config`, `config_store.rs:241`).
- **`ParameterTable`** — `core/parameters/src/parameter_table.rs:346` — `BTreeMap<Parameter, ParameterValue>`; the flat key→value map parsed from a YAML config. A `RuntimeConfig` is built from a `ParameterTable` via `TryFrom` (`parameter_table.rs:409`).
- **`ParameterTableDiff`** — `core/parameters/src/parameter_table.rs:362` — `BTreeMap<Parameter, (Option<old>, Option<new>)>`; one per-version YAML diff. Carries the expected old value so application is checked, not blind.
- **`ProtocolUpgradeVotingSchedule`** — `core/primitives/src/upgrade_schedule.rs:31` — `{ client_protocol_version, schedule: Vec<(DateTime<Utc>, ProtocolVersion)> }`. Decides which version *this node* votes for at a given wall-clock time. Empty schedule = vote for client version immediately.
- **`EpochInfoAggregator.version_tracker`** — `chain/epoch-manager/src/epoch_info_aggregator.rs:25` — `HashMap<ValidatorId, ProtocolVersion>`: the version each block producer voted for across the epoch, accumulated per block.

## Behavior

### Version constants (the binary's declared range)

1. `STABLE_PROTOCOL_VERSION = 86`, `NIGHTLY_PROTOCOL_VERSION = 155`, `SPICE_PROTOCOL_VERSION = 200` — `core/primitives-core/src/version.rs:624,627,633`.
2. The binary's max supported version `PROTOCOL_VERSION` is chosen at compile time: `spice` feature → 200, else `nightly` feature → 155, else 86 — `version.rs:636` — `PROTOCOL_VERSION`.
3. `MIN_SUPPORTED_PROTOCOL_VERSION = 83` — `version.rs:596`. The binary refuses to act as a validator below this; archival read-only paths clamp up to it via `clamp_to_supported_protocol_version` (`version.rs:606`) and `assert_supported_protocol_version` panics below it (`version.rs:616`).
4. `PROD_GENESIS_PROTOCOL_VERSION = 29` — `version.rs:593` — the version mainnet/testnet genesis blocks were stamped with.

### How a feature gates behavior

5. Each feature's activation version is a `const fn` lookup table: `ProtocolFeature::protocol_version` — `core/primitives-core/src/version.rs:453` — one giant `match` whose arms group features by the version they shipped in (e.g. the v85 batch at `version.rs:554-570`, `FixContractLoadingError => 86` at `version.rs:572`).
6. A feature is active iff the running version reached its activation version: `enabled` — `version.rs:587` — `protocol_version >= self.protocol_version()`. This **runtime** check against the *epoch's* protocol version is authoritative for mainnet behavior. The `#[cfg(feature=…)]` (nightly/spice) compile flags only widen the *range of versions the binary can reach* (raising `PROTOCOL_VERSION`); they do not change behavior at any given version. A feature whose activation version exceeds `STABLE_PROTOCOL_VERSION` (e.g. `FixContractLoadingCost => 129`, `ShuffleShardAssignments => 143`, `Spice => 180`, `version.rs:575-582`) is simply never `enabled` on a stable binary because the chain's version never reaches it.

### Assembling the `RuntimeConfig` for a version (base + diffs)

7. `RuntimeConfigStore::new` (`core/parameters/src/config_store.rs:89`) parses the base file `res/runtime_configs/parameters.yaml` (`BASE_CONFIG`, `config_store.rs:22`) into a `ParameterTable`, builds the version-0 config, then walks `CONFIG_DIFFS` (`config_store.rs:26`) — an ordered `&[(ProtocolVersion, diff_yaml)]` — in order.
8. For each `(version, diff)` it parses the diff, calls `params.apply_diff(diff)` to mutate the *running* table, then snapshots a fresh `RuntimeConfig` into `store[version]` (`config_store.rs:118-154`). Diffs are cumulative: each version's config is the base with every diff up to and including that version applied. The v86 diff is `(86, "86.yaml")` (`config_store.rs:64`).
9. `apply_diff` (`parameter_table.rs:526`) is checked: for each parameter it asserts the table's current value equals the diff's declared `old` (errors `NoOldValueExists` / `OldValueExists` / `WrongOldValue`, `parameter_table.rs:533-549`), then sets the `new` value (or removes the key if `new` is `None`).
10. `get_config(version)` (`config_store.rs:241`) returns the config of the greatest stored version `≤ version` via a `BTreeMap` range query — so a version with no diff of its own inherits the most recent prior config.
11. `for_chain_id` (`config_store.rs:171`) overlays chain-specific overrides: testnet re-inserts a different version-0 config (its genesis params differed); benchmarknet / congestion-control-test mutate the `PROTOCOL_VERSION` entry. Mainnet takes the plain `Self::new(None)` path.

### Proposing, voting, and activating an upgrade

12. **Proposal** is implicit: shipping a binary whose `PROTOCOL_VERSION` is higher than the live network version *is* the proposal. There is no separate on-chain proposal transaction.
13. **What a node votes for** is computed at block production: `Block::produce` is called with `upgrade_schedule.protocol_version_to_vote_for(now, next_epoch_protocol_version)` (`chain/client/src/client.rs:1174`), and that value is stored in the block header's `latest_protocol_version`.
14. `protocol_version_to_vote_for_at_date` (`core/primitives/src/upgrade_schedule.rs:130`): if the next epoch's version already ≥ the client version, or the schedule is empty, vote the client version immediately; otherwise vote the highest scheduled version whose datetime has passed, advancing **at most one version past `next_epoch_protocol_version`** per call (`upgrade_schedule.rs:149-160`). The mainnet/testnet schedules are currently empty (`core/primitives/src/version.rs:58-71` — `get_protocol_upgrade_schedule`), so production nodes vote their client version as soon as it is installed.
15. The schedule has hard invariants enforced at construction (`new_from_env_or_schedule`, `upgrade_schedule.rs:73`): the last entry must equal `client_protocol_version` (`InvalidFinalUpgrade`), datetimes must strictly increase (`InvalidDateTimeOrder`), and versions must increase by exactly one (`InvalidProtocolVersionOrder`) — so the chain can only ever step up one version at a time.
16. **Tally** happens once per epoch in `EpochManager::collect_blocks_info` (`chain/epoch-manager/src/lib.rs:516`). Votes are gathered into `version_tracker`: for each block, the sampled block producer's vote (the header's `latest_protocol_version`) is recorded once via `or_insert_with` (`chain/epoch-manager/src/epoch_info_aggregator.rs:188-191`) — so a producer's *first* observed vote in the epoch counts, and it is weighted by that producer's stake.
17. Stake is summed per voted-version (`lib.rs:547-552`), the max-stake version is picked (`lib.rs:567`), and it is adopted as the next-next epoch's version **iff** its stake strictly exceeds `protocol_upgrade_stake_threshold` of total block-producer stake (`lib.rs:570-577`); otherwise the version is unchanged. The activation therefore takes effect two epochs after the votes are cast.
18. **Non-upgrading nodes get kicked out.** Any block producer whose recorded vote is `< next_next_epoch_version` is added to `validator_kickout` with `ValidatorKickoutReason::ProtocolVersionTooOld { version, network_version }` (`lib.rs:588-600`).
19. **The threshold** `protocol_upgrade_stake_threshold` defaults to `4/5` (80%) on production via `PROTOCOL_UPGRADE_STAKE_THRESHOLD = Rational32::new_raw(4, 5)` (`core/chain-configs/src/lib.rs:49`) and the genesis serde default `8/10` (`core/chain-configs/src/genesis_config.rs:63`); test epoch configs use `3/4` (`core/primitives/src/epoch_manager.rs:307`).

### Stable vs nightly vs spice

20. Nightly and spice are not separate chains but compile-time feature flags that raise `PROTOCOL_VERSION` so the binary can reach feature activation versions ≥ 87 (nightly features at 129/143/152, spice at 180/200) — `version.rs:575-582,636`. A feature is "stabilized" by being moved from a nightly version number down to `STABLE_PROTOCOL_VERSION + 1` in the next release.

## Interactions

- **Consumes**: nothing on-chain to define versions; it reads wall-clock time (`Clock::now_utc`) for vote selection and the genesis-configured `protocol_upgrade_stake_threshold`.
- **Produces**: the `enabled(version)` predicate consumed by every component's version-gated behavior; the `Arc<RuntimeConfig>` consumed by [runtime execution](runtime-execution.md) and the [contract VM](contract-vm.md); the per-block `latest_protocol_version` vote written into block headers (see [data structures](data-structures-serialization.md)).
- **Touches**: [epoch management](epoch-validators-staking.md) owns the tally loop, the `version_tracker` aggregation, and `ProtocolVersionTooOld` kickouts; [chain & block processing](chain-block-processing.md) enforces header version validity (see Invariants). The `RuntimeConfig` parameter set is bootstrapped by [genesis & configuration](genesis-configuration.md).

## Protocol-version-gated behavior

This component *is* the gating mechanism; it has little gated behavior of its own. Two relevant points:

- **`Spice` (activates 180)** — `core/primitives-core/src/version.rs:582`. Header validation requires `header.is_spice()` once `ProtocolFeature::Spice.enabled(epoch_protocol_version)` (`chain/chain/src/chain.rs:913`). Not reachable on a stable binary (max 86).
- Header-version rejection (block whose `latest_protocol_version < epoch_protocol_version` is `InvalidProtocolVersion`, `chain/chain/src/chain.rs:905`) is now unconditional; the former `_DeprecatedRejectBlocksWithOutdatedProtocolVersions` gate (version 74) has been collapsed into always-on behavior.

Beyond that: "None known at version 86" for the versioning machinery itself.

## Invariants & failure modes

| Invariant | Enforcement |
|---|---|
| Version is monotonically non-decreasing; a header may not declare a version below its epoch's version | `chain/chain/src/chain.rs:905` — returns `Error::InvalidProtocolVersion` |
| The chain steps up by at most one version per epoch | schedule build (`upgrade_schedule.rs:113-120`) + vote-advance (`upgrade_schedule.rs:155`) |
| Upgrade requires > threshold (80%) block-producer stake voting for the new version | `chain/epoch-manager/src/lib.rs:577` |
| Validators voting below the adopted version are kicked out | `chain/epoch-manager/src/lib.rs:588-600` — `ProtocolVersionTooOld` |
| Every config diff's declared `old` must match the running table | `core/parameters/src/parameter_table.rs:532-549` — panics at store construction (`config_store.rs:125`) if a diff is malformed |
| A `RuntimeConfig` must exist for any queried version | `config_store.rs:246` — panics `"Not found RuntimeConfig for protocol version {}"` if no entry ≤ version (only version 0 is the floor) |
| Running a validator binary below `MIN_SUPPORTED_PROTOCOL_VERSION` (83) | `assert_supported_protocol_version` panics (`version.rs:616`); read paths clamp instead (`version.rs:606`) |
| Schedule's final entry must equal the client version | `upgrade_schedule.rs:95-102` — `InvalidFinalUpgrade` |

`NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE` env var (`now` / `sequential` / explicit `datetime=version` list) replaces the schedule in tests only (`upgrade_schedule.rs:78`, `parse_override` `upgrade_schedule.rs:195`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives-core/src/version.rs:11` | `ProtocolFeature` | Registry enum of all protocol-affecting changes |
| `core/primitives-core/src/version.rs:453` | `ProtocolFeature::protocol_version` | Maps each feature → activation version |
| `core/primitives-core/src/version.rs:587` | `ProtocolFeature::enabled` | Runtime gate: `version >= activation` |
| `core/primitives-core/src/version.rs:572` | match arm | `FixContractLoadingError => 86` (last stable feature) |
| `core/primitives-core/src/version.rs:596,624,627,633,636` | constants | `MIN_SUPPORTED`(83), `STABLE`(86), `NIGHTLY`(155), `SPICE`(200), `PROTOCOL_VERSION` |
| `core/primitives-core/src/version.rs:606,616` | clamp/assert helpers | Archival read clamping; validator floor enforcement |
| `core/primitives/src/version.rs:37` | `get_protocol_upgrade_schedule` | Per-chain vote schedule (empty for mainnet/testnet) |
| `core/primitives/src/upgrade_schedule.rs:130` | `protocol_version_to_vote_for_at_date` | Which version this node votes for now |
| `core/primitives/src/upgrade_schedule.rs:73` | `new_from_env_or_schedule` | Schedule invariant checks + env override |
| `core/parameters/src/config_store.rs:26` | `CONFIG_DIFFS` | Ordered list of per-version parameter diffs |
| `core/parameters/src/config_store.rs:89` | `RuntimeConfigStore::new` | Builds all per-version configs from base + diffs |
| `core/parameters/src/config_store.rs:241` | `get_config` | Returns config of greatest version ≤ requested |
| `core/parameters/src/parameter_table.rs:526` | `apply_diff` | Checked, cumulative diff application |
| `core/parameters/src/parameter_table.rs:409` | `TryFrom<&ParameterTable> for RuntimeConfig` | Materializes a `RuntimeConfig` from the table |
| `chain/client/src/client.rs:1174` | `Client::produce_block` (vote) | Writes `latest_protocol_version` into the header |
| `chain/epoch-manager/src/epoch_info_aggregator.rs:188` | `version_tracker` update | Records each block producer's first vote |
| `chain/epoch-manager/src/lib.rs:546-600` | `collect_blocks_info` | Stake-weighted tally, threshold, kickouts |
| `chain/chain/src/chain.rs:905` | header validation | Rejects under-version headers |
| `core/chain-configs/src/lib.rs:49` | `PROTOCOL_UPGRADE_STAKE_THRESHOLD` | 4/5 production upgrade threshold |

## Open questions

- The voting schedule for mainnet/testnet is empty at this commit (`get_protocol_upgrade_schedule`, `core/primitives/src/version.rs:58-71`), so v86's actual on-chain activation date is not encoded in code and cannot be derived here.
- `docs/practices/protocol_upgrade.md` and `docs/ChainSpec/Upgradability.md` were not re-read line-by-line in this pass; the tally code cites the Upgradability NEP (`chain/epoch-manager/src/lib.rs:545`) but staleness of those docs versus the implemented two-epoch-delay / 80% behavior was not verified.
