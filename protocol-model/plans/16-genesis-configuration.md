# Plan: Genesis & node configuration

Generation brief for `spec/genesis-configuration.md`. Follow `../CONVENTIONS.md`.

## Scope
How a network and a node are configured: the genesis config (initial state records,
chain params, validators), the runtime config schema and how it maps to protocol
versions, and the node/client config (network, store, sync, gc, tracked shards). The
bootstrap of the protocol's initial conditions.

## Out of scope
- How runtime config is *selected* per version →
  [protocol-versioning](../spec/protocol-versioning.md) (cross-link).
- Semantics of individual params (covered by their owning components).

## Code to read
- `core/chain-configs/src/genesis_config.rs` — `GenesisConfig`, `Genesis`, defaults,
  records loading.
- `core/chain-configs/src/` — client config, sync config types.
- `nearcore/src/config.rs` — `NearConfig`, network/store/client config assembly.
- `nearcore/src/{config_validate,dyn_config}.rs` — validation, dynamic (reloadable)
  config.
- `core/parameters/src/{config,parameter_table,config_store}.rs` — runtime config schema.
- Example configs: `nearcore/res/example-config-*.json`.
- `docs/GenesisConfig/*` (cross-check; flag staleness).

## Questions the spec must answer
- What does a genesis config contain (chain id, genesis height/time, protocol version,
  validators, shard layout, economic params) and how are initial state records applied
  to produce the genesis state root?
- How is the runtime config schema structured (fees, wasm config, account-creation,
  congestion, witness, bandwidth) and how is a version's config derived?
- What node-level config controls behavior (tracked shards, archival, gc, state-sync
  source, network) — the ones with protocol-visible effects?
- What is validated at startup and what can be changed dynamically without restart?
- How do genesis params differ between mainnet/testnet/localnet?

## Cross-component edges
- Runtime config feeds [runtime-execution](../spec/runtime-execution.md),
  [contract-vm](../spec/contract-vm.md), [economics](../spec/economics.md); version
  mapping ↔ [protocol-versioning](../spec/protocol-versioning.md); tracked-shards/gc ↔
  [chain-block-processing](../spec/chain-block-processing.md) and
  [sync](../spec/sync.md).

## Relevant ProtocolFeatures
- Genesis/config schema changes across versions; the config-by-version mechanism.
  Verify against `version.rs` and `config_store.rs`.
