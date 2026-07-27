# Plan: Protocol versioning & upgrades

Generation brief for `spec/protocol-versioning.md`. Follow `../CONVENTIONS.md`.

## Scope
How protocol versions are defined, how features gate behavior, how the runtime
config is selected per version, and how upgrades are voted in and activated.

## Out of scope
- The behavior of individual gated features (each owning component covers its own).
- Epoch transition mechanics beyond the version-vote tally → see
  [epoch-validators-staking](../spec/epoch-validators-staking.md).

## Code to read
- `core/primitives-core/src/version.rs` — `ProtocolFeature` enum, `PROTOCOL_VERSION`,
  `STABLE`/`NIGHTLY`/`SPICE`/`MIN_SUPPORTED` constants, `protocol_version()`,
  `enabled()`, upgrade schedule helpers.
- `core/primitives/src/version.rs` — NEAR-specific version glue,
  `get_protocol_upgrade_schedule`.
- `core/parameters/src/config_store.rs` — `RuntimeConfigStore`, config-by-version.
- `core/parameters/src/parameter_table.rs` — versioned parameter parsing.
- `docs/practices/protocol_upgrade.md` and `docs/ChainSpec/Upgradability.md` (cross-check;
  flag staleness).
- Epoch-side vote tally: search `protocol_version` in `chain/epoch-manager/src/`.

## Questions the spec must answer
- What exactly is a `ProtocolVersion`, and what are the current stable/min/nightly values?
- How does a feature get gated — the `#[cfg(feature=…)]` vs runtime `enabled(version)`
  distinction, and which is authoritative for mainnet behavior?
- How is the `RuntimeConfig` for a given protocol version assembled (base + diffs)?
- How is an upgrade proposed, voted, and activated at an epoch boundary? What fraction
  is required? What happens to nodes that don't upgrade?
- What is the relationship between nightly/spice versions and stable?

## Cross-component edges
- Every component's "Protocol-version-gated behavior" section depends on this one;
  link back here from the index. Vote tally → epoch management.

## Relevant ProtocolFeatures
- This spec enumerates the *mechanism*; do not list every feature. Reference the enum
  as the registry.
