# Plan: Epoch management, validators & staking

Generation brief for `spec/epoch-validators-staking.md`. Follow `../CONVENTIONS.md`.

## Scope
Epoch lifecycle and the validator economy: how epochs are defined and transitioned,
how validators are selected from stake proposals, block/chunk producer assignment,
reward computation, kickouts and slashing, shard assignment, and the protocol-version
vote tally.

## Out of scope
- Doomslug approvals → [consensus-finality](../spec/consensus-finality.md).
- Reward *issuance economics* (inflation curve) → [economics](../spec/economics.md)
  (cross-link; reward *mechanics* live here).
- Resharding shard-layout migration → [sharding-chunks](../spec/sharding-chunks.md).

## Code to read
- `chain/epoch-manager/src/lib.rs` — `EpochManager`, `EpochManagerHandle`,
  `EpochManagerAdapter`, epoch finalization.
- `chain/epoch-manager/src/validator_selection.rs` — proposal aggregation, selection.
- `chain/epoch-manager/src/reward_calculator.rs` — reward + protocol treasury.
- `chain/epoch-manager/src/shard_assignment/` and `shard_tracker.rs`.
- `core/primitives/src/epoch_info.rs`, `core/primitives/src/epoch_manager.rs` —
  EpochInfo, EpochConfig, validator structures.
- `core/primitives/src/types.rs` — `ValidatorStake`, `EpochId`.
- `docs/architecture/how/epoch.md`, `docs/ChainSpec/EpochAndStaking/*` (cross-check).

## Questions the spec must answer
- What defines an epoch boundary and how is the next epoch's info computed (from which
  block)? What is `EpochId`?
- How are validators selected from staking proposals (min stake ratio, seats,
  block vs chunk producer vs chunk validator roles)?
- How is the block/chunk producer for a given height+shard determined?
- How are rewards computed and distributed; what is the protocol treasury cut?
- How do kickouts work (low uptime / missed chunks), and what is slashing's current state?
- How is the shard layout for an epoch chosen, and how are producers assigned to shards
  (including sticky assignment / shuffling features)?
- How is the protocol-version upgrade vote tallied at epoch transition?

## Cross-component edges
- Producer schedule → consensus & sharding. Stake changes come from Stake actions in
  [runtime-execution](../spec/runtime-execution.md). Rewards feed
  [economics](../spec/economics.md). Version tally ↔
  [protocol-versioning](../spec/protocol-versioning.md).

## Relevant ProtocolFeatures
- `EarlyKickout`, `StickyReshardingValidatorAssignment`, `ShuffleShardAssignments`,
  chunk-validator-seat features. Verify against `version.rs`.
