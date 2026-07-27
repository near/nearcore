# Plan: Consensus & finality (Doomslug)

Generation brief for `spec/consensus-finality.md`. Follow `../CONVENTIONS.md`.

## Scope
The Doomslug finality gadget and block-production readiness: how approvals are
collected, when a block producer is allowed to produce, how skips/endorsements work,
and how finality of the previous block is established. Block-header-level approval
validation.

## Out of scope
- Chunk production/validation → [sharding-chunks](../spec/sharding-chunks.md),
  [stateless-validation](../spec/stateless-validation.md).
- Who the block producer *is* for a height → [epoch-validators-staking](../spec/epoch-validators-staking.md).
- The block-processing pipeline that applies an accepted block →
  [chain-block-processing](../spec/chain-block-processing.md).

## Code to read
- `chain/chain/src/doomslug.rs` — `Doomslug`, `DoomslugThresholdMode`, approval
  tracking, timers, `ready_to_produce_block`, finality computation.
- `chain/chain/AGENTS.md` — orientation.
- `core/primitives/src/block_header.rs` and `core/primitives/src/block.rs` —
  `Approval`, `ApprovalInner` (Endorsement vs Skip), approval signatures,
  `last_final_block`, `last_ds_final_block`.
- `chain/chain/src/validate.rs` — approval/signature validation during block validation.
- Approval production/sending in `chain/client/src/client.rs` (search `Approval`,
  `send_approval`).
- `docs/ChainSpec/Consensus.md` (cross-check; flag staleness).

## Questions the spec must answer
- What is the Doomslug rule for finality? When is a block "doomslug-final" vs "final"?
- What are endorsements vs skip messages, and how does an approval encode which?
- What stake threshold (and which threshold mode) is required to produce / to finalize?
- The timing/state machine: when does a producer become ready (have enough approvals
  or timed out)? What are the relevant timers?
- How are `last_final_block` and `last_ds_final_block` computed and stored in a header?
- How are approvals validated (signer is the expected approver, stake weighting)?
- Failure modes: not enough approvals, equivocation, invalid approval signature.

## Cross-component edges
- Block producer schedule ← epoch management. Approvals are gossiped via networking.
  Accepted blocks flow into the chain pipeline. Link, don't redescribe.

## Relevant ProtocolFeatures
- Check `version.rs` for anything touching approvals/finality/optimistic blocks
  (e.g. optimistic block production — see `docs/architecture/how/optimistic_block.md`).
