# Plan: Economics

Generation brief for `spec/economics.md`. Follow `../CONVENTIONS.md`.

## Scope
The token economics realized by the implementation: total-supply issuance/inflation,
validator reward distribution and the protocol treasury, transaction/gas fee burning
and refunds, the gas-price adjustment mechanism, and storage staking. Focus on the
*formulas and parameters as implemented*.

## Out of scope
- Reward *assignment* per validator → [epoch-validators-staking](../spec/epoch-validators-staking.md)
  (this spec covers the issuance/inflation math; that one who gets what).
- Gas accounting within execution → [runtime-execution](../spec/runtime-execution.md)
  (cross-link; fee burning mechanics there, monetary effect here).

## Code to read
- `chain/epoch-manager/src/reward_calculator.rs` — inflation rate, epoch reward,
  treasury split.
- `runtime/runtime/src/config.rs` — fee computation, refunds, gas-to-token.
- `core/parameters/src/cost.rs`, `core/parameters/src/config.rs` — fee config,
  storage-cost params, `RuntimeConfig`.
- Gas price adjustment: search `gas_price`/`min_gas_price`/`gas_price_adjustment_rate`
  in `chain/chain/` and `core/primitives`.
- `docs/Economics/Economics.md` (marked under heavy development — cross-check carefully),
  `docs/architecture/how/gas.md`, `docs/RuntimeSpec/{Fees/Fees,Refunds}.md`.

## Questions the spec must answer
- How is annual issuance / per-epoch inflation computed, and what is the max-inflation
  parameter? How does total supply change?
- How is the per-epoch validator reward pool computed and split with the protocol
  treasury account?
- How does the gas price adjust block to block (toward a target gas usage), and within
  what bounds (min gas price)?
- Which fees are *burnt* (removed from supply) vs *redistributed*, and how do gas
  refunds and the gas-refund fee (NEP-536) affect net cost?
- Storage staking: how much balance must back a byte of storage, and what enforces it?
- What are the key economic constants at the pinned version, and where do they come
  from (genesis vs runtime config)?

## Cross-component edges
- Rewards ← epoch reward calculator; fees ← runtime execution; storage cost ↔
  accounts-keys; parameters ↔ genesis-configuration.

## Relevant ProtocolFeatures
- Gas-refund fee (NEP-536), `AccountCostIncrease`, compute costs (NEP-455), any
  fee/parameter version bumps. Verify against `version.rs` and parameter diffs.
