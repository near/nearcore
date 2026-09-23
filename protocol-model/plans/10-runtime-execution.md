# Plan: Runtime & transaction/receipt execution

Generation brief for `spec/runtime-execution.md`. Follow `../CONVENTIONS.md`.

## Scope
The state-transition function: how `Runtime::apply` processes a chunk —
transaction-to-receipt conversion, the ordering of receipt processing (local,
delayed, incoming), action execution for every action type, gas burning/prepaid
accounting and refunds, validator account updates, and state finalization producing a
new state root + outgoing receipts. The heart of execution.

## Out of scope
- Contract bytecode execution internals → [contract-vm](../spec/contract-vm.md)
  (this spec covers the FunctionCall *action* and how the VM is invoked).
- Cross-shard routing/congestion of outgoing receipts →
  [cross-shard-congestion](../spec/cross-shard-congestion.md).
- Trie read/write mechanics → [state-storage](../spec/state-storage.md).
- Account/key semantics → [accounts-keys](../spec/accounts-keys.md).

## Code to read
- `runtime/runtime/AGENTS.md` (READ FIRST per repo guidance).
- `runtime/runtime/src/lib.rs` — `Runtime`, `apply`, `ApplyState`, `ApplyResult`,
  `ActionResult`, `process_transaction`, receipt processing loop, `apply_action`.
- `runtime/runtime/src/actions.rs` — every action handler.
- `runtime/runtime/src/verifier.rs` — transaction validation (signature, nonce, keys,
  balance).
- `runtime/runtime/src/config.rs` — fee/gas computation (send vs exec fees, prepaid,
  refunds).
- `runtime/runtime/src/{action_validation,receipt_manager,pipelining,prefetch}.rs`.
- `docs/RuntimeSpec/*` (ApplyingChunk, Actions, Receipts, Refunds, Transactions,
  FunctionCall, Fees) and `docs/architecture/how/{tx_receipts,gas}.md` (cross-check).

## Questions the spec must answer
- What are the exact ordered phases of `apply` (validator updates → process
  transactions → process receipts: local, then delayed, then incoming → finalize)?
  Why does the order matter?
- How is a transaction validated and converted into a receipt; where are send fees
  burnt and on which shard?
- For each action type, what does execution do and what state/receipts result?
- How is gas accounted: burnt vs used vs prepaid, exec vs send fees, and how are
  refunds (gas + deposit on failure) generated?
- How are execution outcomes and status (success/failure, return data) recorded?
- What limits a chunk's work (gas limit) and how do leftover receipts become delayed?
- How are validator accounts (rewards/stake) updated at chunk apply?

## Cross-component edges
- Invokes the VM for FunctionCall; emits receipts handled by cross-shard/congestion;
  reads/writes state via state-storage; stake proposals flow to epoch management; fees
  feed economics. Link heavily — this spec is a hub.

## Relevant ProtocolFeatures
- `FixContractLoadingCost`, `AccountCostIncrease`, `ClampOutgoingGasAdmission`,
  `GasKeys`, `StrictNonce`, `DelegateV2`, refund-related (NEP-536), `ExecutionMetadataV4`.
  Verify against `version.rs`.
