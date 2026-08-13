# Runtime & transaction/receipt execution

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `runtime/runtime/src/lib.rs`, `runtime/runtime/src/actions.rs`, `runtime/runtime/src/verifier.rs`, `runtime/runtime/src/config.rs`, `runtime/runtime/src/action_validation.rs`, `runtime/runtime/src/receipt_manager.rs`, `core/primitives-core/src/version.rs`

## Role

The runtime is the state-transition function of a single shard. `Runtime::apply` takes a chunk (transactions + incoming receipts) plus the pre-state trie and produces a new state root, outgoing receipts, execution outcomes, validator proposals, and congestion/bandwidth metadata (`runtime/runtime/src/lib.rs:1717` — `Runtime::apply`, `runtime/runtime/src/lib.rs:340` — `ApplyResult`). It validates and converts transactions into receipts, executes action receipts one action at a time, accounts gas/deposits/refunds, and commits or rolls back trie state per receipt. It sits downstream of chunk/block production and epoch management (which supplies the [validator accounts update](epoch-management.md) and config), invokes the [contract VM](contract-vm.md) for `FunctionCall`, routes outgoing receipts through [cross-shard congestion control](cross-shard-congestion.md), and reads/writes the shard via [state storage](state-storage.md). Fees feed [economics](economics.md); stake proposals feed [epoch management](epoch-management.md).

## Key data structures

- **`ApplyState`** — `runtime/runtime/src/lib.rs:164` — inputs constant for the whole chunk apply: `block_height`, `shard_id`, `epoch_id`, `gas_price`, `gas_limit`, `random_seed`, `current_protocol_version`, `config: Arc<RuntimeConfig>`, `is_new_chunk`, `congestion_info`, `bandwidth_requests`. `create_receipt_id` derives child receipt IDs from a parent ID + block height + index (`runtime/runtime/src/lib.rs:220`).
- **`ApplyResult`** — `runtime/runtime/src/lib.rs:340` — outputs: `state_root`, `trie_changes`, `validator_proposals`, `outgoing_receipts`, `outcomes`, `state_changes`, `processed_receipts`, `congestion_info`, `bandwidth_requests`, `contract_updates`, `receipt_to_tx`.
- **`ActionResult`** — `runtime/runtime/src/lib.rs:364` — per-action outcome: `gas_burnt`, `gas_burnt_for_function_call`, `gas_used`, `compute_usage`, `result: Result<ReturnData, ActionError>`, `logs`, `new_receipts`, `validator_proposals`, `current_contract` (contract on the receiver observed before the action; feeds `ExecutionMetadata::V4`), `tokens_burnt`.
- **`ActionReceiptResult`** — `runtime/runtime/src/lib.rs:406` — receipt-level fold of per-action `ActionResult`s via `merge` (`runtime/runtime/src/lib.rs:439`). `merge` asserts `gas_burnt_for_function_call <= gas_burnt <= gas_used`, sums gas/compute/profile, and on error calls `set_error` (`runtime/runtime/src/lib.rs:487`) which clears queued `new_receipts`, `validator_proposals`, `tokens_burnt`, `subsidized_amount` — i.e. a failing receipt discards receipt-scoped side effects but keeps gas counters/logs.
- **`TxVerdict`** — `runtime/runtime/src/lib.rs:274` — result of tx verification: `Success(VerificationResult)`, `DepositFailed { result, error }` (gas-key path: gas charged, deposit not covered), `Failed(InvalidTxError)` (no state change).
- **`VerificationResult`** / **`AccessKeyUpdate`** — `runtime/runtime/src/lib.rs:285` / `:305` — the computed charge (`gas_burnt`, `burnt_amount`, `gas_remaining`, `receipt_gas_price`, `new_account_amount`) plus how to mutate the key (`Regular { nonce, new_allowance }` or `GasKey { new_balance, nonce_index, nonce }`). Verification functions do not mutate state; callers apply via `VerificationResult::apply` (`runtime/runtime/src/lib.rs:314`).
- **`TransactionCost`** — `runtime/runtime/src/config.rs:20` — output of `tx_cost` / `calculate_tx_cost`: `gas_burnt` (burnt on conversion), `gas_remaining` (attached to the receipt), `receipt_gas_price`, `burnt_amount`, `gas_cost`, `deposit_cost`, `total_cost`.
- **`GasRefundResult`** — `runtime/runtime/src/lib.rs:498` — `price_deficit`, `price_surplus`, `refund_penalty`, `create_account_charge`.
- **`ValidatorAccountsUpdate`** — `runtime/runtime/src/lib.rs:231` — per-epoch-boundary stake/reward info consumed by `update_validator_accounts`.
- Receipt types (`Receipt`, `ReceiptEnum`, `VersionedActionReceipt`) live in `near-primitives`; see [receipts](receipts.md).

## Behavior

### Ordered phases of `apply`

`Runtime::apply` (`runtime/runtime/src/lib.rs:1717`) runs conceptually as: (1) validator accounts, (2) transactions, (3) receipts, (4) finalize. Concretely, in order:

1. **Prefetch** transaction data (best-effort) (`runtime/runtime/src/lib.rs:1746`).
2. **Update validator accounts** if a `ValidatorAccountsUpdate` is supplied (`runtime/runtime/src/lib.rs:1752` → `update_validator_accounts` `:1599`). This happens first so reward/stake changes land before any tx/receipt reads balances, and it commits with `StateChangeCause::ValidatorAccountsUpdate` (`runtime/runtime/src/lib.rs:1692`).
3. **Load the delayed receipt queue** (`runtime/runtime/src/lib.rs:1759`).
4. **Run the bandwidth scheduler** — for every chunk including missing ones (`runtime/runtime/src/lib.rs:1767` — `run_bandwidth_scheduler`).
5. **Missing-chunk short-circuit**: if `!apply_state.is_new_chunk`, finalize immediately and return without processing any receipts (`runtime/runtime/src/lib.rs:1775` → `missing_chunk_apply_result` `:2937`), carrying congestion info and bandwidth requests forward unchanged.
6. **Build the `ReceiptSink`** from own congestion info + scheduler output, then **forward buffered receipts** from prior chunks first (`runtime/runtime/src/lib.rs:1787`, `:1795`). See [cross-shard congestion](cross-shard-congestion.md).
7. **Process transactions** → local receipts / forwarded receipts (`runtime/runtime/src/lib.rs:1798` — `process_transactions`).
8. **Process receipts** in the order local → delayed → incoming, then resolve promise-yield timeouts (`runtime/runtime/src/lib.rs:1801` — `process_receipts` `:2658`).
9. **Finalize**: `validate_apply_state_update` (`runtime/runtime/src/lib.rs:1813` / `:2723`) — persist promise-yield indices, finalize congestion (choose allowed shard), generate bandwidth requests, apply the sandbox state patch, run `state_update.finalize()`, dedup validator proposals (keeping the last per account, reversed) (`runtime/runtime/src/lib.rs:2816`), and assemble `ApplyResult`.

The local → delayed → incoming order matters: local receipts (from this chunk's transactions) and the delayed backlog are drained before new incoming cross-shard receipts, so backlog is preferred over new work and gas-limit exhaustion pushes the newest work to the delayed queue (`runtime/runtime/src/lib.rs:2670`-`2692`).

### Transaction validation and conversion (`process_transactions`, `runtime/runtime/src/lib.rs:1882`)

1. In parallel (rayon), each tx is stateless-validated via `validate_transaction` (`runtime/runtime/src/verifier.rs:109`) — action validation + `ValidatedTransaction::new` (signature, version, action limits) — and signer accounts/access-keys/gas-key nonces are prefetched into `DashMap`s (`runtime/runtime/src/lib.rs:1893`-`1977`). Expired txs are marked `InvalidTxError::Expired`.
2. Sequentially, for each tx: if `UniqueChunkTransactions` is enabled, skip any repeat of a tx hash already seen in this chunk (`runtime/runtime/src/lib.rs:1988`). Stateless-validation failures produce a failed outcome (recorded per `register_outcome`, below) and skip the tx (`runtime/runtime/src/lib.rs:1994`).
3. Compute `tx_cost` (`runtime/runtime/src/config.rs:400`); on overflow → `CostOverflow` failed outcome (`runtime/runtime/src/lib.rs:2010`).
4. Look up prefetched account / access key; missing account → `InvalidSignerId`, missing key → `AccessKeyNotFound` (`runtime/runtime/src/lib.rs:2030`-`2075`).
5. Charge: gas-key txs (nonce carries a `nonce_index`) use `verify_and_charge_gas_key_tx_ephemeral` (`runtime/runtime/src/verifier.rs:383`); regular txs use `verify_and_charge_tx_ephemeral` (`runtime/runtime/src/verifier.rs:272`). Both are pure (no state mutation) and return a `TxVerdict`.
6. On `TxVerdict::Success`, build the receipt via `Receipt::from_tx` with `receipt_gas_price` and the tx's actions (`runtime/runtime/src/lib.rs:2151`). If `receiver_id == signer_id` the receipt is a **local receipt** pushed to `local_receipts`; otherwise it is handed to `ReceiptSink::forward_or_buffer_receipt` (`runtime/runtime/src/lib.rs:2187`-`2195`). The outcome is `SuccessReceiptId`.
7. On `TxVerdict::DepositFailed` (gas-key only): gas is still burnt, a failed-with-gas outcome is emitted, no receipt is created (`runtime/runtime/src/lib.rs:2130`). On `TxVerdict::Failed`: failed outcome, tx skipped (`runtime/runtime/src/lib.rs:2199`).
8. On success, `result.apply` mutates the in-memory signer account + access key, then `set_account`/`set_access_key`/(gas-key `set_gas_key_nonce`) write them and the update commits with `StateChangeCause::TransactionProcessing` (`runtime/runtime/src/lib.rs:2241`-`2265`).

**Send fees are burnt here, on the signer's shard, at conversion time.** `calculate_tx_cost` (`runtime/runtime/src/config.rs:417`) sets `burnt = new_action_receipt send_fee + total_send_fees(actions) + signature_verification_cost`, all burnt at `current_gas_price`; `gas_remaining` (prepaid gas + prepaid send/exec fees + receipt exec fee) is attached to the receipt and purchased at `receipt_gas_price = max(current_gas_price, min_gas_purchase_price)` (`runtime/runtime/src/config.rs:459`-`465`). `sender_is_receiver` (sir) selects the cheaper send-fee column (`runtime/runtime/src/config.rs:425`).

`register_outcome` (`runtime/runtime/src/lib.rs:1856`) controls which failed-tx outcomes are recorded: with `InvalidTxGenerateOutcomes` (v83) enabled, all outcomes (including failures) are pushed; otherwise only `SuccessReceiptId` outcomes are recorded.

### Nonce and balance checks (`verifier.rs`)

- `verify_nonce` (`runtime/runtime/src/verifier.rs:212`): `Monotonic` requires `tx_nonce > current_nonce`; `Strict` (gated by `StrictNonce`, v85) requires `tx_nonce == current_nonce + 1`. Both enforce `tx_nonce < block_height * ACCESS_KEY_NONCE_RANGE_MULTIPLIER`.
- `verify_and_charge_tx_ephemeral` (`runtime/runtime/src/verifier.rs:272`): rejects gas-key access keys used without a nonce index; checks nonce, then balance (`amount < total_cost` → `NotEnoughBalance`), computes new allowance (`check_and_compute_new_allowance` `:240`), checks storage staking on the post-charge amount (`check_storage_stake` `:48`), and verifies function-call-permission constraints (`verify_function_call_permission` `:167`). With `FixAccessKeyAllowanceCharging` (v85) the allowance is only recorded in the returned `AccessKeyUpdate`; pre-fix it is mutated in place before later checks, so a later failure still decremented allowance (`runtime/runtime/src/verifier.rs:338`).
- `verify_and_charge_gas_key_tx_ephemeral` (`runtime/runtime/src/verifier.rs:383`): validates the key is a gas key, `nonce_index < num_nonces`, nonce, gas-key balance covers `gas_cost`; then checks the account balance covers `deposit_cost` and storage staking. Insufficient deposit balance yields `DepositFailed` (gas key charged only `burnt_amount`), not a hard failure.

### Action receipt execution (`apply_action_receipt`, `runtime/runtime/src/lib.rs:776`)

1. Gather each `input_data_id` from state as a `PromiseResult` and remove it (`runtime/runtime/src/lib.rs:796`); commit prior updates with `ActionReceiptProcessingStarted` (`:825`).
2. Load the receiver account; seed the receipt-level result with the `new_action_receipt` exec fee (`runtime/runtime/src/lib.rs:833`).
3. If `EnforcePerReceiptStorageProofLimit` (v86) is enabled, snapshot `recorded_storage_size_upper_bound` before the receipt so per-receipt storage-proof growth can be bounded (`runtime/runtime/src/lib.rs:838`).
4. **Execute actions in order** (`runtime/runtime/src/lib.rs:848`): for each action compute an `action_hash`, call `apply_action`, and on success validate every newly created receipt with `validate_receipt(..., NewReceipt)` (`:871`). `merge` folds the result; on the first `Err` the loop records the action index and breaks (`runtime/runtime/src/lib.rs:884`).
5. If the receipt still succeeded, re-check receiver storage staking; a shortfall sets `LackBalanceForState` (`runtime/runtime/src/lib.rs:891`-`912`).
6. **Refunds** (see below): system-predecessor receipts (refund receipts) are free — no refund generated, and a failed refund burns its deposit into `other_burnt_amount` (`runtime/runtime/src/lib.rs:929`). Otherwise `refund_unspent_gas_and_deposits` runs (`:943`).
7. **Commit or rollback**: success commits with `ReceiptProcessing`; failure calls `state_update.rollback()`, discarding all state changes from the receipt (`runtime/runtime/src/lib.rs:961`-`970`).
8. **tx_burnt_amount** = `gas_burn_price * gas_burnt − price_deficit + refund_penalty + create_account_charge + tokens_burnt` (`runtime/runtime/src/lib.rs:975`-`984`); refund receipts contribute zero burnt gas. `tokens_burnt` for the outcome equals this pre-reward value.
9. **Function-call gas reward**: `receiver_gas_reward = gas_burnt_for_function_call * burnt_gas_reward` fraction; if the receiver account exists, that reward is credited to it and subtracted from `tx_burnt_amount`, committed with `ActionReceiptGasReward` (`runtime/runtime/src/lib.rs:990`-`1021`). If the account is gone, validators keep the full amount.
10. **Output data**: if the receipt has `output_data_receivers`, either the returned `ReceiptIndex` receipt absorbs them or `Data` receipts carrying the return value are appended (`runtime/runtime/src/lib.rs:1034`-`1073`).
11. **Emit new receipts**: assign receipt IDs; instant receipts go to the `instant_receipts` queue (processed immediately after the current receipt, `process_receipt_and_instant_receipts` `:2619`), others are forwarded/buffered via the `ReceiptSink` (`runtime/runtime/src/lib.rs:1104`-`1116`).
12. **Build the outcome** (`runtime/runtime/src/lib.rs:1122`): status is `SuccessReceiptId` / `SuccessValue` / `Failure(ActionError)`. Metadata is `ExecutionMetadata::V4` (per-action `Vec<AccountContract>`, padded to action count) when `ExecutionMetadataV4` (v85) is enabled, else `V3` (profile only) (`runtime/runtime/src/lib.rs:1134`-`1141`).

### Per-action execution (`apply_action`, `runtime/runtime/src/lib.rs:523`)

`apply_action` seeds `ActionResult` with the action's `exec_fee` (gas/compute, `runtime/runtime/src/lib.rs:540`) and captures `current_contract` before running. It then runs `check_account_existence` (`runtime/runtime/src/actions.rs:824`) and `check_actor_permissions` (`runtime/runtime/src/actions.rs:776`); either failure returns early with the error set. Implicit account creation is allowed only when the action is the sole action and not a refund (`runtime/runtime/src/lib.rs:549`). Dispatch by action:

| Action | Handler | Effect |
| --- | --- | --- |
| `CreateAccount` | `action_create_account` `runtime/runtime/src/actions.rs:155` | Creates an empty account; enforces top-level length / sub-account naming rules. |
| `DeployContract` | `action_deploy_contract` `runtime/runtime/src/actions.rs:297` | Clears old contract storage usage, sets local code hash, updates storage usage, records + precompiles code. |
| `DeployGlobalContract` | `action_deploy_global_contract` (global_contracts) | Publishes a global contract; burnt storage counted into outcome when `IncludeDeployGlobalContractOutcomeBurntStorage` (v83) enabled. |
| `UseGlobalContract` | `action_use_global_contract` | Points the account at a global contract. |
| `DeterministicStateInit` | `action_deterministic_state_init` | Creates/initializes a deterministic account. |
| `FunctionCall` | `action_function_call` `runtime/runtime/src/function_call.rs:31` | Resolves the contract (`RuntimeContractIdentifier::resolve`), fetches the prepared contract from the pipeline, invokes the [contract VM](contract-vm.md); records burnt/used gas, logs, new receipts. |
| `Transfer` | `action_transfer_or_implicit_account_creation` `runtime/runtime/src/lib.rs:2887` | Credits balance, or creates an implicit (NEAR/ETH/deterministic) account; gas refunds may route to a gas key / allowance. |
| `Stake` | `action_stake` `runtime/runtime/src/actions.rs:47` | Emits a `ValidatorStake` proposal after checking minimum stake and moving amount↔locked. |
| `AddKey`/`DeleteKey` | `action_add_key`/`action_delete_key` (access_keys) | Adds/removes an access key (incl. gas keys). |
| `DeleteAccount` | `action_delete_account` `runtime/runtime/src/actions.rs:343` | Refunds remaining balance to beneficiary, burns gas-key balances, removes the account; requires zero locked stake (`check_actor_permissions`). |
| `Delegate`/`DelegateV2` | `apply_delegate_action` `runtime/runtime/src/actions.rs:487` | Verifies the inner signed delegate action and spawns a receipt for its inner actions (meta-transactions, NEP-366). |
| `TransferToGasKey`/`WithdrawFromGasKey` | `action_transfer_to_gas_key`/`action_withdraw_from_gas_key` (access_keys) | Move balance to/from a gas key. |

### Gas accounting and refunds (`refund_unspent_gas_and_deposits`, `runtime/runtime/src/lib.rs:1166`)

Since NEP-536 (the now-deprecated always-on `_DeprecatedReducedGasRefunds`, activated at v78) gas is purchased at one price for the whole execution; price changes do not create refunds. The function computes `prepaid_gas` (prepaid attached gas + prepaid send fees) and `prepaid_exec_gas` (prepaid exec fees + the receipt exec fee). On failure, the full deposit is refunded and `gross_gas_refund = prepaid − gas_burnt`; on success `gross_gas_refund = prepaid − gas_used` (`runtime/runtime/src/lib.rs:1185`-`1198`). A 5% NEP-536 penalty (`gas_penalty_for_gas_refund`) is deducted (`runtime/runtime/src/lib.rs:1201`). A deposit-refund receipt (`Receipt::new_balance_refund`) and/or a gas-refund receipt (`Receipt::new_gas_refund`, which also credits allowance) are pushed to `new_receipts` (`runtime/runtime/src/lib.rs:1284`-`1298`).

**`AccountCostIncrease` (v85)** changes the gas-price model: `gas_burn_price = min(gas_purchase_price, apply_state.gas_price)` instead of the current price (`runtime/runtime/src/lib.rs:920`-`927`). Post-feature, the price surplus (purchase > burn) is refunded as `burned_gas_refund`, and creating an account triggers `create_account_charge` — an extra charge up to `account_creation_charge` taken out of the burned-gas refund (`runtime/runtime/src/lib.rs:1234`-`1280`). Pre-feature, the surplus was instead folded into `tx_burnt_amount` (`runtime/runtime/src/lib.rs:978`).

### Execution outcome / status

Every processed transaction and receipt yields an `ExecutionOutcomeWithId` (`runtime/runtime/src/lib.rs:1143`, tx side `:2159`) recording `status`, `logs`, `receipt_ids`, `gas_burnt`, `compute_usage`, `tokens_burnt`, `executor_id`, `metadata`. Status is one of `SuccessValue`, `SuccessReceiptId`, or `Failure(TxExecutionError)`.

### Gas/compute limit and delayed receipts

`process_receipts` sets `compute_limit = gas_limit` (`runtime/runtime/src/lib.rs:2668`; there is no separate compute limit yet). In each of `process_local_receipts`/`process_delayed_receipts`/`process_incoming_receipts`, before processing a receipt the loop checks `total.compute >= compute_limit || trie.check_proof_size_limit_exceed()`; if exceeded, the receipt is pushed onto the delayed queue instead of executing (local/incoming, `runtime/runtime/src/lib.rs:2385`, `:2575`) or the delayed loop breaks (`:2463`). Incoming receipts are `validate_receipt(..., ExistingReceipt)`-checked before either path so invalid receipts are never stored as delayed (`runtime/runtime/src/lib.rs:2568`). Whichever limit stopped the chunk is recorded in `CHUNK_RECEIPTS_LIMITED_BY` (`runtime/runtime/src/lib.rs:2699`).

### Validator accounts update (`update_validator_accounts`, `runtime/runtime/src/lib.rs:1599`)

For each validator with stake info: add reward to `locked`, assert the staking invariant `locked >= max_of_stakes`, compute `return_stake = locked − max(max_of_stakes, last_proposal)`, move it from `locked` back to `amount` (`runtime/runtime/src/lib.rs:1604`-`1652`). The protocol treasury account gets its reward if it did not already receive one as a validator (`runtime/runtime/src/lib.rs:1664`). Commits with `ValidatorAccountsUpdate`.

## Interactions

- **Consumes**: pre-state `Trie`, `ApplyState`, incoming receipts, `SignedValidPeriodTransactions`, optional `ValidatorAccountsUpdate`, `EpochInfoProvider` (min stake, shard layout, chain id), `RuntimeConfig`. Config/fees come from [economics](economics.md); the validator update comes from [epoch management](epoch-management.md).
- **Produces**: new state root + `TrieChanges` → [state storage](state-storage.md); outgoing receipts (via `ReceiptSink`) and bandwidth requests → [cross-shard congestion control](cross-shard-congestion.md); `ValidatorStake` proposals → [epoch management](epoch-management.md); execution outcomes → chunk outcome root / RPC.
- **Invokes**: the [contract VM](contract-vm.md) for `FunctionCall` (contract preparation is pipelined, `runtime/runtime/src/pipelining.rs`; `receipt_manager.rs` collects VM-created receipts). Transaction/receipt data structures are defined in [receipts](receipts.md) and [accounts & keys](accounts-keys.md).

## Protocol-version-gated behavior

Verified against `core/primitives-core/src/version.rs` in this tree (`STABLE_PROTOCOL_VERSION = 86`, `MIN_SUPPORTED_PROTOCOL_VERSION = 83`). Features listed in the plan were re-checked; several differ from an older baseline.

| Feature | Activates | Effect on this component |
| --- | --- | --- |
| `EnforcePerReceiptStorageProofLimit` | **86** (`version.rs:576`) | Snapshots per-receipt storage-proof upper bound before executing a receipt's actions (`runtime/runtime/src/lib.rs:838`). This is the sole PV-86 feature on 2.13.0. |
| `AccountCostIncrease` | 85 (`version.rs:574`) | `gas_burn_price = min(purchase, current)`, price-surplus refund, and `create_account_charge` (`runtime/runtime/src/lib.rs:920`, `:1234`). |
| `ClampOutgoingGasAdmission` | 85 (`version.rs:573`) | Clamps outgoing-receipt gas admission in the receipt sink (`runtime/runtime/src/congestion_control.rs:443`). See [cross-shard congestion](cross-shard-congestion.md). |
| `GasKeys` | 85 (`version.rs:562`) | Enables gas-key transactions/actions and the gas-key charge path (`verify_and_charge_gas_key_tx_ephemeral`). |
| `DelegateV2` | 85 (`version.rs:575`) | `Action::DelegateV2` (gas-key-aware meta-transactions) handled by `apply_delegate_action`. |
| `StrictNonce` | 85 (`version.rs:566`) | `NonceMode::Strict` enforced in `verify_nonce` (`runtime/runtime/src/verifier.rs:224`). |
| `ExecutionMetadataV4` | 85 (`version.rs:571`) | Outcome carries per-action `Vec<AccountContract>` (`runtime/runtime/src/lib.rs:1135`). |
| `FixAccessKeyAllowanceCharging` | 83 (`version.rs:551`) | Allowance no longer mutated before later checks (`runtime/runtime/src/verifier.rs:338`). |
| `FixDeleteAccountGlobalContractStorageUsage` | 85 (`version.rs:560`) | `DeleteAccount` subtracts global-contract identifier storage (`runtime/runtime/src/actions.rs:355`). |
| `ExcludeExistingCodeFromWitnessForCodeLen` | 83 (`version.rs:550`) | Code length read from trie value-ref instead of reading code (`runtime/runtime/src/actions.rs:440`). |
| `InvalidTxGenerateOutcomes` | 83 (`version.rs:549`) | Failed-tx outcomes recorded (`runtime/runtime/src/lib.rs:1861`). |
| `IncludeDeployGlobalContractOutcomeBurntStorage` | 83 (`version.rs:552`) | Global-contract deploy burnt storage counted in outcome. |
| `EthImplicitGlobalContract` | 83 (`version.rs:556`) | ETH implicit accounts use a global contract (`runtime/runtime/src/actions.rs:238`). |
| `UniqueChunkTransactions` | 85 (`version.rs:568`) | Duplicate tx hashes skipped within a chunk (`runtime/runtime/src/lib.rs:1988`). |
| `YieldWithId` | 85 (`version.rs:570`) | Yield/resume `yield_id`↔`data_id` mapping cleanup (`runtime/runtime/src/lib.rs:1460`). |

NEP-536 reduced gas refunds is **always on** here: the corresponding flag is `_DeprecatedReducedGasRefunds` at v78 (`version.rs:545`), below `MIN_SUPPORTED_PROTOCOL_VERSION = 83`, so `refund_unspent_gas_and_deposits` always uses the reduced-refund model.

**Feature-name caveats vs. the plan's list.** `FixContractLoadingCost` exists but is **nightly (129)** (`version.rs:579`), not active at stable 86; the plan's "`FixContractLoadingCost`" is not a stable-path behavior here. There is **no** `FixContractLoadingError` feature in this tree. The refund-related NEP-536 feature is deprecated/always-on, not a live gate.

## Invariants & failure modes

- **Gas ordering**: `merge` asserts `gas_burnt_for_function_call <= gas_burnt <= gas_used` per action (`runtime/runtime/src/lib.rs:440`).
- **Failed receipt atomicity**: a receipt whose result is `Err` triggers `state_update.rollback()`, so no state changes persist except the outcome/gas accounting (`runtime/runtime/src/lib.rs:967`). `set_error` additionally clears queued receipts, proposals, and burnt/subsidized amounts (`runtime/runtime/src/lib.rs:487`).
- **Staking invariant**: `update_validator_accounts` returns a fatal `StorageInconsistentState` if `locked < max_of_stakes` (`runtime/runtime/src/lib.rs:1617`).
- **Invalid txs make progress, not failure**: a chunk with invalid transactions is not rejected; the offending txs are skipped during conversion, polluting the chain with junk but keeping the shard live (`runtime/runtime/src/lib.rs:1706` doc; skip sites at `:1994`, `:2199`).
- **Refund receipts are free**: system-predecessor receipts burn zero gas; a failed refund burns its deposit into `other_burnt_amount` rather than refunding (`runtime/runtime/src/lib.rs:929`, `:972`).
- **Delayed receipts must stay valid**: a delayed receipt that fails `validate_receipt` on dequeue is treated as `StorageInconsistentState` (`runtime/runtime/src/lib.rs:2500`).
- **No balance-mutation on tx-verify error**: `verify_and_charge_*_ephemeral` are pure; the only historical exception (allowance) is fixed by `FixAccessKeyAllowanceCharging` (v85).
- **Storage staking**: receiver must cover storage after execution or the receipt fails with `LackBalanceForState` (`runtime/runtime/src/lib.rs:897`); zero-balance accounts (≤ `ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT = 770` bytes) are exempt (`runtime/runtime/src/verifier.rs:25`, `:88`).
- **Compute/storage limit**: chunk work bounded by `compute_limit` (= gas limit) and the storage-proof size limit; overflow pushes work to the delayed queue (`runtime/runtime/src/lib.rs:2385`).

## Code anchors

| Location | Symbol | What happens here |
| --- | --- | --- |
| `runtime/runtime/src/lib.rs:1717` | `Runtime::apply` | Top-level chunk state transition; ordered phases. |
| `runtime/runtime/src/lib.rs:1775` | `apply` missing-chunk branch | Short-circuit for missing chunks. |
| `runtime/runtime/src/lib.rs:1599` | `update_validator_accounts` | Phase 1: rewards/return stake/treasury. |
| `runtime/runtime/src/lib.rs:1882` | `process_transactions` | Phase 2: validate, charge, convert to receipts. |
| `runtime/runtime/src/lib.rs:1856` | `register_outcome` | Whether failed-tx outcomes are recorded (`InvalidTxGenerateOutcomes`). |
| `runtime/runtime/src/lib.rs:2658` | `process_receipts` | Phase 3: local → delayed → incoming + yield timeouts. |
| `runtime/runtime/src/lib.rs:2357` | `process_local_receipts` | Drains local receipts; overflow → delayed. |
| `runtime/runtime/src/lib.rs:2441` | `process_delayed_receipts` | Drains delayed backlog. |
| `runtime/runtime/src/lib.rs:2541` | `process_incoming_receipts` | Validates + executes incoming; overflow → delayed. |
| `runtime/runtime/src/lib.rs:1303` | `process_receipt` | Dispatch by receipt kind (data/action/yield/resume/global-contract). |
| `runtime/runtime/src/lib.rs:776` | `apply_action_receipt` | Execute one action receipt end-to-end. |
| `runtime/runtime/src/lib.rs:523` | `apply_action` | Per-action fee seeding, checks, dispatch. |
| `runtime/runtime/src/lib.rs:1166` | `refund_unspent_gas_and_deposits` | Gas + deposit refunds and NEP-536 penalty. |
| `runtime/runtime/src/lib.rs:2723` | `validate_apply_state_update` | Phase 4: finalize state, congestion, bandwidth, proposals. |
| `runtime/runtime/src/lib.rs:2986` | `resolve_promise_yield_timeouts` | Times out yielded receipts. |
| `runtime/runtime/src/config.rs:400` | `tx_cost` / `calculate_tx_cost` | Send fees burnt vs. attached prepaid gas. |
| `runtime/runtime/src/config.rs:279` | `exec_fee` | Per-action execution fee (gas + compute). |
| `runtime/runtime/src/config.rs:71` | `total_send_fees` | Per-action send fees (sir-aware). |
| `runtime/runtime/src/verifier.rs:272` | `verify_and_charge_tx_ephemeral` | Regular-tx nonce/balance/allowance/storage checks. |
| `runtime/runtime/src/verifier.rs:383` | `verify_and_charge_gas_key_tx_ephemeral` | Gas-key charge path; `DepositFailed`. |
| `runtime/runtime/src/verifier.rs:212` | `verify_nonce` | Monotonic vs. strict nonce. |
| `runtime/runtime/src/verifier.rs:540` | `validate_receipt` | Receipt size/id/action validation (New vs. Existing mode). |
| `runtime/runtime/src/actions.rs:824` | `check_account_existence` | Account existence rules per action. |
| `runtime/runtime/src/actions.rs:776` | `check_actor_permissions` | Actor==account permission checks; no-stake for delete. |
| `runtime/runtime/src/actions.rs:47` | `action_stake` | Stake proposal + amount↔locked move. |
| `runtime/runtime/src/actions.rs:343` | `action_delete_account` | Delete: beneficiary refund, gas-key burn, storage checks. |
| `runtime/runtime/src/actions.rs:487` | `apply_delegate_action` | Meta-transaction inner-action receipt spawn. |
| `runtime/runtime/src/function_call.rs:31` | `action_function_call` | VM invocation for `FunctionCall`. |
| `core/primitives-core/src/version.rs:458` | `ProtocolFeature::protocol_version` | Feature → activation version mapping. |

## Open questions

None.
