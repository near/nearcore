# Runtime execution (state-transition function)

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `runtime/runtime/src/lib.rs`, `runtime/runtime/src/actions.rs`, `runtime/runtime/src/verifier.rs`, `runtime/runtime/src/config.rs`, `runtime/runtime/src/function_call.rs`, `runtime/runtime/src/access_keys.rs`

## Role

This is the chunk-level state-transition function. `Runtime::apply` takes a shard's pre-state trie, the validator-accounts update for an epoch boundary, the chunk's transactions, and the incoming cross-shard receipts, and produces a new state root plus the side-effects of executing the chunk (outgoing receipts, execution outcomes, validator proposals, congestion info, bandwidth requests). It is the heart of execution: it validates and converts transactions into receipts, charges gas/fees, executes every action in every ready receipt in a load-bearing order, generates gas/deposit refunds, and finalizes the trie. It is invoked once per shard per block by chain block processing ([chain-block-processing](chain-block-processing.md)). It invokes the contract VM for `FunctionCall` ([contract-vm](contract-vm.md)), forwards/buffers outgoing receipts through congestion control ([cross-shard-congestion](cross-shard-congestion.md)), reads/writes the trie via `TrieUpdate` ([state-storage](state-storage.md)), and feeds stake proposals to epoch management ([epoch-validators-staking](epoch-validators-staking.md)).

## Key data structures

- **`ApplyState`** — `runtime/runtime/src/lib.rs:164` — the immutable per-chunk context: `block_height`, `shard_id`, `epoch_id`, `gas_price`, `gas_limit` (`Option<Gas>`, `None` = unlimited), `random_seed`, `current_protocol_version`, `config: Arc<RuntimeConfig>`, `is_new_chunk`, the `BlockCongestionInfo` map of all shards, and the `BlockBandwidthRequests`. `create_receipt_id` (`:220`) derives child receipt IDs from a parent receipt ID + block height + index.
- **`ValidatorAccountsUpdate`** — `runtime/runtime/src/lib.rs:231` — only present at the first block of an epoch: `stake_info` (max stake over last 3 epochs per validator), `validator_rewards`, `last_proposals`, optional `protocol_treasury_account_id`.
- **`ApplyResult`** — `runtime/runtime/src/lib.rs:340` — outputs: `state_root`, `trie_changes`, `validator_proposals`, `outgoing_receipts`, `outcomes` (`Vec<ExecutionOutcomeWithId>`), `state_changes`, `processed_receipts`, `processed_yield_timeouts`, `proof` (recorded storage for stateless validation), `delayed_receipts_count`, `congestion_info`, `bandwidth_requests`, `contract_updates`, `receipt_to_tx`.
- **`ActionResult`** — `runtime/runtime/src/lib.rs:364` — per-action result accumulator: `gas_burnt`, `gas_burnt_for_function_call`, `gas_used`, `compute_usage`, `result: Result<ReturnData, ActionError>`, `logs`, `new_receipts`, `validator_proposals`, `profile`, `current_contract`, `tokens_burnt`, `subsidized_amount`.
- **`ActionReceiptResult`** — `runtime/runtime/src/lib.rs:406` — receipt-level aggregate built by folding `ActionResult`s via `merge` (`:439`). `merge` asserts `gas_burnt_for_function_call <= gas_burnt <= gas_used`, sums all gas/compute counters, and on a per-action error calls `set_error` (`:487`), which records the error and **discards** the receipt's queued receipts, validator proposals, `tokens_burnt`, and `subsidized_amount` (but keeps gas counters, logs, profile).
- **`VerificationResult` / `TxVerdict`** — `runtime/runtime/src/lib.rs:285` / `:274` — outcome of tx verify+charge. `TxVerdict` is `Success(VerificationResult)`, `DepositFailed { result, error }` (gas-key only: gas charged, deposit not), or `Failed(InvalidTxError)` (no state change). `VerificationResult::apply` (`:314`) writes the new balance and access-key/gas-key update.
- **`GasRefundResult`** — `runtime/runtime/src/lib.rs:498` — `price_deficit`, `price_surplus`, `refund_penalty`, `create_account_charge`.
- **`TransactionCost`** — `runtime/runtime/src/config.rs:20` — output of `tx_cost`: `gas_burnt`/`compute_burnt` (burnt at conversion), `gas_remaining` (attached to the receipt), `receipt_gas_price`, `burnt_amount`, `gas_cost`, `deposit_cost`, `total_cost`.
- **`Receipt` / `ReceiptEnum`** — `core/primitives/src/receipt.rs:73` / `:566` — task to execute. Variants: `Action`/`ActionV2`, `Data`, `PromiseYield`/`PromiseYieldV2`, `PromiseResume`, `GlobalContractDistribution`. Action receipts carry `signer_id`, `signer_public_key`, `gas_price`, `input_data_ids`, `output_data_receivers`, `actions`. `is_instant_receipt` (`:473`) is true for `PromiseYield` and for action receipts that are a single `DeleteAccount` with no input data.
- **`ApplyProcessingState` / `ApplyProcessingReceiptState`** — `runtime/runtime/src/lib.rs:3072` / `:3147` — mutable per-`apply` working set: `state_update: TrieUpdate`, `total: TotalResourceGuard` (running gas+compute), `stats`, `outcomes`, `local_receipts`, `instant_receipts`, `incoming_receipts`, `delayed_receipts`, `pipeline_manager`, `receipt_to_tx`.

## Behavior

### Top-level ordered phases of `apply`

`Runtime::apply` (`runtime/runtime/src/lib.rs:1702`) runs strictly in this order; the order is consensus-critical because receipt IDs, gas-limit cutoffs, and committed outcomes all depend on it:

1. **Build working state & prefetch** — construct `ApplyProcessingState`, record `transactions_num`/`incoming_receipts_num`, kick off the transaction-data prefetcher (`:1725`–`:1734`).
2. **Update validator accounts** — only if `validator_accounts_update` is `Some` (epoch's first block); see below (`:1737`).
3. **Load the delayed-receipt queue** from state (`:1744`).
4. **Run the bandwidth scheduler** for every chunk including missing ones, producing outgoing-bandwidth limits (`:1752`, [cross-shard-congestion](cross-shard-congestion.md)).
5. **Missing-chunk early exit** — if `!apply_state.is_new_chunk`, finalize immediately with no transaction/receipt processing via `missing_chunk_apply_result` (`:1760`, `:2881`); congestion info and bandwidth requests are copied through unchanged.
6. **Construct `ReceiptSink`** with this shard's own congestion info and the bandwidth limits, then **forward buffered receipts** left over from previous chunks first (`:1772`, `:1780`).
7. **Process transactions** → local receipts + outgoing receipts (`:1783`).
8. **Process receipts**: local, then delayed, then incoming, then resolve promise-yield timeouts (`:1786`).
9. **Validate & apply the state update**: finalize delayed-receipt/congestion/bandwidth state, finalize the trie, dedup proposals, build `ApplyResult` (`:1798`).

### Phase 2 — validator account updates

`update_validator_accounts` (`runtime/runtime/src/lib.rs:1584`) iterates `stake_info`: for each validator account it adds any `validator_rewards` to `locked`, asserts the staking invariant `account.locked() >= max_of_stakes` (else a fatal `StorageInconsistentState`), computes `return_stake = locked - max(max_of_stakes, last_proposal)`, moves that from `locked` back to `amount`, and writes the account. If the protocol-treasury account is not itself a validator, its reward is credited to `amount`. The phase commits under `StateChangeCause::ValidatorAccountsUpdate` (`:1677`). Stake proposals themselves flow out as `validator_proposals` from `Stake` actions, deduped at finalize.

### Phase 7 — transaction validation and conversion to receipts

`process_transactions` (`runtime/runtime/src/lib.rs:1853`):

1. **Parallel pre-pass** (`rayon::join`, `:1864`): stateless `validate_transaction` per tx (signature/action limits, `:1878`, `verifier.rs:108`) while concurrently prefetching signer accounts, access keys, and gas-key nonces into `DashMap`s.
2. **Duplicate skip**: under `UniqueChunkTransactions` (enabled) a tx hash seen earlier in the same chunk is skipped — because the tx hash is also the outcome id, processing it twice would commit conflicting outcomes (`:1952`–`:1963`).
3. **Per-tx sequential pass**: failed stateless validations become a `failed` outcome with no receipt (`:1965`). Otherwise compute `tx_cost` (`:1977`, `config.rs:400`). Missing signer account → `InvalidSignerId`; missing access key → `AccessKeyNotFound` (`:1994`–`:2030`).
4. **Verify & charge**: gas-key txs (`nonce().nonce_index().is_some()`) go through `verify_and_charge_gas_key_tx_ephemeral`; regular txs through `verify_and_charge_tx_ephemeral` (`:2032`–`:2076`). These functions are pure: they return a `TxVerdict` and mutate nothing.
5. **Apply the verdict** (`:2079`):
   - `Success`: build the receipt with `Receipt::from_tx` (`:2101`) at `result.receipt_gas_price`, build a `SuccessReceiptId` outcome. **If `receiver_id == signer_id` (sender-is-receiver), push to `local_receipts`; otherwise forward/buffer it as an outgoing receipt** (`:2137`–`:2145`).
   - `DepositFailed` (gas key only): a failed outcome that still records `cost.gas_burnt`/`cost.burnt_amount` — gas was charged to the gas key, deposit was not (`:2080`).
   - `Failed`: a failed outcome, no receipt, no state change (`:2149`).
6. **Commit state** for each successfully charged tx (`set_account`, `set_access_key`, optional `set_gas_key_nonce`) under `StateChangeCause::TransactionProcessing` (`:2187`–`:2211`).

Where send fees are burnt and on which shard: `tx_cost`/`calculate_tx_cost` (`config.rs:400`/`:417`) computes `burnt` = the `new_action_receipt` send fee + `total_send_fees(actions)` + signature-verification cost; this is burnt at the **current** gas price (`burnt_amount`, `:459`) on the **signer's shard** during conversion. The receipt's attached gas (`gas_remaining`) is priced at `receipt_gas_price = max(current_gas_price, config.min_gas_purchase_price)` (`:464`) and is burnt/refunded later on the receiver's shard.

### Phase 8 — receipt processing order

`process_receipts` (`runtime/runtime/src/lib.rs:2602`) sets `compute_limit = gas_limit` (compute limit currently equals gas limit, `:2612`) and processes in this order, each stopping when `total.compute >= compute_limit` or the storage-proof size limit is hit:

1. **Local receipts** (`process_local_receipts`, `:2301`) — generated this chunk by sender-is-receiver txs, processed in tx order. Over-limit ones are pushed onto the **delayed** queue (`:2332`).
2. **Delayed receipts** (`process_delayed_receipts`, `:2385`) — the backlog from prior chunks, popped FIFO from the trie queue; each is re-validated in `ExistingReceipt` mode before execution (`:2444`).
3. **Incoming receipts** (`process_incoming_receipts`, `:2485`) — cross-shard receipts for this chunk, validated in `ExistingReceipt` mode first; over-limit ones are pushed onto the **delayed** queue (`:2522`).
4. **Promise-yield timeouts** (`resolve_promise_yield_timeouts`, `:2930`) — for queue entries whose `expires_at <= block_height`, emit a timeout `PromiseResume` receipt (data `None`) if the yield is still unresolved.

This order is load-bearing: local receipts (which include staking and same-account calls) run before the backlog, and the backlog runs before new incoming work, so the queue drains FIFO and gas-limit pressure preferentially delays the newest work. Each receipt is processed by `process_receipt_and_instant_receipts` (`:2563`), which runs the receipt and then drains any **instant receipts** it produced (single-`DeleteAccount` and `PromiseYield` receipts) before moving on.

### Receipt dispatch and the postponed/data-dependency machinery

`process_receipt` (`runtime/runtime/src/lib.rs:1288`) dispatches by receipt kind:
- **Data receipt** (`:1307`): store the data keyed by `data_id`; if a postponed action receipt was waiting on it and this was its last pending input, load and execute it; else decrement the pending-data count.
- **Action receipt** (`:1396`): `process_action_receipt` (`:1514`) counts inputs not yet present; if `pending_data_count == 0`, execute now; else store the receipt as postponed and a `PostponedReceiptId` link per missing `data_id`.
- **PromiseYield** (`:1416`): store the yield receipt, await its `PromiseResume`.
- **PromiseResume** (`:1421`): a timeout resume (`data None`) is dropped if a real resume was already initiated; otherwise find the matching yield receipt, clean up status/`yield_id` mappings (under `YieldWithId`), store the data, and execute the yield receipt.
- **GlobalContractDistribution** (`:1491`): handled by global-contracts code; only its compute cost is added.

### Executing an action receipt

`apply_action_receipt` (`runtime/runtime/src/lib.rs:771`):
1. Collect each input `data_id`'s `ReceivedData`, removing it from state; a missing entry is a fatal `StorageInconsistentState` (`:791`). `PromiseResult::Successful`/`Failed` is built per input.
2. Commit pending writes under `StateChangeCause::ActionReceiptProcessingStarted` so the receipt executes on a clean boundary (`:820`).
3. Seed `result` with the `new_action_receipt` exec fee as base gas/compute (`:828`).
4. **Execute actions one at a time** (`:834`): for each action build an `action_hash`, call `apply_action`, then **validate any newly created receipts** in `NewReceipt` mode (size + action limits); a validation failure becomes `NewReceiptValidationError` (`:856`). `merge` the action result; **on the first error, record the action index and break** — remaining actions do not run (`:869`).
5. **Storage-staking check**: if the receipt succeeded and the receiver account still exists, require its balance covers storage; insufficient → `LackBalanceForState` error (rolls the receipt back) (`:876`).
6. **Refunds** (see below) (`:914`).
7. **Commit or rollback**: success commits under `StateChangeCause::ReceiptProcessing`; failure calls `state_update.rollback()`, undoing all of the receipt's state changes (`:946`).
8. **Burnt-amount & function-call reward** accounting (`:957`–`:1006`).
9. **Generate output data receipts** to `output_data_receivers`: if the return is a `ReceiptIndex` (a promise), the output receivers are attached to that promise's receipt; otherwise `Data` receipts carrying the return value (or `None` on failure) are created (`:1019`).
10. **Assign receipt IDs and route** each new receipt: instant receipts go to the local `instant_receipts` queue; all others are `forward_or_buffer_receipt`'d through the sink (`:1060`–`:1105`).
11. Build the `ExecutionOutcomeWithId`: status is `SuccessReceiptId` / `SuccessValue` / `Failure`; `gas_burnt`, `compute_usage`, `tokens_burnt`, `executor_id`, and metadata are recorded (`:1107`–`:1140`). Metadata is `ExecutionMetadata::V4` (per-action `current_contracts`) under `ExecutionMetadataV4` (activated at PV 85, so enabled at PV 86), else `V3`.

### Per-action execution

`apply_action` (`runtime/runtime/src/lib.rs:523`) charges the static `exec_fee` for the action up front into `gas_used`/`gas_burnt`/`compute_usage` (`:539`), captures `current_contract`, then runs `check_account_existence` and `check_actor_permissions` before the per-action handler. Implicit account creation is allowed only when the action is the sole action and not a refund (`:549`). Handlers (all in `actions.rs` / `access_keys.rs` / `function_call.rs` unless noted):

| Action | What it does / state & receipts produced | Anchor |
|---|---|---|
| `CreateAccount` | Validates top-level/sub-account naming rules; creates a zero-balance `Account` with base storage. | `actions.rs:152` — `action_create_account` |
| `DeployContract` | Clears old contract storage usage, sets `AccountContract::Local(hash)`, writes code, precompiles (with next-epoch warming). | `actions.rs:259` — `action_deploy_contract` |
| `DeployGlobalContract` / `UseGlobalContract` | Deploy/reference a global contract (distribution receipts; [contract-vm](contract-vm.md)). | `lib.rs:593`/`:605` |
| `DeterministicStateInit` | Creates/initializes a deterministic account's state. | `deterministic_account_id::action_deterministic_state_init` |
| `FunctionCall` | Resolves the contract, runs the VM, accumulates burnt/used gas + compute + logs + profile, creates promise/yield/data receipts on success, sets new balance/storage. | `function_call.rs:31` — `action_function_call` |
| `Transfer` | Credits the account; if the account does not exist, performs implicit account creation (near/eth/deterministic). Gas refunds to a gas key first, then allowance. | `lib.rs:2831` — `action_transfer_or_implicit_account_creation` |
| `Stake` | Validates min stake / can't-unstake-if-not-staked, pushes a `ValidatorStake` proposal, moves `amount`↔`locked`. | `actions.rs:44` — `action_stake` |
| `AddKey` | Rejects duplicate key; adds a regular or gas key (initializes nonces; charges storage usage). | `access_keys.rs:149` — `action_add_key` |
| `DeleteKey` | Removes a regular/gas key and reclaims storage. | `access_keys.rs:52` — `action_delete_key` |
| `DeleteAccount` | Refuses if state too large or gas-key balance too high; refunds remaining balance to beneficiary; burns gas-key balances; removes the account. | `actions.rs:299` — `action_delete_account` |
| `Delegate` / `DelegateV2` | Meta-transaction: verifies inner signature/nonce/permission, emits a new action receipt to the inner receiver; burns the inner actions' prepaid send fees. | `actions.rs:422` — `apply_delegate_action` |
| `TransferToGasKey` / `WithdrawFromGasKey` | Move balance into / out of a gas key's balance. | `access_keys.rs:257`/`:290` |

`FunctionCall` accounting (`function_call.rs:140`–`:147`): `outcome.burnt_gas` is added to both `gas_burnt` and `gas_burnt_for_function_call`; `outcome.used_gas` is added to `gas_used` (the difference is gas attached to outgoing receipts the call spawned). Unused prepaid gas is distributed/recovered by the VM's `distribute_gas` (`function_call.rs:326`).

### Gas, fee, and refund accounting

Two fee classes, both defined in `config.rs`:
- **Send fees** — burnt when a receipt is *created/sent*. `total_send_fees` (`config.rs:71`) sums per-action send fees with `sender_is_receiver` (sir) discount.
- **Exec fees** — burnt when a receipt is *executed*. `exec_fee` (`config.rs:279`) per action; `apply_action` charges these.

`tx_cost` (`config.rs:400`): `gas_remaining` = `total_prepaid_gas` (attached function-call gas) + `prepaid_send_fee.gas` + `new_action_receipt` exec fee + `prepaid_exec_fee.gas`. `burnt` (send fees + signature verification) is burnt at `current_gas_price`; `gas_remaining` is purchased at `receipt_gas_price = max(current_gas_price, min_gas_purchase_price)`. Signature-verification cost (`config.rs:512`) is 0 for ed25519/secp256k1 and non-zero only for ML-DSA-65 (`PostQuantumSignatures`).

**Refunds** — `refund_unspent_gas_and_deposits` (`runtime/runtime/src/lib.rs:1151`), the NEP-536 model:
- On failure, the **whole deposit** is refunded (`deposit_refund = total_deposit`); on success, none (`:1170`).
- `gross_gas_refund` = prepaid gas + prepaid exec gas − (`gas_burnt` on failure / `gas_used` on success) (`:1171`).
- A **gas-refund penalty** = `max(gross_gas_refund * gas_refund_penalty, min_gas_refund_penalty)` (capped at `gross_gas_refund`) is applied via `gas_penalty_for_gas_refund` (`:1186`, `core/parameters/src/cost.rs:683`); the penalty is burnt, not refunded. **At PV 86 the active parameters are `gas_refund_penalty = 0/100` and `min_gas_refund_penalty = 0`, so the penalty is 0** (`core/parameters/res/runtime_configs/parameters.yaml:14`). The 5% / 1 Tgas rate from NEP-536 is the hardcoded Rust default (`cost.rs:611`) but is overridden to 0 by the YAML at every shipped version; it is not active at PV 86.
- A **deposit refund receipt** (`new_balance_refund`, system → `balance_refund_receiver`) and/or a **gas refund receipt** (`new_gas_refund`, system → signer, which also tops up the access-key allowance) are pushed (`:1269`–`:1283`).
- Under `AccountCostIncrease` (enabled at PV 86), gas burnt at price differences is reconciled: `gas_burn_price = min(receipt.gas_price, apply_state.gas_price)` (`:905`); a `price_deficit` (price rose) reduces `tx_burnt_amount`, a `price_surplus` (price fell) is refunded to the signer; and creating a new account incurs `create_account_charge` deducted from the refund up to `account_creation_charge` (`:1228`).

**Refund (system) receipts** are free: `predecessor_id().is_system()` ⇒ `gas_burnt` is forced to 0 and no refund-of-refund is generated; a failed refund **burns** the deposit into `other_burnt_amount` (`:914`–`:922`, `:957`). Their gas use is still counted toward the chunk gas limit (`ApplyProcessingState::new`, `:3093`).

**Function-call reward**: a fraction (`burnt_gas_reward`) of `gas_burnt_for_function_call` is credited to the receiver account if it still exists; otherwise validators get the full amount via `tx_burnt_amount` (`:974`–`:1006`).

### Recording outcomes & gas limit

Every transaction and every executed receipt produces an `ExecutionOutcomeWithId` (`outcomes`), keyed by tx hash / receipt id. Status is `SuccessValue` / `SuccessReceiptId` / `Failure`. Running totals (`TotalResourceGuard`, `:3039`) accumulate gas+compute; once `total.compute >= compute_limit` (or the storage-proof limit is exceeded) each processing loop stops and **leftover local/incoming receipts are pushed onto the delayed queue** so they execute next chunk.

### Phase 9 — finalization

`validate_apply_state_update` (`runtime/runtime/src/lib.rs:2667`): persist updated `PromiseYieldIndices`; finalize own congestion info incl. `finalize_allowed_shard` using a `block_height`-derived seed; generate bandwidth requests; commit `UpdatedDelayedReceipts`; apply any sandbox state patch; optionally invoke `on_post_state_ready`; `state_update.finalize()` to produce the new root, trie changes, state changes, and contract updates; dedup validator proposals (keep last per account in reverse order, `:2758`); collect outgoing receipts from the sink; assemble `ApplyResult`.

## Interactions

- **Consumes**: a `Trie` + state root, `ApplyState`, `incoming_receipts`, `SignedValidPeriodTransactions`, optional `ValidatorAccountsUpdate`, an `EpochInfoProvider`. State reads/writes go through `TrieUpdate` → [state-storage](state-storage.md).
- **Produces**: `ApplyResult` — new state root + trie changes, `outgoing_receipts`, `outcomes`, `validator_proposals`, congestion info, bandwidth requests, `proof` for stateless validation ([stateless-validation](stateless-validation.md)).
- **Invokes the VM** for `FunctionCall` and contract preparation/precompilation ([contract-vm](contract-vm.md)).
- **Outgoing receipt routing** (`forward_or_buffer_receipt`, `ReceiptSink`, bandwidth scheduler) → [cross-shard-congestion](cross-shard-congestion.md).
- **Stake proposals** → [epoch-validators-staking](epoch-validators-staking.md); **rewards & fee burning** feed [economics](economics.md).
- **Account / access-key / gas-key semantics** → [accounts-keys](accounts-keys.md).

## Protocol-version-gated behavior

Features cited against `core/primitives-core/src/version.rs:453`. At PV 86 every feature activated at 85 is **enabled**; `FixContractLoadingError` activates exactly at 86; nightly features (e.g. `FixContractLoadingCost` = 129) are **disabled**.

| Feature | Activates | Effect on this component |
|---|---|---|
| `AccountCostIncrease` | 85 (enabled) | Reconciles gas burnt at price differences: `gas_burn_price = min(receipt_price, current_price)`; surplus refunded, deficit reduces burnt amount; adds `create_account_charge`. `lib.rs:905`, `:1187`, `:1228` |
| `GasKeys` | 85 (enabled) | Gas-key transactions/actions: separate nonce store, `verify_and_charge_gas_key_tx_ephemeral`, `TransferToGasKey`/`WithdrawFromGasKey`, gas-key refund path. `verifier.rs:370`, `access_keys.rs:257` |
| `DelegateV2` | 85 (enabled) | `Action::DelegateV2` meta-transactions with gas-key support, handled identically to `Delegate`. `lib.rs:736`, `config.rs:259` |
| `FixDelegateActionDepositWithFunctionCallError` | 85 (enabled) | Fix missing early-return on the `DepositWithFunctionCall` error path in delegate-action key validation, so the error is no longer overwritten by a later check. `actions.rs:645` |
| `FixDelegatedDeterministicStateInit` | 85 (enabled) | Allow creating a `DeterministicStateInitAction` from a delegated action by fixing the receiver-id check. `action_validation.rs:189` |
| `StrictNonce` | 85 (enabled) | `NonceMode::Strict` requires `tx_nonce == ak_nonce + 1`; otherwise monotonic `tx_nonce > ak_nonce`. `verifier.rs:217` |
| `UniqueChunkTransactions` | 85 (enabled) | Skip duplicate tx hashes within a chunk to avoid conflicting outcomes. `lib.rs:1952` |
| `ExecutionMetadataV4` | 85 (enabled) | Emit `ExecutionMetadata::V4` with per-action `current_contracts`; else `V3`. `lib.rs:1120` |
| `YieldWithId` | 85 (enabled) | Maintain/clean up `yield_id ↔ data_id` mappings on resume. `lib.rs:1445` |
| `FixDeleteAccountGlobalContractStorageUsage` | 85 (enabled) | `action_delete_account` subtracts global-contract identifier storage (not just local code). `actions.rs:311` |
| `PostQuantumSignatures` | 85 (enabled) | ML-DSA-65 keys/signatures accepted; non-zero signature-verification cost burnt at conversion. `config.rs:485`, `:512` |
| `ClampOutgoingGasAdmission` | 85 (enabled) | Affects outgoing-receipt gas admission in the receipt sink → see [cross-shard-congestion](cross-shard-congestion.md). |
| `FixContractLoadingError` | 86 (enabled) | Charge contract-loading fee and finalize as a gas-bearing abort when `Module::deserialize` fails (was a zero-gas nop) → see [contract-vm](contract-vm.md). |
| `FixContractLoadingCost` | 129 (nightly, disabled) | Further contract-loading-cost fix; not active at PV 86. |
| NEP-536 (`_DeprecatedReducedGasRefunds`) | 78 (long enabled) | The refund model: charge current gas price, plus a parameterized gas-refund penalty mechanism. The penalty *rate* is set by the `gas_refund_penalty` / `min_gas_refund_penalty` parameters, which are **0 at PV 86** (no penalty); the NEP's 5% / 1 Tgas is only the inert Rust default. `lib.rs:1143`, `core/parameters/res/runtime_configs/parameters.yaml:14` |

## Invariants & failure modes

- **Gas ordering invariant**: `gas_burnt_for_function_call <= gas_burnt <= gas_used`, asserted on every `merge` (`lib.rs:440`).
- **Atomic receipt execution**: any action error rolls back the entire receipt's state changes via `state_update.rollback()`; only successes commit (`lib.rs:946`). `set_error` clears queued receipts/proposals/burnt amounts (`lib.rs:487`).
- **Storage staking**: after a successful receipt, the receiver's balance must cover its storage or the receipt fails with `LackBalanceForState` (`lib.rs:882`, `verifier.rs:47`); zero-balance accounts (≤ `ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT` bytes) are exempt (`verifier.rs:87`).
- **Invalid transactions are skipped, not fatal**: a chunk with invalid txs still applies; each invalid tx yields a failed outcome and produces no receipt (`lib.rs:1691` doc, `:1965`).
- **Staking invariant**: at validator update, `account.locked() >= max_of_stakes` must hold or `apply` returns a fatal `StorageInconsistentState` (`lib.rs:1602`).
- **State-inconsistency = hard error**: missing input data, missing postponed receipt, missing pending-data count, or an invalid delayed receipt all return `StorageError::StorageInconsistentState` (`lib.rs:796`, `:1352`, `:2451`).
- **Gas-limit backpressure**: leftover local/incoming receipts when over the compute limit are stored as delayed (`lib.rs:2332`, `:2522`); delayed-queue indices are persisted (`UpdatedDelayedReceipts`).
- **Refund failures burn**: a failed system-refund receipt burns the deposit into `other_burnt_amount` (`lib.rs:917`).
- **Asserted by tests**: signature-verification charging and gas/compute independence in `config.rs` tests (`ml_dsa_65_*`, `:671`–`:793`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `runtime/runtime/src/lib.rs:1702` | `Runtime::apply` | Top-level ordered state transition |
| `runtime/runtime/src/lib.rs:1584` | `update_validator_accounts` | Rewards + return-stake + treasury at epoch boundary |
| `runtime/runtime/src/lib.rs:2881` | `missing_chunk_apply_result` | Early-exit result for missing chunks |
| `runtime/runtime/src/lib.rs:1853` | `process_transactions` | Validate/charge txs → local/outgoing receipts |
| `runtime/runtime/src/lib.rs:2602` | `process_receipts` | Orders local → delayed → incoming → yield timeouts |
| `runtime/runtime/src/lib.rs:2301` | `process_local_receipts` | Local receipts; over-limit → delayed |
| `runtime/runtime/src/lib.rs:2385` | `process_delayed_receipts` | FIFO backlog from prior chunks |
| `runtime/runtime/src/lib.rs:2485` | `process_incoming_receipts` | Cross-shard receipts; over-limit → delayed |
| `runtime/runtime/src/lib.rs:2563` | `process_receipt_and_instant_receipts` | Run receipt then drain instant receipts |
| `runtime/runtime/src/lib.rs:1288` | `process_receipt` | Dispatch by receipt kind; data-dependency machinery |
| `runtime/runtime/src/lib.rs:1514` | `process_action_receipt` | Postpone or execute action receipt |
| `runtime/runtime/src/lib.rs:771` | `apply_action_receipt` | Execute actions, refund, commit/rollback, build outcome |
| `runtime/runtime/src/lib.rs:523` | `apply_action` | Per-action dispatch + existence/permission checks |
| `runtime/runtime/src/lib.rs:1151` | `refund_unspent_gas_and_deposits` | NEP-536 gas/deposit refunds + penalty + account charge |
| `runtime/runtime/src/lib.rs:2667` | `validate_apply_state_update` | Finalize trie, congestion, bandwidth, proposals |
| `runtime/runtime/src/lib.rs:2930` | `resolve_promise_yield_timeouts` | Emit timeout PromiseResume receipts |
| `runtime/runtime/src/config.rs:400` | `tx_cost` / `calculate_tx_cost` | Burnt vs attached gas, receipt gas price |
| `runtime/runtime/src/config.rs:71` | `total_send_fees` | Per-action send fees |
| `runtime/runtime/src/config.rs:279` | `exec_fee` | Per-action exec fees |
| `runtime/runtime/src/verifier.rs:269` | `verify_and_charge_tx_ephemeral` | Regular tx verify/charge (pure) |
| `runtime/runtime/src/verifier.rs:370` | `verify_and_charge_gas_key_tx_ephemeral` | Gas-key tx verify/charge (pure) |
| `runtime/runtime/src/verifier.rs:211` | `verify_nonce` | Monotonic vs strict nonce |
| `runtime/runtime/src/verifier.rs:527` | `validate_receipt` | New vs existing receipt validation |
| `runtime/runtime/src/actions.rs:711` | `check_actor_permissions` | Per-action actor authorization |
| `runtime/runtime/src/actions.rs:759` | `check_account_existence` | Existence + implicit-creation rules |
| `runtime/runtime/src/actions.rs:422` | `apply_delegate_action` | Meta-transaction expansion |
| `runtime/runtime/src/function_call.rs:31` | `action_function_call` | VM invocation + gas accounting |
| `core/primitives/src/receipt.rs:473` | `Receipt::is_instant_receipt` | Defines instant receipts |
| `core/primitives-core/src/version.rs:453` | `ProtocolFeature::protocol_version` | Feature activation versions |

## Open questions

- **`compute_limit == gas_limit`**: the chunk has no separate compute budget yet (`lib.rs:2612`, TODO #8859); both are driven by `apply_state.gas_limit`. If a dedicated compute limit is introduced this section needs revising.
- **`ClampOutgoingGasAdmission` and `FixContractLoadingError` effects** are realized in the receipt sink / VM respectively; this spec notes their activation but defers the mechanism to [cross-shard-congestion](cross-shard-congestion.md) and [contract-vm](contract-vm.md). The exact admission-clamp formula was not read here.
- **`subsidized_amount`** is tracked through `ActionResult`/stats but its downstream economic interpretation was not traced in this component; see [economics](economics.md).
- **Docs staleness**: `docs/RuntimeSpec/ApplyingChunk.md` still lists a "slash locked balance" step and omits the bandwidth scheduler, buffered-receipt forwarding, congestion finalization, and promise-yield timeout resolution that the code performs. Treat the code ordering in this spec as authoritative.
