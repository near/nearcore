# Economics

> Protocol version: 87 (stable) · Release: 2.14.0-rc.1 · Derived from commit: 233252e · Generated: 2026-09-14
> Primary crates/files: `chain/epoch-manager/src/reward_calculator.rs`, `runtime/runtime/src/lib.rs`, `runtime/runtime/src/config.rs`, `runtime/runtime/src/verifier.rs`, `runtime/runtime/src/global_contracts.rs`, `core/parameters/src/cost.rs`, `core/parameters/src/config.rs`, `core/parameters/src/config_store.rs`, `core/parameters/res/runtime_configs/`, `core/primitives/src/block.rs`, `core/primitives/src/chunk_apply_stats.rs`, `chain/chain/src/types.rs`, `chain/chain/src/runtime/mod.rs`

## Role
Economics is the set of monetary rules realized by the implementation: per-epoch token issuance (inflation) and the validator-reward / protocol-treasury split; block-to-block gas-price adjustment toward a target load; which gas/fees are *burnt* (removed from total supply) versus *redistributed*; the price-difference and NEP-536 penalty handling of gas refunds; and storage staking. It does *not* decide who gets reward — validator reward *assignment* lives in [epoch-validators-staking](epoch-validators-staking.md). Fee *amounts* and gas accounting during action execution live in [runtime-execution](runtime-execution.md); this spec covers the monetary effect (burning, rewards, total-supply change). Parameters originate from genesis and the versioned `RuntimeConfig`/`EpochConfig` stores ([genesis-configuration](genesis-configuration.md)). Storage byte-cost enforcement ties into [accounts-keys](accounts-keys.md) and [state-storage](state-storage.md).

**Headline change at PV 87:** `RemoveGasRewards` sets `burnt_gas_reward` from `3/10` to `0/1`, so a contract account no longer receives a share of the gas its `FunctionCall`s burn. That share is now simply burnt — **destroyed**, i.e. subtracted from total supply via the per-block `balance_burnt` term (§3) — like the rest of the receipt's burnt gas (§6.4). It is *not* redirected to validators; validator income is the §1 inflation pool and is unaffected.

## Key data structures
- **`RewardCalculator`** — `chain/epoch-manager/src/reward_calculator.rs:27` — inputs to issuance: `num_blocks_per_year`, `epoch_length`, `protocol_reward_rate` (`Rational32`), `protocol_treasury_account`, `num_seconds_per_year` (always `NUM_SECONDS_IN_A_YEAR = 24*60*60*365`, `reward_calculator.rs:10`), and `genesis_protocol_version`. Built from `GenesisConfig` in `new` (`reward_calculator.rs:37`).
- **`ValidatorOnlineThresholds`** — `chain/epoch-manager/src/reward_calculator.rs:14` — `online_min_threshold`/`online_max_threshold` (rationals) bound the linear uptime→reward ramp; optional `endorsement_cutoff_threshold` (0–100 percent) remaps a validator's endorsement ratio to 0 or 1 before averaging.
- **`RuntimeFeesConfig`** — `core/parameters/src/cost.rs:568` — per-action `Fee` map (`action_fees`), `storage_usage_config`, `burnt_gas_reward` (`Rational32`; `cost.rs:576`), `pessimistic_gas_price_inflation_ratio` (`cost.rs:579`), `gas_refund_penalty` + `min_gas_refund_penalty` (NEP-536; `cost.rs:587`, `cost.rs:594`), the global-contract distribution compute costs, and `signature_verification_costs` (per-`SignatureKind`, all 0 except ML-DSA-65; `cost.rs:613`).
- **`Fee`** — `core/parameters/src/cost.rs:18` — `send_sir` / `send_not_sir` / `execution` components, each a `FeeComponent` (`Gas` or `GasAndCompute`); `send_fee(sir)` (`cost.rs:38`) and `exec_fee()` (`cost.rs:42`) return a `ParameterCost { gas, compute }`.
- **`StorageUsageConfig`** — `core/parameters/src/cost.rs:618` — `storage_amount_per_byte` (yN backing each storage byte, `cost.rs:621`), `num_bytes_account`, `num_extra_bytes_record`, `global_contract_storage_amount_per_byte` (`cost.rs:627`).
- **`RuntimeConfig`** — `core/parameters/src/config.rs:17` — top-level runtime params; economics-relevant fields: `fees` (`config.rs:22`), `wasm_config` (`config.rs:27`), `min_gas_purchase_price` (`config.rs:42`), `account_creation_charge` (`config.rs:46`), and the `storage_amount_per_byte()` accessor (`config.rs:109`).
- **`BlockEconomicsConfig`** — `chain/chain/src/types.rs:182` — gas-price bounds derived from genesis: `gas_price_adjustment_rate`, `genesis_min_gas_price`, `genesis_max_gas_price`, `genesis_protocol_version`.
- **`GasRefundResult`** — `runtime/runtime/src/lib.rs:546` — `price_deficit`, `price_surplus`, `refund_penalty`, and private `create_account_charge`, produced while refunding unspent gas.
- **`TransactionCost`** — `runtime/runtime/src/config.rs:22` — output of `tx_cost`: `gas_burnt`, `compute_burnt`, `gas_remaining`, `receipt_gas_price`, `burnt_amount`, `gas_cost`, `deposit_cost`, `total_cost`.
- **`BalanceStatsV1`** — `core/primitives/src/chunk_apply_stats.rs:243` — the per-chunk money ledger: `tx_burnt_amount`, `slashed_burnt_amount`, `other_burnt_amount`, `gas_deficit_amount` (informational only), and `subsidized_amount` (effectively minted; `chunk_apply_stats.rs:253`).

## Behavior

### 1. Per-epoch issuance (inflation)
`RewardCalculator::calculate_reward` computes the newly minted tokens for an epoch (`chain/epoch-manager/src/reward_calculator.rs:51`). It is called once at each epoch boundary from `EpochManager` with the previous block's total supply and the epoch's `max_inflation_rate` (`chain/epoch-manager/src/lib.rs:1176`, result bound at `lib.rs:1149`).

1. The epoch total reward is a *time-proportional slice* of the annual max-inflation cap (`reward_calculator.rs:69`):
   `epoch_total_reward = max_inflation_rate.numer * total_supply * epoch_duration / (num_seconds_per_year * max_inflation_rate.denom * NUM_NS_IN_SECOND)`.
   `epoch_duration` is in nanoseconds (actual wall-clock epoch length, `chain/epoch-manager/src/lib.rs:1154`), so a shorter/longer epoch mints proportionally less/more. `max_inflation_rate` on mainnet is `1/40` (2.5%) since PV 81 (`core/primitives/res/epoch_configs/mainnet/81.json`), reduced from `1/20` (5%; `.../80.json`); the newest mainnet epoch config at or below PV 87 is `.../85.json`, which keeps `1/40`.
2. `protocol_reward_rate` is forced to `1/10` when `genesis_protocol_version == PROD_GENESIS_PROTOCOL_VERSION` (29), otherwise taken from `GenesisConfig` (`reward_calculator.rs:63`).
3. The treasury slice is `epoch_protocol_treasury = epoch_total_reward * protocol_reward_rate` and is credited to `protocol_treasury_account` (`reward_calculator.rs:78`).
4. If there are no validators, the function returns early with the treasury entry in the map but an `epoch_actual_reward` of `Balance::ZERO` (`reward_calculator.rs:85`), so nothing is added to supply that epoch.
5. The validator pool is the remainder `epoch_total_reward - epoch_protocol_treasury` (`reward_calculator.rs:88`), distributed stake-weighted and uptime-scaled (see §2).
6. Returns `(rewards_map, epoch_actual_reward)` where `epoch_actual_reward` is the *actual* sum minted — treasury plus each validator's earned reward (`reward_calculator.rs:145`). Validators below the online-min threshold earn 0, so `epoch_actual_reward` can be less than `epoch_total_reward`; only `epoch_actual_reward` (stored as the epoch's `minted_amount`) is added to supply.
7. Validators kicked out for `NotEnoughBlocks`/`NotEnoughChunks`/`NotEnoughChunkEndorsements` are removed from the stats map *before* the call, so they earn nothing (`chain/epoch-manager/src/lib.rs:1156`); the endorsement cutoff threshold is the epoch config's `chunk_validator_only_kickout_threshold` (`lib.rs:1169`).

### 2. Validator reward (uptime ramp)
For each validator (`reward_calculator.rs:94`):
1. `production_ratio` = its combined online ratio across blocks/chunks/endorsements (`get_validator_online_ratio`, `chain/epoch-manager/src/validator_stats.rs:16`; the endorsement ratio is first cut to 0/1 by `endorsement_cutoff_threshold` when set).
2. If `production_ratio < online_min_threshold`, or all of expected blocks/chunks/endorsements are 0, reward is 0 (`reward_calculator.rs:109`).
3. Otherwise the online multiplier is `min(1, (uptime − online_min)/(online_max − online_min))`, and the reward is `epoch_validator_reward * multiplier * stake / total_stake` (`reward_calculator.rs:133`). All arithmetic is exact integer `U256`/`U512` rational math. Mainnet thresholds are `online_min = 90/100`, `online_max = 99/100` (`core/primitives/res/epoch_configs/mainnet/85.json`).

### 3. Total-supply change per block
`new_total_supply = prev.total_supply + minted_amount − balance_burnt` (`core/primitives/src/block.rs:195`; the verification counterpart is `Block::verify_total_supply_checked`, `block.rs:335`). `minted_amount` is `Some` only on the first block of an epoch, taken from the epoch info populated by `calculate_reward` (produced at `chain/client/src/client.rs:1112`, verified at `chain/chain/src/chain.rs:2543`). `balance_burnt` is the sum of each included chunk's `prev_balance_burnt()` (`block.rs:154`). Thus inflation *adds* to supply once per epoch, and burnt fees *subtract* every block; the difference is net issuance.

The per-chunk `balance_burnt` is assembled by the runtime adapter: `tx_burnt_amount + other_burnt_amount + slashed_burnt_amount` minus `subsidized_amount` (`chain/chain/src/runtime/mod.rs:392`, `mod.rs:405`), surfaced as `ApplyChunkResult::total_balance_burnt` (`chain/chain/src/types.rs:119`) and written into the chunk extra / chunk header by `to_chunk_extra` (`types.rs:166`). `subsidized_amount` is balance the protocol effectively **mints**. When `one_yocto_on_promise` is set (`85.yaml:1`, so PV ≥ 85; no `ProtocolFeature` variant) and the calling contract's balance is exactly zero, attaching exactly 1 yoctoNEAR to a promise function call skips the balance deduction entirely: the account is *not* debited, and the skipped yocto is added to `subsidized_amount` instead (`runtime/near-vm-runner/src/wasmtime_runner/logic.rs:4291` — `promise_batch_action_function_call`, and the weight variant at `logic.rs:3452`; conditions are `amount == 1yN && one_yocto_on_promise && current_account_balance.is_zero()`). The receipt still carries the 1 yoctoNEAR deposit to the callee, so the yocto appears in circulation without ever having been debited — i.e. it is minted. It propagates `VMOutcome → ActionResult` (`runtime/runtime/src/function_call.rs:223`) → `stats.balance.subsidized_amount` (`runtime/runtime/src/lib.rs:1143`), and subtracting it from the chunk burn (`chain/chain/src/runtime/mod.rs:405`) is what keeps `new_total_supply` exact: the mint is netted against the same block's burn rather than added to `minted_amount`. `gas_deficit_amount` is recorded for observability only (`runtime/runtime/src/lib.rs:1072`) and does not enter the supply equation.

### 4. Gas-price adjustment
The next block's gas price is a load-feedback controller (`core/primitives/src/block.rs:455` — `compute_next_gas_price_checked`):
- Formula (documented at `block.rs:432`): `next_gas_price = gas_price * (1 + (gas_used/gas_limit − 1/2) * adjustment_rate)`. Implemented as the exact integer ratio `numerator/denominator` at `block.rs:475`. When utilization is exactly 50% the price is unchanged; above 50% it rises, below it falls.
- If the block was skipped (`gas_limit == 0`) the price is unchanged (`block.rs:464`).
- The result is clamped to `[min_gas_price, max_gas_price]` (`block.rs:486`).
- `min_gas_price` is `MIN_GAS_PRICE_NEP_92_FIX` (`100_000_000` yN) for chains whose genesis is `PROD_GENESIS_PROTOCOL_VERSION`, else the genesis value (`chain/chain/src/types.rs:199`, const at `core/primitives/src/version.rs:29`). `max_gas_price` is `min(genesis_max_gas_price, min_gas_price * 20)` (`types.rs:207`, `MAX_GAS_MULTIPLIER = 20` at `types.rs:191`).
- The chain applies this over one block at block construction (`block.rs:175`); under Spice (nightly only) it folds over certified results (`compute_gas_price_from_certified_results_checked`, `block.rs:494`), and `balance_burnt` likewise folds over certified results (`block.rs:515`).

### 5. Transaction cost & what is burnt at conversion
`calculate_tx_cost` (`runtime/runtime/src/config.rs:481`; `tx_cost` at `config.rs:464` is the thin wrapper over a `Transaction`) splits a transaction's cost:
1. `burnt` = `new_action_receipt` send fee + `total_send_fees` of the actions (`config.rs:493`) + the signature-verification surcharge (`config.rs:504`). This gas is burnt *immediately* at `current_gas_price` (`burnt_amount`, `config.rs:527`).
2. `gas_remaining` = attached (prepaid) function-call gas + prepaid send fees + the wrapping receipt's exec fee + prepaid exec fees (`config.rs:521`). This gas is *purchased* (not yet burnt) at `receipt_gas_price = max(current_gas_price, min_gas_purchase_price)` (`config.rs:532`).
3. `gas_cost = burnt_amount + remaining_gas_amount` (`config.rs:534`) and `total_cost = gas_cost + total_deposit` (`config.rs:536`).

Send/exec fees per action are computed by `total_send_fees` (`config.rs:86`) and `exec_fee` (`config.rs:334`), each reading the `Fee` for the action's `ActionCosts` and adding per-byte components. Transfers add implicit-account-creation fees via `transfer_send_fee`/`transfer_exec_fee` (`core/parameters/src/cost.rs:812`/`775`). Gas-key transfers price the exec fee with `gas_key_transfer_exec_fee` (`cost.rs:882`, called at `config.rs:411`/`config.rs:415`) and `AddKey` with gas-key permissions adds `gas_key_add_key_exec_fee` (`cost.rs:928`). Universal-account state inits are priced by `universal_state_init_fee` (`config.rs:242`).

The signature-verification surcharge is `signature_verification_cost` (`config.rs:589`): the signer's own scheme cost plus, for every delegate action, the inner signer's scheme cost. Only ML-DSA-65 is non-zero. It raises the burnt gas (and therefore `total_cost`) but never `gas_remaining`, so on-chain function-call budgets are unaffected (comment at `config.rs:500`).

### 6. Gas burning, rewards, and refunds during receipt execution
In `Runtime::apply_action_receipt` (`runtime/runtime/src/lib.rs`), after actions run:
1. **Gas burn price** — `gas_purchase_price` is the receipt's own price (`lib.rs:1034`); with `AccountCostIncrease` enabled, gas is burnt at `min(gas_purchase_price, apply_state.gas_price)`, before it at `apply_state.gas_price` (`lib.rs:1038`).
2. **Refunds** — `refund_unspent_gas_and_deposits` (`lib.rs:1284`) refunds unspent gas and, on failure, the full deposit. `gross_gas_refund` = prepaid gas + prepaid exec gas − gas actually used/burnt (`lib.rs:1304`). It then:
   - Computes the NEP-536 penalty `gas_penalty_for_gas_refund(gross)` = `min(gross, max(gross * gas_refund_penalty, min_gas_refund_penalty))` (`core/parameters/src/cost.rs:736`), priced at `gas_burn_price` post-`AccountCostIncrease` or `gas_purchase_price` before (`lib.rs:1320`). The unused-gas refund is issued at `gas_purchase_price` minus this penalty (`lib.rs:1328`).
   - Records `price_deficit` if the burn price rose above purchase price, else `price_surplus` (`lib.rs:1338`). With `AccountCostIncrease`, the surplus is refunded to the signer (`burned_gas_refund`, `lib.rs:1353`); before, it was retained as burnt.
   - If a new account was created (post-`AccountCostIncrease`), an extra `create_account_charge` = `min(account_creation_charge − already-burned, burned_gas_refund)` is subtracted from the refund (`lib.rs:1361`).
   - System-originated (refund) receipts skip all of this and use `GasRefundResult::default()`; if such a refund itself fails, its deposit is added to `other_burnt_amount` (`lib.rs:1047`).
3. **tx_burnt_amount** — burnt tokens for the receipt = `gas_burn_price * gas_burnt − price_deficit` (`lib.rs:1093`), plus `refund_penalty` (`lib.rs:1099`), `create_account_charge` (`lib.rs:1100`) and `result.tokens_burnt` (`lib.rs:1102`); pre-`AccountCostIncrease` also plus `price_surplus` (`lib.rs:1096`). System/refund receipts burn 0 gas (`lib.rs:1090`). The `ExecutionOutcome.tokens_burnt` reported to clients is snapshotted *here*, before any contract reward is deducted (`lib.rs:1105`, used at `lib.rs:1269`).
4. **Contract reward (zero at PV 87)** — `receiver_gas_reward = gas_burnt_for_function_call * burnt_gas_reward` (`lib.rs:1108`), converted to tokens at `gas_burn_price` post-`AccountCostIncrease`, else at `gas_purchase_price` (`lib.rs:1116`). If it is positive and the receiver account still exists, it is *subtracted* from `tx_burnt_amount` and credited to the account (`lib.rs:1126`–`lib.rs:1132`). At PV 87 `burnt_gas_reward` is `0/1` (`core/parameters/res/runtime_configs/87.yaml:3`), so `receiver_gas_reward` is always 0, the branch is never taken, and the full `tx_burnt_amount` stays burnt. Consequences:
   - Function-call gas that used to be redistributed to the contract is now removed from total supply via §3, benefiting all holders proportionally rather than the callee. (The in-code comment at `lib.rs:1129` saying "validators receive the remaining execution reward" is stale: burnt tokens are destroyed, not paid out; validator income comes from the §1 inflation pool.)
   - `ExecutionOutcome.gas_burnt` and `ExecutionOutcome.tokens_burnt` are **unchanged** by the switch. `gas_burnt` is `result.gas_burnt`, which the reward path never touches (`lib.rs:1267`), and `tokens_burnt` is the snapshot taken at `lib.rs:1105` and moved into the outcome at `lib.rs:1269` — the reward is subtracted only from the local `tx_burnt_amount` afterwards (`lib.rs:1132`). So the only observable differences are the chunk-level `balance_burnt` (larger by the former reward) and the receiver account's balance (smaller by it).
   - `ActionResult::gas_burnt_for_function_call` (`lib.rs:414`, accumulated in `runtime/runtime/src/function_call.rs:141`) is still tracked; it simply no longer has an effect.
5. **Accumulation** — the remaining `tx_burnt_amount` accumulates into `stats.balance.tx_burnt_amount` (`lib.rs:1141`) and `result.subsidized_amount` into `stats.balance.subsidized_amount` (`lib.rs:1143`). Transaction-conversion burn is accumulated separately from the transaction's own outcome (`lib.rs:2297`, `lib.rs:2338`).
6. **Global-contract storage burn** — `DeployGlobalContract` burns `global_contract_storage_amount_per_byte * code.len()` straight out of the deployer's balance into `result.tokens_burnt` (`runtime/runtime/src/global_contracts.rs:35`, `global_contracts.rs:50`), failing with `ActionErrorKind::LackBalanceForState` if the balance is short. The rate is `0.0001 N` per byte since PV 77 (`core/parameters/res/runtime_configs/77.yaml:2`).

### 7. Storage staking
`check_storage_stake` (`runtime/runtime/src/verifier.rs:48`) enforces that an account backs its storage: `required = storage_amount_per_byte * account.storage_usage()` (`verifier.rs:54`), `available = account_balance + account.locked()` (`verifier.rs:64`). If `available < required` and the account is not a zero-balance account (`storage_usage() <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT = 770`, `verifier.rs:25`, `verifier.rs:89`, NEP-448), it returns `LackBalanceForStorageStaking(required − available)` (`verifier.rs:80`). Call sites:

| Site | Location | Failure surfaced as |
|---|---|---|
| ordinary tx verification | `runtime/runtime/src/verifier.rs:375` | `InvalidTxError::LackBalanceForState` |
| universal-account bootstrap tx | `runtime/runtime/src/verifier.rs:493` | `InvalidTxError::LackBalanceForState` |
| deposit (gas-key) path | `runtime/runtime/src/verifier.rs:659` | `InvalidTxError::NotEnoughBalanceForDeposit` with `DepositCostFailureReason::LackBalanceForState` (`verifier.rs:668`) |
| end of receipt application | `runtime/runtime/src/lib.rs:1011` | `ActionErrorKind::LackBalanceForState`, rolling the receipt back |
| deterministic-account state init | `runtime/runtime/src/deterministic_account_id.rs:83` | deposit refunded instead of the init proceeding |

`storage_amount_per_byte` is `1e19` yN (1 NEAR backs 100,000 bytes ≈ 100 KB) — base value at `core/parameters/res/runtime_configs/parameters.yaml:35`, unchanged at PV 87. `num_bytes_account = 100` and `num_extra_bytes_record = 40` (`parameters.yaml:36`, `parameters.yaml:37`).

### 8. Where the numbers live
Runtime parameters are the base table `core/parameters/res/runtime_configs/parameters.yaml` plus an ordered list of per-version diffs applied by `RuntimeConfigStore::new` (`core/parameters/src/config_store.rs:26` — `CONFIG_DIFFS`). **At this release the diff list starts at `53.yaml`**: the 46/48/49/50/52 diffs were deleted along with `MIN_SUPPORTED_PROTOCOL_VERSION` rising to 84, so the store no longer reproduces configs for those versions (it still holds the base config under key 0 and every retained diff version). Economic parameters in force at PV 87:

| Parameter | Value at PV 87 | Set by |
|---|---|---|
| `burnt_gas_reward` | `0 / 1` | `87.yaml:3` (was `3/10` from `parameters.yaml:6`) |
| `pessimistic_gas_price_inflation` | `1 / 1` (no-op) | `78.yaml` |
| `gas_refund_penalty` | `0 / 100` | `parameters.yaml:14` |
| `min_gas_refund_penalty` | `0` | `parameters.yaml:18` |
| `min_gas_purchase_price` | `1_000_000_000` yN | `85.yaml:41` |
| `account_creation_charge` | `0.007 N` | `85.yaml:43` |
| `storage_amount_per_byte` | `1e19` yN | `parameters.yaml:35` |
| `global_contract_storage_amount_per_byte` | `0.0001 N` | `77.yaml:2` |
| `ml_dsa_65_verification_cost` (tx-conversion surcharge) | `100 Ggas` | `85.yaml:15` |
| `action_create_account` | send `0.5 Tgas` / exec `7.2 Tgas` | `85.yaml:16` |
| `max_state_init_entries` | `1_500` | `87.yaml:61` |

Host-function gas costs stabilized at PV 87 (charged as ordinary burnt wasm gas inside `FunctionCall`, see [runtime-execution](runtime-execution.md)); note these are gated purely by yaml flags and have **no `ProtocolFeature` enum variant**:

| Host function | Base gas | Per-byte gas | Flag |
|---|---|---|---|
| `sha3_256` | `5_879_491_275` | `21_471_105` | `sha3_host_fns` (`87.yaml`) |
| `sha3_384` / `sha3_512` | `5_811_388_236` | `36_649_701` | `sha3_host_fns` |
| `ml_dsa_verify` | `540_000_000_000` | `11_000_000` | `ml_dsa_verify_host_fn` (`87.yaml`) |
| `universal_state_init_to_account_id` | `8_520_000_000` | `21_471_105` | `universal_accounts` (`87.yaml`) |

(Values from `core/parameters/res/runtime_configs/parameters.yaml:201`–`parameters.yaml:227`; effective at PV 87 per `core/parameters/src/snapshots/near_parameters__config_store__tests__87.json.snap`.)

## Interactions
- **Issuance ← epoch manager.** `EpochManager` calls `calculate_reward` at epoch boundaries and stores `validator_reward`/`minted_amount` in the next-next epoch info; assignment of who validates is [epoch-validators-staking](epoch-validators-staking.md).
- **Fees ← runtime execution.** Per-action gas/compute burning happens in [runtime-execution](runtime-execution.md); this spec covers the resulting burn/reward/refund token flows.
- **Gas price ← block processing.** [chain-block-processing](chain-block-processing.md) supplies `gas_used`/`gas_limit` and stores the resulting `next_gas_price` in the block header; [consensus-finality](consensus-finality.md) verifies total supply.
- **Storage cost ↔ accounts.** Storage-usage byte counts come from [accounts-keys](accounts-keys.md) / [state-storage](state-storage.md); this spec prices them.
- **Parameters ↔ genesis.** `max_inflation_rate`, `protocol_reward_rate`, `num_blocks_per_year`, and gas-price bounds come from genesis / the versioned config stores ([genesis-configuration](genesis-configuration.md), [protocol-versioning](protocol-versioning.md)).

## Protocol-version-gated behavior
Verified against `core/primitives-core/src/version.rs` at this commit (`STABLE_PROTOCOL_VERSION = 87`, `version.rs:680`; `MIN_SUPPORTED_PROTOCOL_VERSION = 84`). Features folded into the ≤84 baseline are now `_Deprecated*` and their behavior is unconditional.

| Feature | Activates | Effect on economics |
|---|---|---|
| **`RemoveGasRewards`** | **PV 87** (`version.rs:477`, mapped at `version.rs:623`) | Purely a parameter change: `burnt_gas_reward` `3/10` → `0/1` (`core/parameters/res/runtime_configs/87.yaml:3`). No code gate — the reward site at `runtime/runtime/src/lib.rs:1108` computes 0 and the credit branch (`lib.rs:1126`) is skipped. Function-call gas that previously went to the callee is now burnt (removed from supply). `ExecutionOutcome.gas_burnt` / `tokens_burnt` are unchanged; only the chunk `balance_burnt` and the receiver's balance differ. |
| `FixMlDsaCostCharging` | PV 87 (`version.rs:487`) | Two fee-pricing fixes, gated by the `fix_ml_dsa_cost_charging` flag (`87.yaml:35`). (a) Gas keys: the SEND fee is priced on the wire length `public_key.len()` instead of the on-trie length (`runtime/runtime/src/config.rs:73` — `gas_key_send_pk_len`), while exec fees keep using `trie_id_len()` (`config.rs:411`, `config.rs:415`). (b) Meta transactions: the inner `DelegateAction` signature-verification *compute* is zeroed on the signer shard (`config.rs:589`) and metered on the receiver shard that actually verifies (`delegate_signature_verification_compute`, `config.rs:615`). Gas charged is unchanged; only compute moves, so token flows are unaffected — the fee *gas* for gas-key sends can change. |
| `FixContractLoadingError` | PV 87 (`version.rs:448`) | A failing `Module::deserialize` is now returned as a cached `FunctionCallError::LoadingError` so it flows through the fee-charge points and finalizes as a gas-bearing abort, instead of a zero-gas runner error (`runtime/near-vm-runner/src/wasmtime_runner/mod.rs:753`, flag `fix_contract_loading_error`, `87.yaml:28`). Economic effect: the contract-loading fee is burnt rather than the receipt costing nothing. |
| `UniversalAccounts` | PV 87 (`version.rs:491`) | Adds the `UniversalStateInit` action, priced by `universal_state_init_fee` (`runtime/runtime/src/config.rs:242`), and a bootstrap transaction path with its own balance/storage-stake checks (`runtime/runtime/src/verifier.rs:424`, storage check at `verifier.rs:493`). `max_state_init_entries` is capped at `1_500` (`87.yaml:61`) so one receipt cannot reserve an unbounded share of congestion gas. |
| `AccountCostIncrease` | PV 85 (`version.rs:434`) | Gas is burnt at `min(purchase, block)` price; the surplus from a *dropped* gas price is refunded to the signer instead of retained as burnt; contract reward (when non-zero) priced at the burn price; a `create_account_charge` (`account_creation_charge`, `0.007 N`) is levied on account creation. Also raises the `create_account` action fee and sets `min_gas_purchase_price = 1e9 yN` (`85.yaml`). Gated at `lib.rs:1038`, `lib.rs:1096`, `lib.rs:1116`, `lib.rs:1320`, `lib.rs:1361`. |
| `PostQuantumSignatures` | PV 85 (`version.rs:408`) | Adds the ML-DSA-65 `SignatureKind` and its non-zero `ml_dsa_65_verification_cost` (`100 Ggas`, `85.yaml:15`) charged as extra *burnt* gas at tx conversion (`runtime/runtime/src/config.rs:504`); ed25519/secp256k1 stay 0. |
| `GasKeys` | PV 85 (`version.rs:604`) | Adds `TransferToGasKey` / `WithdrawFromGasKey`, which move balance between an account and a gas key, priced by `gas_key_transfer_exec_fee` (`core/parameters/src/cost.rs:882`, called at `runtime/runtime/src/config.rs:411`/`:415`), and `AddKey` with gas-key permissions by `gas_key_add_key_exec_fee` (`cost.rs:928`). Also adds the gas-key storage-stake path in the deposit verifier (`runtime/runtime/src/verifier.rs:659`). |
| `one_yocto_on_promise` (flag, no `ProtocolFeature`) | PV 85 (`core/parameters/res/runtime_configs/85.yaml:1`; param `Parameter::OneYoctoOnPromise`, `core/parameters/src/parameter.rs:318`) | A zero-balance contract may attach exactly 1 yoctoNEAR to a promise function call without being debited. The skipped yocto is effectively minted and tracked in `subsidized_amount` (`runtime/near-vm-runner/src/wasmtime_runner/logic.rs:4291`), then netted out of the chunk burn (§3). Effective at PV 87 per the v87 snapshot (`one_yocto_on_promise: true`). |
| Compute costs (NEP-455) | baseline (`_DeprecatedComputeCosts`, PV 61, `version.rs:168`) | Each `Fee`/`ParameterCost` carries a `compute` alongside `gas`. Compute debits the chunk wall-clock budget, not tokens. |
| Reduced gas refunds (NEP-536) | baseline at PV 78 (`version.rs:315`) | `gas_refund_penalty` / `min_gas_refund_penalty` exist and are applied at `lib.rs:1319`, but the values are `0/100` and `0`, so the penalty is currently 0. `pessimistic_gas_price_inflation` was also set to `1/1` at PV 78 (`78.yaml`), making the pessimistic multiplier a no-op. |
| Global-contract deploy burn in outcome | baseline (`_DeprecatedIncludeDeployGlobalContractOutcomeBurntStorage`, PV 83, `version.rs:377`) | Global-contract deploy storage tokens are unconditionally folded into the outcome's `tokens_burnt` (`runtime/runtime/src/global_contracts.rs:50`). |

Other PV-87 features (`EnforceStorageProofLimitForAllActions` `version.rs:473`, `RejectDelegateV2` `version.rs:460`, `RejectEmptyMethodName`, `ReceiptPromiseInputSizeLimit`, `RejectWithdrawFromGasKeyInDelegate`, `EarlyKickout` `version.rs:395`, `GlobalContractSameChunkCallFix`) and the PV-86 `EnforcePerReceiptStorageProofLimit` (`version.rs:466`) do not change token flows. `ClampOutgoingGasAdmission` (PV 85, `version.rs:444`) bounds gas admission, not tokens.

Upgrade scheduling at this commit: mainnet has an **empty** voting schedule (votes for `PROTOCOL_VERSION` immediately once nodes upgrade) and testnet is scheduled to vote PV 87 on 2026-09-21 00:00 UTC (`core/primitives/src/version.rs:58`–`version.rs:75`).

## Invariants & failure modes
- **Non-negative supply arithmetic.** `new_total_supply` uses checked add/sub and panics on overflow/underflow at block construction (`core/primitives/src/block.rs:195`); the verification path returns `None` instead of panicking (`block.rs:343`).
- **Burn ≥ subsidy.** `total_balance_burnt = burnt − subsidized_amount` errors with "subsidized amount exceeds total burnt balance" if the subsidy would exceed the burn (`chain/chain/src/runtime/mod.rs:405`).
- **Minted ≤ cap.** `epoch_actual_reward` ≤ `epoch_total_reward` because per-validator rewards are fractions of the pool and below-threshold validators earn 0 (`reward_calculator.rs:109`). Asserted by `test_adjust_max_inflation` (`reward_calculator.rs:591`).
- **Burn price ≤ purchase price (post-`AccountCostIncrease`).** `min(purchase, block)` guarantees refunds (and any contract reward) never mint new tokens or underflow (comment at `runtime/runtime/src/lib.rs:1040`).
- **Refund penalty ≤ refund.** `gas_penalty_for_gas_refund` clamps to `min(penalty, gas_refund)` (`core/parameters/src/cost.rs:745`).
- **`gas_burnt_for_function_call ≤ gas_burnt`.** Asserted when merging action results (`runtime/runtime/src/lib.rs:488`).
- **Gas-price bounds.** Always clamped to `[min_gas_price, max_gas_price]` and unchanged on skipped blocks (`block.rs:464`, `block.rs:486`).
- **Storage staking.** An account failing `check_storage_stake` and not zero-balance cannot transact or receive deposits that push it under the threshold (`runtime/runtime/src/verifier.rs:375`, `verifier.rs:659`), and a receipt that leaves it under the threshold is rolled back with `ActionErrorKind::LackBalanceForState` (`runtime/runtime/src/lib.rs:1011`).
- **Contract reward requires a live account.** If the receiver account is gone at the end of execution, its reward is not credited and remains burnt (`runtime/runtime/src/lib.rs:1126`). Vacuous at PV 87 since the reward is always 0.
- **Config store no longer covers pre-84 versions.** `CONFIG_DIFFS` starts at 53 and `MIN_SUPPORTED_PROTOCOL_VERSION` is 84 (`core/parameters/src/config_store.rs:26`); replaying blocks from removed versions is out of scope for this binary.

## Code anchors
| Location | Symbol | What happens here |
|---|---|---|
| `chain/epoch-manager/src/reward_calculator.rs:51` | `RewardCalculator::calculate_reward` | Per-epoch issuance + treasury/validator split |
| `chain/epoch-manager/src/reward_calculator.rs:69` | `epoch_total_reward` | Time-proportional slice of max-inflation cap |
| `chain/epoch-manager/src/reward_calculator.rs:63` | `use_hardcoded_value` | `protocol_reward_rate` forced to 1/10 for prod-genesis chains |
| `chain/epoch-manager/src/reward_calculator.rs:109` | uptime gate | Below `online_min_threshold` ⇒ zero reward |
| `chain/epoch-manager/src/lib.rs:1176` | `calculate_reward` call site | Feeds `total_supply`, `epoch_duration`, `max_inflation_rate` at epoch boundary |
| `core/primitives/src/block.rs:195` | `new_total_supply` | supply += minted − burnt |
| `core/primitives/src/block.rs:455` | `compute_next_gas_price_checked` | Load-feedback gas-price adjustment + clamp |
| `chain/chain/src/types.rs:199` | `BlockEconomicsConfig::min_gas_price` | Effective min gas price; NEP-92-fix override |
| `chain/chain/src/types.rs:207` | `BlockEconomicsConfig::max_gas_price` | `min(genesis_max, min*20)` |
| `chain/chain/src/runtime/mod.rs:405` | `total_balance_burnt` | Burn total minus the 1-yN subsidy |
| `core/primitives/src/chunk_apply_stats.rs:243` | `BalanceStatsV1` | Per-chunk burn/subsidy/deficit ledger |
| `runtime/runtime/src/config.rs:481` | `calculate_tx_cost` | Splits tx into burnt vs purchased gas |
| `runtime/runtime/src/config.rs:532` | `receipt_gas_price` | `max(current, min_gas_purchase_price)` |
| `runtime/runtime/src/config.rs:589` | `signature_verification_cost` | ML-DSA-65 surcharge; inner-delegate compute moved off signer shard |
| `runtime/runtime/src/config.rs:73` | `gas_key_send_pk_len` | `FixMlDsaCostCharging`: send fee on `len()`, exec on `trie_id_len()` |
| `runtime/runtime/src/lib.rs:1038` | `gas_burn_price` | `min(purchase, block)` under `AccountCostIncrease` |
| `runtime/runtime/src/lib.rs:1093` | `tx_burnt_amount` | Tokens burnt for a receipt |
| `runtime/runtime/src/lib.rs:1105` | `tokens_burnt` | Outcome value, snapshotted before any contract reward |
| `runtime/runtime/src/lib.rs:1108` | `receiver_gas_reward` | `burnt_gas_reward` share — **0 at PV 87** |
| `runtime/runtime/src/lib.rs:1284` | `refund_unspent_gas_and_deposits` | Refund of unused gas + deposits, penalties, account charge |
| `runtime/runtime/src/global_contracts.rs:35` | global-contract storage burn | `0.0001 N`/byte burnt from the deployer |
| `core/parameters/src/cost.rs:736` | `gas_penalty_for_gas_refund` | NEP-536 refund penalty formula |
| `core/parameters/src/cost.rs:568` | `RuntimeFeesConfig` | Fee map, reward/penalty ratios, storage config |
| `core/parameters/src/cost.rs:618` | `StorageUsageConfig` | `storage_amount_per_byte` and friends |
| `core/parameters/src/config_store.rs:26` | `CONFIG_DIFFS` | Per-version parameter diffs; now starts at `53.yaml` |
| `core/parameters/src/config_store.rs:152` | genesis override | Genesis runtime config replaces **only** the key-0 entry |
| `core/parameters/src/config_store.rs:237` | `get_config` | Greatest stored version ≤ requested ⇒ `87.yaml` wins at PV 87 |
| `core/parameters/res/runtime_configs/87.yaml:3` | `burnt_gas_reward` | 3/10 → 0/1 |
| `runtime/runtime/src/verifier.rs:48` | `check_storage_stake` | Storage-staking enforcement |
| `runtime/near-vm-runner/src/wasmtime_runner/logic.rs:4291` | one-yocto exemption | Zero-balance contract subsidy (effectively minted) |
| `runtime/runtime/src/function_call.rs:223` | `result.subsidized_amount` | VM outcome subsidy folded into the action result |
| `runtime/runtime/src/lib.rs:1269` | `ExecutionOutcome.tokens_burnt` | Snapshot moved into the outcome, pre-reward |
| `core/primitives-core/src/version.rs:477` | `RemoveGasRewards` (PV 87) | Headline change: no contract gas reward |
| `core/primitives-core/src/version.rs:487` | `FixMlDsaCostCharging` (PV 87) | Gas-key fee basis + meta-tx verify compute shard |
| `core/primitives-core/src/version.rs:448` | `FixContractLoadingError` (PV 87) | Loading failure burns the contract-loading fee |
| `core/primitives-core/src/version.rs:434` | `AccountCostIncrease` (PV 85) | Burn-price / surplus-refund / account-charge changes |

## Open questions
- **Could a custom parameter table still pay gas rewards at PV ≥ 87?** *Not on mainnet or testnet.* `RemoveGasRewards` is parameter-only, but the parameter table is not chain-configurable at PV 87: `RuntimeConfigStore::new`'s `genesis_runtime_config` override replaces **only the key `0`** entry (`core/parameters/src/config_store.rs:152`), and `get_config` returns the greatest key ≤ the protocol version (`config_store.rs:237`), which at PV 87 is the compiled-in `87.yaml` entry (`config_store.rs:58`). Testnet's `initial_testnet_config` therefore affects nothing at PV 87 (`config_store.rs:167`). The only ways to get a non-zero `burnt_gas_reward` at PV ≥ 87 are (a) a recompiled binary with a patched `parameters.yaml`/diff (they are compile-time `include_config!`s), (b) a cargo-feature variant such as `calimero_zero_storage` (`config_store.rs:100`) or a hardcoded chain override like `BENCHMARKNET` (`config_store.rs:173`), or (c) test-only constructors (`with_one_config`/`new_custom`, `config_store.rs:209`/`:214`). All of those are custom chains running a forked binary — a node doing that on mainnet/testnet would simply fall out of consensus. Remaining uncertainty is only editorial: whether the authors *intended* the stabilization to be parameter-only rather than also code-gated.
- The comment at `runtime/runtime/src/lib.rs:1129` ("Validators receive the remaining execution reward…") is **stale and wrong** for this tree: the un-credited remainder stays in `tx_burnt_amount`, flows into the chunk's `balance_burnt` (`chain/chain/src/runtime/mod.rs:405`) and is *subtracted from total supply* (`core/primitives/src/block.rs:195`) — destroyed, not paid to anyone. Validator income is exclusively the §1 inflation pool. Not corrected in code at this commit.
- `gas_deficit_amount` is accumulated (`runtime/runtime/src/lib.rs:1072`) but never consumed by supply accounting or block validation in this tree; its only consumer appears to be chunk-apply stats/telemetry.
