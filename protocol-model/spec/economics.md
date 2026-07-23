# Economics

> Protocol version: 86 (stable) · Release: 2.13.0 · Derived from commit: 499283a · Generated: 2026-07-09
> Primary crates/files: `chain/epoch-manager/src/reward_calculator.rs`, `runtime/runtime/src/lib.rs`, `runtime/runtime/src/config.rs`, `runtime/runtime/src/verifier.rs`, `core/parameters/src/cost.rs`, `core/parameters/src/config.rs`, `core/primitives/src/block.rs`, `chain/chain/src/types.rs`

## Role
Economics is the set of monetary rules realized by the implementation: per-epoch token issuance (inflation) and the validator-reward / protocol-treasury split; block-to-block gas-price adjustment toward a target load; which gas/fees are *burnt* (removed from total supply) versus *redistributed* (to contracts / validators / treasury); the price-difference and NEP-536 penalty handling of gas refunds; and storage staking. It does *not* decide who gets reward — validator reward *assignment* lives in [epoch-validators-staking](epoch-validators-staking.md). Fee *amounts* and gas accounting during action execution live in [runtime-execution](runtime-execution.md); this spec covers the monetary effect (burning, rewards, total-supply change). Parameters originate from genesis and the versioned `RuntimeConfig`/`EpochConfig` stores ([genesis-configuration](genesis-configuration.md)). Storage byte-cost enforcement ties into [accounts-keys](accounts-keys.md) and [state-storage](state-storage.md).

## Key data structures
- **`RewardCalculator`** — `chain/epoch-manager/src/reward_calculator.rs:27` — inputs to issuance: `num_blocks_per_year`, `epoch_length`, `protocol_reward_rate` (`Rational32`), `protocol_treasury_account`, `num_seconds_per_year` (always `NUM_SECONDS_IN_A_YEAR = 24*60*60*365`, `reward_calculator.rs:10`), and `genesis_protocol_version`. Built from `GenesisConfig` in `new` (`reward_calculator.rs:37`).
- **`ValidatorOnlineThresholds`** — `chain/epoch-manager/src/reward_calculator.rs:14` — `online_min_threshold`/`online_max_threshold` (rationals) bound the linear uptime→reward ramp; optional `endorsement_cutoff_threshold` (0–100 percent) remaps a validator's endorsement ratio to 0 or 1 before averaging.
- **`RuntimeFeesConfig`** — `core/parameters/src/cost.rs:517` — per-action `Fee` map (`action_fees`), `storage_usage_config`, `burnt_gas_reward` (`Rational32`, the share of function-call burnt gas paid to the receiver contract account), `pessimistic_gas_price_inflation_ratio`, `gas_refund_penalty` + `min_gas_refund_penalty` (NEP-536), and `signature_verification_costs` (per-`SignatureKind`, all 0 except ML-DSA-65).
- **`Fee`** — `core/parameters/src/cost.rs:17` — `send_sir` / `send_not_sir` / `execution` fee components; `send_fee(sir)` (`cost.rs:37`) and `exec_fee()` (`cost.rs:41`) return a `ParameterCost { gas, compute }`.
- **`StorageUsageConfig`** — `core/parameters/src/cost.rs:568` — `storage_amount_per_byte` (yN backing each storage byte), `num_bytes_account`, `num_extra_bytes_record`, `global_contract_storage_amount_per_byte`.
- **`RuntimeConfig`** — `core/parameters/src/config.rs:17` — top-level runtime params; economics-relevant fields: `min_gas_purchase_price` (`config.rs:42`) and `account_creation_charge` (`config.rs:46`), plus `fees`/`wasm_config`.
- **`BlockEconomicsConfig`** — `chain/chain/src/types.rs:182` — gas-price bounds derived from genesis: `gas_price_adjustment_rate`, `genesis_min_gas_price`, `genesis_max_gas_price`, `genesis_protocol_version`.
- **`GasRefundResult`** — `runtime/runtime/src/lib.rs:498` — `price_deficit`, `price_surplus`, `refund_penalty`, and private `create_account_charge`, produced while refunding unspent gas.
- **`TransactionCost`** — `runtime/runtime/src/config.rs:20` — output of `tx_cost`: `gas_burnt`, `compute_burnt`, `gas_remaining`, `receipt_gas_price`, `burnt_amount`, `gas_cost`, `deposit_cost`, `total_cost`.

## Behavior

### 1. Per-epoch issuance (inflation)
`RewardCalculator::calculate_reward` computes the newly minted tokens for an epoch (`chain/epoch-manager/src/reward_calculator.rs:51`). It is called at each epoch boundary from `EpochManager` with the previous block's total supply and the epoch's `max_inflation_rate` (`chain/epoch-manager/src/lib.rs:849`).

1. The epoch total reward is a *time-proportional slice* of the annual max-inflation cap (`reward_calculator.rs:69`):
   `epoch_total_reward = max_inflation_rate.numer * total_supply * epoch_duration / (num_seconds_per_year * max_inflation_rate.denom * NUM_NS_IN_SECOND)`.
   `epoch_duration` is in nanoseconds (actual wall-clock epoch length, `lib.rs:827`), so a shorter/longer epoch mints proportionally less/more. `max_inflation_rate` on mainnet is `1/40` (2.5%) since PV 81 (`core/primitives/res/epoch_configs/mainnet/81.json`, `max_inflation_rate`), reduced from `1/20` (5%; `.../80.json`); it is still `1/40` at PV 86 (`.../85.json`).
2. `protocol_reward_rate` is forced to `1/10` when `genesis_protocol_version == PROD_GENESIS_PROTOCOL_VERSION` (29), otherwise taken from config (`reward_calculator.rs:63`).
3. The treasury slice is `epoch_protocol_treasury = epoch_total_reward * protocol_reward_rate` and is credited to `protocol_treasury_account` (`reward_calculator.rs:78`).
4. If there are no validators, the function returns early with the treasury entry in the map but an `epoch_actual_reward` of `Balance::ZERO` (`reward_calculator.rs:85`), so nothing is added to supply that epoch.
5. The validator pool is the remainder `epoch_total_reward - epoch_protocol_treasury` (`reward_calculator.rs:88`), distributed stake-weighted and uptime-scaled (see §2).
6. Returns `(rewards_map, epoch_actual_reward)` where `epoch_actual_reward` is the *actual* sum minted — treasury plus each validator's earned reward (`reward_calculator.rs:145`). Validators below the online-min threshold earn 0, so `epoch_actual_reward` can be less than `epoch_total_reward`; only `epoch_actual_reward` (returned as `minted_amount`) is added to supply.

### 2. Validator reward (uptime ramp)
For each validator (`reward_calculator.rs:94`):
1. `production_ratio` = its combined online ratio across blocks/chunks/endorsements (`get_validator_online_ratio`; endorsement ratio first cut to 0/1 by `endorsement_cutoff_threshold` if set).
2. If `production_ratio < online_min_threshold`, or all of expected blocks/chunks/endorsements are 0, reward is 0 (`reward_calculator.rs:109`).
3. Otherwise the online multiplier is `min(1, (uptime − online_min)/(online_max − online_min))`, and the reward is `epoch_validator_reward * multiplier * stake / total_stake` (`reward_calculator.rs:133`). All arithmetic is exact integer `U256`/`U512` rational math.

### 3. Total-supply change per block
`new_total_supply = prev.total_supply + minted_amount − balance_burnt` (`core/primitives/src/block.rs:193`). `minted_amount` is `Some` only on the first block of an epoch, taken from the epoch info populated by `calculate_reward` (`chain/chain/src/chain.rs:2519`). `balance_burnt` is the sum of each included chunk's `prev_balance_burnt()` (`block.rs:152`). Thus inflation *adds* to supply once per epoch, and burnt fees *subtract* every block; the difference is net issuance.

### 4. Gas-price adjustment
The next block's gas price is a load-feedback controller (`core/primitives/src/block.rs:440` — `compute_next_gas_price_checked`):
- Formula (`block.rs:417`): `next_gas_price = gas_price * (1 + (gas_used/gas_limit − 1/2) * adjustment_rate)`. Implemented as the exact integer ratio `numerator/denominator` at `block.rs:460`. When utilization is exactly 50% the price is unchanged; above 50% it rises, below it falls.
- If the block was skipped (`gas_limit == 0`) the price is unchanged (`block.rs:449`).
- The result is clamped to `[min_gas_price, max_gas_price]` (`block.rs:471`).
- `min_gas_price` is `MIN_GAS_PRICE_NEP_92_FIX` (`100_000_000` yN) for chains whose genesis is `PROD_GENESIS_PROTOCOL_VERSION`, else the genesis value (`chain/chain/src/types.rs:199`, const at `core/primitives/src/version.rs:29`). `max_gas_price` is `min(genesis_max_gas_price, min_gas_price * 20)` (`types.rs:207`, `MAX_GAS_MULTIPLIER = 20` at `types.rs:191`).
- The chain applies this over one block via `compute_next_gas_price_checked` (called at `block.rs:182`); under Spice it folds over certified results (`compute_gas_price_from_certified_results_checked`, `block.rs:479`).

### 5. Transaction cost & what is burnt at conversion
`calculate_tx_cost` (`runtime/runtime/src/config.rs:417`) splits a transaction's cost:
1. `burnt` = `new_action_receipt` send fee + `total_send_fees` of the actions (`config.rs:427`) + the signature-verification surcharge (`signature_verification_cost`, `config.rs:440`). This gas is burnt *immediately* at `current_gas_price` (`burnt_amount`, `config.rs:459`).
2. `gas_remaining` = attached (prepaid) function-call gas + prepaid send/exec fees + the wrapping receipt's exec fee (`config.rs:453`). This gas is *purchased* (not yet burnt) at `receipt_gas_price = max(current_gas_price, min_gas_purchase_price)` (`config.rs:464`).
3. `gas_cost = burnt_amount + remaining_gas_amount` (`config.rs:466`) and `total_cost = gas_cost + total_deposit` (`config.rs:468`).

Send/exec fees per action are computed by `total_send_fees` (`config.rs:71`) and `exec_fee` (`config.rs:279`), each reading the `Fee` for the action's `ActionCosts` and adding per-byte components. Transfers add implicit-account-creation fees via `transfer_send_fee`/`transfer_exec_fee` (`core/parameters/src/cost.rs:750`/`722`).

### 6. Gas burning, rewards, and refunds during receipt execution
In `Runtime::apply_action_receipt` (`runtime/runtime/src/lib.rs`), after actions run:
1. **Gas burn price** — with `AccountCostIncrease` enabled, gas is burnt at `min(gas_purchase_price, apply_state.gas_price)`; before it, at `apply_state.gas_price` (`lib.rs:920`).
2. **Refunds** — `refund_unspent_gas_and_deposits` (`lib.rs:1166`) refunds unspent gas and, on failure, the full deposit. `gross_gas_refund` = prepaid gas + prepaid exec gas − gas actually used/burnt (`lib.rs:1186`). It then:
   - Computes the NEP-536 penalty `gas_penalty_for_gas_refund(gross)` = `min(gross, max(gross * gas_refund_penalty, min_gas_refund_penalty))` (`core/parameters/src/cost.rs:683`) at `gas_burn_price` (post-`AccountCostIncrease`) or `gas_purchase_price` (before) (`lib.rs:1201`). The unused-gas refund is issued at `gas_purchase_price` minus this penalty (`lib.rs:1210`).
   - Records `price_deficit` if the burn price rose above purchase price, else `price_surplus` (`lib.rs:1220`). With `AccountCostIncrease`, the surplus is refunded to the signer (`burned_gas_refund`, `lib.rs:1235`); before, it was retained as burnt.
   - If a new account was created (post-`AccountCostIncrease`), an extra `create_account_charge` = `min(desired = account_creation_charge − already-burned, burned_gas_refund)` is subtracted from the refund (`lib.rs:1243`).
3. **tx_burnt_amount** — burnt tokens for the receipt = `gas_burn_price * gas_burnt − price_deficit` (`lib.rs:975`), plus `refund_penalty`, `create_account_charge`, and `result.tokens_burnt`; pre-`AccountCostIncrease` also plus `price_surplus` (`lib.rs:978`). System/refund receipts burn 0 gas (`lib.rs:972`).
4. **Contract reward** — of the gas burnt *for function calls*, a `burnt_gas_reward` fraction (mainnet `3/10`) is paid to the receiver account and *subtracted* from `tx_burnt_amount` (`lib.rs:990`). Reward is priced at `gas_burn_price` post-`AccountCostIncrease`, else `gas_purchase_price` (`lib.rs:998`). If the receiver account no longer exists, the whole amount stays burnt (goes to validators via §3). The remaining `tx_burnt_amount` accumulates into `stats.balance.tx_burnt_amount` (`lib.rs:1023`) and ultimately reduces total supply as `balance_burnt`.

### 7. Storage staking
`check_storage_stake` (`runtime/runtime/src/verifier.rs:48`) enforces that an account backs its storage: `required = storage_amount_per_byte * account.storage_usage()` (`verifier.rs:54`), `available = amount + locked` (`verifier.rs:64`). If `available < required` and the account is not a zero-balance account (`storage_usage() <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT`, `verifier.rs:88`, NEP-448), it returns `LackBalanceForStorageStaking(required − available)` (`verifier.rs:80`). At transaction verification this becomes `InvalidTxError::LackBalanceForState` (`verifier.rs:347`); on the deposit path it becomes a `DepositCostFailureReason::LackBalanceForState` action error (`verifier.rs:520`). `storage_amount_per_byte` on mainnet is `1e19` yN (i.e. 1 NEAR backs 100,000 bytes ≈ 100 KB) (`core/parameters/res/runtime_configs/parameters.snap` — `storage_amount_per_byte`).

## Interactions
- **Issuance ← epoch manager.** `EpochManager` calls `calculate_reward` at epoch boundaries and stores `validator_reward`/`minted_amount` in the next-next epoch info; assignment of who validates is [epoch-validators-staking](epoch-validators-staking.md).
- **Fees ← runtime execution.** Per-action gas/compute burning happens in [runtime-execution](runtime-execution.md); this spec covers the resulting burn/reward/refund token flows.
- **Gas price ← block processing.** [chain-block-processing](chain-block-processing.md) supplies `gas_used`/`gas_limit` and stores the resulting `next_gas_price` in the block header; [consensus-finality](consensus-finality.md) verifies total supply.
- **Storage cost ↔ accounts.** Storage-usage byte counts come from [accounts-keys](accounts-keys.md) / [state-storage](state-storage.md); this spec prices them.
- **Parameters ↔ genesis.** `max_inflation_rate`, `protocol_reward_rate`, `num_blocks_per_year`, and gas-price bounds come from genesis / the versioned config stores ([genesis-configuration](genesis-configuration.md), [protocol-versioning](protocol-versioning.md)).

## Protocol-version-gated behavior
Verified against `core/primitives-core/src/version.rs` in this tree (PV 86 = 2.13.0 stable). NEP-536 (reduced gas refunds) is *not* a live gate here — it is baseline: `_DeprecatedReducedGasRefunds` at PV 78 (`version.rs:545`). Likewise `pessimistic_gas_price_inflation` was set to `1/1` at PV 78 (`core/parameters/res/runtime_configs/78.yaml`), so the pessimistic multiplier is a no-op at PV 86.

| Feature | Activates | Effect on economics |
|---|---|---|
| `AccountCostIncrease` | PV 85 (`version.rs:574`) | Gas now burnt at `min(purchase, block)` price; the surplus from a *dropped* gas price is refunded to the signer instead of retained as burnt; contract reward priced at the burn price; a `create_account_charge` (`account_creation_charge`, mainnet `0.007 N`) is levied on account creation. Also raises the `create_account` action fee and sets `min_gas_purchase_price = 1e9 yN` (`core/parameters/res/runtime_configs/85.yaml`). Gated at `lib.rs:920`, `lib.rs:978`, `lib.rs:998`, `lib.rs:1202`, `lib.rs:1243`. |
| Compute costs (NEP-455) | baseline (`_DeprecatedComputeCosts`, PV 61, `version.rs:506`) | Each `Fee`/`ParameterCost` carries a `compute` alongside `gas`; the signature-verification surcharge can set compute independently of gas (`runtime/runtime/src/config.rs:512` — `signature_verification_cost`). Compute debits the chunk wall-clock budget, not tokens. |
| `PostQuantumSignatures` | PV 85 (`version.rs:567`) | Adds the ML-DSA-65 `SignatureKind` and its non-zero `ml_dsa_65_verification_cost` (`100 Ggas`, `85.yaml`) charged as extra *burnt* gas at tx conversion (`config.rs:512`); ed25519/secp256k1 stay 0. |
| Reduced gas refunds (NEP-536) | baseline at PV 78 (`version.rs:545`) | `gas_refund_penalty` / `min_gas_refund_penalty` fields exist and are applied at `lib.rs:1201`, but mainnet values are `0/100` and `0` (`parameters.snap`), so the penalty is currently 0. |
| `IncludeDeployGlobalContractOutcomeBurntStorage` | PV 83 (`version.rs:552`) | Global-contract deploy storage tokens are folded into the outcome's `tokens_burnt`. |

The PV-86 feature on this release is `EnforcePerReceiptStorageProofLimit` (`version.rs:576`); it constrains witness storage proofs and does not change token flows. There is no `FixContractLoadingError` on 2.13.0. Mainnet is scheduled to vote PV 86 on 2026-07-20 (`core/primitives/src/version.rs:59`).

## Invariants & failure modes
- **Non-negative supply arithmetic.** `new_total_supply` uses checked add/sub and panics on overflow/underflow (`block.rs:193`); a block whose `balance_burnt` exceeds `prev + minted` is impossible by construction.
- **Minted ≤ cap.** `epoch_actual_reward` ≤ `epoch_total_reward` because per-validator rewards are fractions of the pool and below-threshold validators earn 0 (`reward_calculator.rs:109`). Verified by `test_adjust_max_inflation` (`reward_calculator.rs:590`).
- **Burn price ≤ purchase price (post-`AccountCostIncrease`).** `min(purchase, block)` guarantees the receiver reward and refunds never mint new tokens or underflow (comment at `lib.rs:922`).
- **Refund penalty ≤ refund.** `gas_penalty_for_gas_refund` clamps to `min(penalty, gas_refund)` (`cost.rs:692`).
- **Gas-price bounds.** Always clamped to `[min_gas_price, max_gas_price]` and unchanged on skipped blocks (`block.rs:449`, `block.rs:471`).
- **Storage staking.** An account failing `check_storage_stake` and not zero-balance cannot transact / receive deposits that push it under the threshold: `LackBalanceForState` (`verifier.rs:347`, `verifier.rs:520`).
- **Contract reward requires a live account.** If the receiver account is gone at the end of execution, its reward is not credited and remains burnt (goes to validators) (`lib.rs:1008`).

## Code anchors
| Location | Symbol | What happens here |
|---|---|---|
| `chain/epoch-manager/src/reward_calculator.rs:51` | `RewardCalculator::calculate_reward` | Per-epoch issuance + treasury/validator split |
| `chain/epoch-manager/src/reward_calculator.rs:69` | `epoch_total_reward` | Time-proportional slice of max-inflation cap |
| `chain/epoch-manager/src/reward_calculator.rs:63` | `use_hardcoded_value` | `protocol_reward_rate` forced to 1/10 for prod-genesis chains |
| `chain/epoch-manager/src/lib.rs:849` | `calculate_reward` call site | Feeds `total_supply` and `max_inflation_rate` at epoch boundary |
| `core/primitives/src/block.rs:193` | `new_total_supply` | supply += minted − burnt |
| `core/primitives/src/block.rs:440` | `compute_next_gas_price_checked` | Load-feedback gas-price adjustment + clamp |
| `chain/chain/src/types.rs:199` | `BlockEconomicsConfig::min_gas_price` | Effective min gas price; NEP-92-fix override |
| `chain/chain/src/types.rs:207` | `BlockEconomicsConfig::max_gas_price` | `min(genesis_max, min*20)` |
| `runtime/runtime/src/config.rs:417` | `calculate_tx_cost` | Splits tx into burnt vs purchased gas |
| `runtime/runtime/src/config.rs:464` | `receipt_gas_price` | `max(current, min_gas_purchase_price)` |
| `runtime/runtime/src/lib.rs:920` | `gas_burn_price` | `min(purchase, block)` under `AccountCostIncrease` |
| `runtime/runtime/src/lib.rs:975` | `tx_burnt_amount` | Tokens burnt for a receipt |
| `runtime/runtime/src/lib.rs:990` | `receiver_gas_reward` | `burnt_gas_reward` share paid to contract |
| `runtime/runtime/src/lib.rs:1166` | `refund_unspent_gas_and_deposits` | Refund of unused gas + deposits, penalties |
| `core/parameters/src/cost.rs:683` | `gas_penalty_for_gas_refund` | NEP-536 refund penalty formula |
| `core/parameters/src/cost.rs:517` | `RuntimeFeesConfig` | Fee map, reward/penalty ratios, storage config |
| `core/parameters/src/cost.rs:568` | `StorageUsageConfig` | `storage_amount_per_byte` and friends |
| `runtime/runtime/src/verifier.rs:48` | `check_storage_stake` | Storage-staking enforcement |
| `core/primitives-core/src/version.rs:574` | `AccountCostIncrease` (PV 85) | Burn-price / surplus-refund / account-charge changes |
| `core/primitives-core/src/version.rs:576` | `EnforcePerReceiptStorageProofLimit` (PV 86) | The PV-86 feature (no token effect) |

## Open questions
None.
