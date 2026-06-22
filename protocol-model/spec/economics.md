# Economics

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `chain/epoch-manager/src/reward_calculator.rs`, `runtime/runtime/src/config.rs`, `runtime/runtime/src/lib.rs`, `core/parameters/src/cost.rs`, `core/parameters/src/config.rs`, `core/primitives/src/block.rs`, `chain/chain/src/types.rs`, `runtime/runtime/src/verifier.rs`

## Role
Economics is the set of monetary rules realized by the implementation: per-epoch token issuance (inflation) and the validator-reward / protocol-treasury split; block-to-block gas-price adjustment; which gas/fees are burnt (removed from total supply) versus redistributed; gas refunds and the NEP-536 gas-refund penalty; and storage staking. It does not decide *who* gets reward — that is [epoch-validators-staking](epoch-validators-staking.md). The fee *amounts* and gas accounting during execution live in [runtime-execution](runtime-execution.md); this spec covers their monetary effect (burning, rewards, total-supply changes). Parameters originate from genesis ([genesis-configuration](genesis-configuration.md)) and the versioned `RuntimeConfig` / `EpochConfig` stores. Storage byte-cost enforcement ties into [accounts-keys](accounts-keys.md) and [state-storage](state-storage.md).

## Key data structures
- **`RewardCalculator`** — `chain/epoch-manager/src/reward_calculator.rs:26` — holds the inputs to issuance: `num_blocks_per_year`, `epoch_length`, `protocol_reward_rate` (`Rational32`), `protocol_treasury_account`, `num_seconds_per_year` (always `NUM_SECONDS_IN_A_YEAR = 24*60*60*365`), and `genesis_protocol_version`. Built from `GenesisConfig` in `new` (`reward_calculator.rs:37`).
- **`ValidatorOnlineThresholds`** — `chain/epoch-manager/src/reward_calculator.rs:14` — `online_min_threshold`/`online_max_threshold` (rationals) bound the linear uptime→reward ramp; optional `endorsement_cutoff_threshold` remaps a validator's endorsement ratio to 0/1 before averaging.
- **`RuntimeFeesConfig`** — `core/parameters/src/cost.rs:517` — per-action `Fee` map, `storage_usage_config`, `burnt_gas_reward` (`Rational32`, the share of function-call burnt gas paid to the contract account), `pessimistic_gas_price_inflation_ratio`, `gas_refund_penalty` + `min_gas_refund_penalty` (NEP-536), and `signature_verification_costs` (per-`SignatureKind`, all 0 except ML-DSA-65).
- **`Fee`** — `core/parameters/src/cost.rs:16` — `send_sir` / `send_not_sir` / `execution` fee components; `send_fee(sir)` and `exec_fee()` return a `ParameterCost { gas, compute }` (`cost.rs:79`).
- **`StorageUsageConfig`** — `core/parameters/src/cost.rs:567` — `storage_amount_per_byte` (yN backing each storage byte), `num_bytes_account`, `num_extra_bytes_record`, `global_contract_storage_amount_per_byte`.
- **`RuntimeConfig`** — `core/parameters/src/config.rs:16` — top-level runtime params; economics-relevant fields: `min_gas_purchase_price`, `account_creation_charge`, plus `fees`/`wasm_config`.
- **`BlockEconomicsConfig`** — `chain/chain/src/types.rs:182` — gas-price bounds derived from genesis: `gas_price_adjustment_rate`, `genesis_min_gas_price`, `genesis_max_gas_price`, `genesis_protocol_version`.
- **`GasRefundResult`** — `runtime/runtime/src/lib.rs:498` — `price_deficit`, `price_surplus`, `refund_penalty`, `create_account_charge` produced while refunding unspent gas.

## Behavior

### 1. Per-epoch issuance (inflation) and treasury split
`RewardCalculator::calculate_reward` (`reward_calculator.rs:51`) runs at epoch finalization, called from `EpochManager` (`chain/epoch-manager/src/lib.rs:849`) with `total_supply` taken from the last block of the closing epoch (`lib.rs:852`) and `epoch_duration` measured in nanoseconds between the first block of the new epoch and the last block of the previous epoch (`lib.rs:827`).

1. **Epoch total reward** (newly minted tokens), `reward_calculator.rs:69`:
   `epoch_total_reward = max_inflation_rate.numer * total_supply * epoch_duration / (num_seconds_per_year * max_inflation_rate.denom * 1e9)`.
   This is `max_inflation_rate` annualized and pro-rated by the epoch's wall-clock duration; `max_inflation_rate` comes from the per-version `EpochConfig` (`lib.rs:856`), **not** from `RuntimeConfig`.
2. **Protocol treasury cut**, `reward_calculator.rs:78`: `epoch_protocol_treasury = epoch_total_reward * protocol_reward_rate`. The treasury account (`protocol_treasury_account`) is inserted into the reward map first (`reward_calculator.rs:84`). For chains whose genesis is `PROD_GENESIS_PROTOCOL_VERSION` (29) the rate is hardcoded to `1/10` (`reward_calculator.rs:63`); otherwise the genesis `protocol_reward_rate` is used.
3. **Validator pool**, `reward_calculator.rs:88`: `epoch_validator_reward = epoch_total_reward - epoch_protocol_treasury`, distributed pro-rata by stake and scaled by an uptime multiplier `min(1, (uptime - online_min)/(online_max - online_min))` (`reward_calculator.rs:119`-`140`). Below `online_min_threshold`, or when a validator has zero expected blocks/chunks/endorsements, reward is `Balance::ZERO` (`reward_calculator.rs:109`).
4. Returns `(rewards_map, epoch_actual_reward)`. `epoch_actual_reward` is the *minted_amount*: treasury cut plus the sum of validators that actually qualified (`reward_calculator.rs:90`, `:143`) — not necessarily the full `epoch_total_reward` (unqualified validators' shares are never minted). With no validators the function early-returns the treasury entry in the map but `epoch_actual_reward = Balance::ZERO` (`reward_calculator.rs:85`-`86`), so nothing is reported as minted that epoch.

This `minted_amount` is carried into the next-next `EpochInfo` (`lib.rs:889`) and surfaces at the epoch boundary.

### 2. Total-supply change per block
Each block recomputes total supply: `new_total_supply = prev_total_supply + minted_amount - balance_burnt` (`core/primitives/src/block.rs:331`, `verify_total_supply_checked`). `minted_amount` is `Some` only on the first block of an epoch, read from the next epoch's `EpochInfo::minted_amount()` (`chain/chain/src/chain.rs:2519`); otherwise it is `None`/zero. `balance_burnt` is the tokens burnt by execution in the block. If `balance_burnt > prev + minted` the check fails (treated as invalid balance burnt, `block.rs:334`).

### 3. Gas-price adjustment (block to block)
`Block::compute_next_gas_price_checked` (`core/primitives/src/block.rs:440`) sets the price for the *next* block toward a 50%-utilization target:
`next_gas_price = gas_price * (1 + (gas_used/gas_limit - 1/2) * adjustment_rate)`, computed in integer arithmetic (`block.rs:460`-`467`) and then clamped into `[min_gas_price, max_gas_price]` (`block.rs:471`). If `gas_limit == 0` (skipped block) the price is unchanged (`block.rs:449`). The header's `next_gas_price` is verified against this formula in `verify_gas_price_checked` (`block.rs:385`); under SPICE the price is instead derived from certified execution results (`block.rs:394`).

Bounds come from `BlockEconomicsConfig` (`chain/chain/src/types.rs:182`):
- `min_gas_price()` (`types.rs:199`): for `PROD_GENESIS_PROTOCOL_VERSION` chains it is hardcoded to `MIN_GAS_PRICE_NEP_92_FIX = 100_000_000` yN (`core/primitives/src/version.rs:29`); otherwise the genesis `min_gas_price`.
- `max_gas_price()` (`types.rs:207`): `min(genesis_max_gas_price, min_gas_price * MAX_GAS_MULTIPLIER)` with `MAX_GAS_MULTIPLIER = 20` (`types.rs:191`).
- `gas_price_adjustment_rate` from genesis; validation rejects a rate `>= 1` (`core/chain-configs/src/genesis_validate.rs:181`).

### 4. Burnt vs redistributed; the receiver (contract) reward
During execution (`runtime/runtime/src/lib.rs`), `tx_burnt_amount` starts as `gas_burnt * gas_burn_price` minus the `price_deficit` (`lib.rs:960`-`962`), then accumulates: the `price_surplus` **only pre-AccountCostIncrease** (added when `!AccountCostIncrease.enabled`, `lib.rs:963`-`965`), plus the NEP-536 `refund_penalty`, plus any `create_account_charge`, plus `result.tokens_burnt` (`lib.rs:966`-`969`). Of the *function-call* gas burnt, a fraction `burnt_gas_reward` is paid to the **receiver (contract) account** rather than burnt: `receiver_gas_reward = gas_burnt_for_function_call * burnt_gas_reward.numer / .denom` (`lib.rs:975`). That amount is subtracted from `tx_burnt_amount` and credited to the receiver account if it still exists at the end of execution; otherwise validators (via `balance_burnt`) keep it (`lib.rs:993`-`1006`). The net `tx_burnt_amount` is what removes tokens from total supply (§2).

### 5. Gas refunds and the NEP-536 refund penalty
`refund_unspent_gas_and_deposits` (`runtime/runtime/src/lib.rs:1151`) refunds unused prepaid gas to the signer:
1. `gross_gas_refund` = prepaid (gas + send + exec) minus gas actually used/burnt (`lib.rs:1171`-`1183`).
2. **Penalty** (NEP-536): `refund_penalty = config.fees.gas_penalty_for_gas_refund(gross_gas_refund)` (`lib.rs:1186`), which is `clamp(gross_refund * gas_refund_penalty, min_gas_refund_penalty, gross_refund)` (`core/parameters/src/cost.rs:683`). The penalty is converted to balance at `gas_burn_price` when `AccountCostIncrease` is active, else at `gas_purchase_price` (`lib.rs:1187`). The penalty is *burnt* (added to `tx_burnt_amount`, `lib.rs:966`).
3. The signer's refund is `gross_refund * gas_purchase_price` minus the penalty amount (`lib.rs:1195`).
4. **Price-difference accounting**: if the burn price rose above the purchase price there is a `price_deficit`; if it fell, a `price_surplus` (`lib.rs:1205`-`1217`). The deficit is always subtracted from `tx_burnt_amount` (`lib.rs:961`). The surplus direction flips on `AccountCostIncrease`: **post-AccountCostIncrease the surplus is refunded to the signer** (added to `burned_gas_refund`/`gas_balance_refund`, `lib.rs:1220`-`1225`, `:1267`); **pre-AccountCostIncrease the surplus is burnt** (added to `tx_burnt_amount`, `lib.rs:964`, and `burned_gas_refund` stays zero).
5. **Pessimistic pricing.** Receipt gas is purchased at `receipt_gas_price = max(current_gas_price, min_gas_purchase_price)` (`runtime/runtime/src/config.rs:464`); the conversion gas is burnt at `current_gas_price` (`config.rs:459`). NEP-536 set `pessimistic_gas_price_inflation_ratio` to `1/1` on mainnet (was `103/100`), removing the historical pessimistic markup and the resulting flood of small refund receipts (`core/parameters/res/runtime_configs/78.yaml`).

### 6. Storage staking
`check_storage_stake` (`runtime/runtime/src/verifier.rs:47`) enforces that an account's balance backs its storage: `required = storage_amount_per_byte * account.storage_usage()` (`verifier.rs:53`), and `available = account.amount() + account.locked()` (`verifier.rs:63`). If `available < required` the account must add the deficit (`verifier.rs:79`) — unless it is a zero-balance account (`storage_usage <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT`, NEP-448, `verifier.rs:76`/`:87`). A failing transaction surfaces `InvalidTxError::LackBalanceForState` (`verifier.rs:335`). No tokens are minted or burnt by storage staking — it gates whether balance may leave an account.

### 7. Key economic constants at version 86
- `max_inflation_rate`: `1/40` (≈2.5% annual). Reduced from `1/20` at mainnet epoch-config version 81 (`core/primitives/res/epoch_configs/mainnet/80.json` had `1/20`, `81.json` has `1/40`); default `EpochConfig` also `1/40` (`core/primitives/src/epoch_manager.rs:262`).
- `protocol_reward_rate`: `1/10` on mainnet (genesis), hardcoded for prod-genesis chains (`reward_calculator.rs:64`).
- `online_min_threshold` `90/100`, `online_max_threshold` `99/100` (`epoch_manager.rs:246`; mainnet `85.json`).
- `storage_amount_per_byte`: `10_000_000_000_000_000_000` yN = 1e-5 N/byte (`core/parameters/res/runtime_configs/parameters.yaml:35`).
- `burnt_gas_reward`: `3/10` (`parameters.yaml:6`).
- `gas_refund_penalty`: `0/100` and `min_gas_refund_penalty`: `0` on mainnet base config (`parameters.yaml:14`/`:18`) — i.e. the penalty machinery exists but is inert on mainnet at v86 (the `RuntimeFeesConfig::test` config uses `5/100` and `1` teragas, `core/parameters/src/cost.rs:611`).
- `min_gas_purchase_price`: `1_000_000_000` yN and `account_creation_charge`: `0.007 N`, both set at version 85 (`core/parameters/res/runtime_configs/85.yaml:41`/`:43`).
- `MAX_GAS_MULTIPLIER`: 20 (`types.rs:191`); `MIN_GAS_PRICE_NEP_92_FIX`: `100_000_000` yN (`version.rs:29`).
- `num_blocks_per_year`: `31_536_000` and `protocol_reward_rate` `1/10` in genesis (`core/chain-configs/src/genesis_config.rs:1131`/`:1140`).

## Interactions
- **Consumes**: `total_supply` and validator stats/stake from epoch finalization ([epoch-validators-staking](epoch-validators-staking.md)); `gas_used`/`gas_limit` from chunk application ([sharding-chunks](sharding-chunks.md), [chain-block-processing](chain-block-processing.md)); per-action `Fee`/gas amounts and `tokens_burnt` from [runtime-execution](runtime-execution.md); genesis/versioned parameters from [genesis-configuration](genesis-configuration.md).
- **Produces**: `minted_amount` fed into the next `EpochInfo` and into the block's total-supply check; `next_gas_price` written to the block header; the receiver gas-reward credit and the signer's gas refund (both as state changes applied by [runtime-execution](runtime-execution.md)); storage-staking verdicts gating account balance changes ([accounts-keys](accounts-keys.md)).
- **Touches**: block header `next_gas_price` / `total_supply` fields ([data-structures-serialization](data-structures-serialization.md)).

## Protocol-version-gated behavior
Activation versions cross-checked against `core/primitives-core/src/version.rs`.
- **Gas-refund fee / reduced gas refunds (NEP-536)** — `_DeprecatedReducedGasRefunds` activated at **78** (`version.rs:540`); support for the pre-feature path is removed at v86, so the penalty + non-pessimistic pricing are unconditional in code. On mainnet the penalty rate is `0` (inert) and `pessimistic_gas_price_inflation_ratio` was set to `1/1` at version 78 (`78.yaml`).
- **`AccountCostIncrease`** — activated at **85** (`version.rs:569`). After it: the gas-refund penalty and receiver reward use `gas_burn_price` (not `gas_purchase_price`, `lib.rs:984`, `:1187`); the `price_surplus` from a price decrease is **refunded to the signer** rather than burnt (`lib.rs:1220`-`1225`; pre-feature it was burnt via `tx_burnt_amount`, `lib.rs:963`-`965`); and a `create_account_charge` (`0.007 N`) tops up account-creation cost when an account was created (`lib.rs:1228`). Companion params `min_gas_purchase_price` and `account_creation_charge` are introduced at version 85 (`85.yaml`).
- **Compute costs (NEP-455)** — `_DeprecatedComputeCosts` activated at **61** (`version.rs:501`); compute budgets are now always present (`ParameterCost { gas, compute }`), letting signature-verification compute be set independently of gas (`config.rs:512`).
- **`PostQuantumSignatures`** — activated at **85** (`version.rs:562`); enables the per-scheme `signature_verification_costs`, with ML-DSA-65 carrying a non-zero burnt verify charge while ed25519/secp256k1 stay 0 (`config.rs:512`).
- **Inflation-rate reduction** — not a `ProtocolFeature`; realized purely through the versioned `EpochConfig` store (mainnet `max_inflation_rate` `1/20`→`1/40` at epoch-config version 81).

## Invariants & failure modes
- **Total-supply equation**: every block must satisfy `total_supply == prev + minted - balance_burnt`; violated/over-burnt blocks fail verification (`block.rs:331`-`338`).
- **Gas price within bounds**: `next_gas_price` is always clamped to `[min_gas_price, max_gas_price]` (`block.rs:471`); header value must match the recomputation (`block.rs:414`). Genesis validation rejects `gas_price_adjustment_rate >= 1` (`genesis_validate.rs:181`).
- **Penalty bound**: `gas_penalty_for_gas_refund` always returns `<= gross_gas_refund` (`cost.rs:692`), so a refund can never go negative.
- **Storage backing**: a non-zero-balance account's `amount + locked` must cover `storage_amount_per_byte * storage_usage`; otherwise transactions error with `LackBalanceForState` (`verifier.rs:79`, `:335`).
- **Reward arithmetic**: reward sums use `checked_add`/`checked_sub` with `unwrap` (`reward_calculator.rs:89`, `:143`); an unexpected overflow or a stake lookup for a non-validator panics (`reward_calculator.rs:118`). Big-int (`U256`/`U512`) intermediates guard against multiplication overflow (`reward_calculator.rs:69`, `:134`).
- **`account_creation_charge` invariant**: configs must keep `min_gas_purchase_price * create_account.exec_fee >= account_creation_charge` (noted at `core/parameters/src/cost.rs:589`).

## Code anchors
| Location | Symbol | What happens here |
|---|---|---|
| `chain/epoch-manager/src/reward_calculator.rs:51` | `RewardCalculator::calculate_reward` | Per-epoch issuance, treasury cut, validator pro-rata reward |
| `chain/epoch-manager/src/reward_calculator.rs:69` | (inline) | `epoch_total_reward` inflation formula |
| `chain/epoch-manager/src/reward_calculator.rs:63` | (inline) | Hardcode `protocol_reward_rate = 1/10` for prod-genesis chains |
| `chain/epoch-manager/src/lib.rs:849` | `EpochManager` finalize | Calls reward calc with epoch `total_supply`, `max_inflation_rate` |
| `core/primitives/src/block.rs:440` | `compute_next_gas_price_checked` | Gas-price adjustment toward 50% utilization, clamped |
| `core/primitives/src/block.rs:331` | `verify_total_supply_checked` | `total_supply = prev + minted - burnt` |
| `chain/chain/src/types.rs:199` | `BlockEconomicsConfig::min_gas_price`/`max_gas_price` | Gas-price bounds, `MAX_GAS_MULTIPLIER=20` |
| `runtime/runtime/src/lib.rs:960` | `apply_action_receipt` (burn accounting) | Accumulate `tx_burnt_amount` (burn, surplus, penalty, charge) |
| `runtime/runtime/src/lib.rs:975` | (inline) | Receiver (contract) reward = `burnt_gas_reward` share of FC gas |
| `runtime/runtime/src/lib.rs:1151` | `refund_unspent_gas_and_deposits` | Gas refund + NEP-536 penalty + price-diff handling |
| `core/parameters/src/cost.rs:683` | `gas_penalty_for_gas_refund` | `clamp(gross*penalty, min, gross)` |
| `core/parameters/src/cost.rs:567` | `StorageUsageConfig` | `storage_amount_per_byte` and friends |
| `runtime/runtime/src/verifier.rs:47` | `check_storage_stake` | Enforce balance >= `storage_amount_per_byte * usage` |
| `runtime/runtime/src/config.rs:417` | `calculate_tx_cost` | Burnt vs remaining gas, `receipt_gas_price`, total cost |
| `core/primitives-core/src/version.rs:540` | `ProtocolFeature::protocol_version` | NEP-536 (78), AccountCostIncrease/PostQuantum (85) |
| `core/primitives/src/epoch_manager.rs:262` | `EpochConfig` default | `max_inflation_rate = 1/40` |

## Open questions
- Whether the validator reward map's unqualified-validator shortfall (the difference between `epoch_total_reward` and `epoch_actual_reward`) is intentionally never minted versus rolled forward is not asserted anywhere in code beyond `calculate_reward` returning `epoch_actual_reward`; behavior is clear but the rationale is not documented in-tree.
- `core/parameters/res/runtime_configs/parameters.yaml` (mainnet base) keeps `gas_refund_penalty: 0/100` and `min_gas_refund_penalty: 0`, so the NEP-536 penalty is inert on mainnet at v86 despite the feature being long active; whether a non-zero penalty is scheduled for a later version was not determined from the repo.
