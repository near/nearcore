# Economics

<!-- cspell:ignore calimero -->

> Protocol version: 88 (stable) · Release: 2.14.0 · Derived from commit: bc4e9e8 · Generated: 2026-11-03
> Primary crates/files: `chain/epoch-manager/src/reward_calculator.rs`, `runtime/runtime/src/lib.rs`, `runtime/runtime/src/config.rs`, `runtime/runtime/src/verifier.rs`, `runtime/runtime/src/global_contracts.rs`, `runtime/runtime/src/universal_account_id.rs`, `runtime/runtime/src/deterministic_account_id.rs`, `runtime/near-vm-runner/src/logic/host.rs`, `core/parameters/src/cost.rs`, `core/parameters/src/config.rs`, `core/parameters/src/config_store.rs`, `core/parameters/res/runtime_configs/`, `core/primitives/src/universal_state_init.rs`, `core/primitives/src/block.rs`, `core/primitives/src/chunk_apply_stats.rs`, `chain/chain/src/types.rs`, `chain/chain/src/runtime/mod.rs`

## Role
Economics is the set of monetary rules realized by the implementation: per-epoch token issuance (inflation) and the validator-reward / protocol-treasury split; block-to-block gas-price adjustment toward a target load; which gas/fees are *burnt* (removed from total supply) versus *redistributed*; the price-difference and NEP-536 penalty handling of gas refunds; and storage staking. It does *not* decide who gets reward — validator reward *assignment* lives in [epoch-validators-staking](epoch-validators-staking.md). Fee *amounts* and gas accounting during action execution live in [runtime-execution](runtime-execution.md); this spec covers the monetary effect (burning, rewards, total-supply change). Parameters originate from genesis and the versioned `RuntimeConfig`/`EpochConfig` stores ([genesis-configuration](genesis-configuration.md)). Storage byte-cost enforcement ties into [accounts-keys](accounts-keys.md) and [state-storage](state-storage.md).

**Headline change at PV 87:** `RemoveGasRewards` sets `burnt_gas_reward` from `3/10` to `0/1`, so a contract account no longer receives a share of the gas its `FunctionCall`s burn. That share is now simply burnt — **destroyed**, i.e. subtracted from total supply via the per-block `balance_burnt` term (§3) — like the rest of the receipt's burnt gas (§6.4). It is *not* redirected to validators; validator income is the §1 inflation pool and is unaffected.

**Headline change at PV 88:** `UniversalAccounts` turns on (two days after 87 on mainnet, §10), bringing `CanonicalUniversalStateInit` and `RefundRepeatedUniversalStateInitDeposit` with it. Economically this is (a) a repricing of the `UniversalStateInit` action and of the `universal_state_init_to_account_id` host function to pay for a new canonical-encoding check (§9), (b) a transfer to a `0u` account now paying the implicit `create_account` fee because it creates the account, and (c) a repeated state init refunding its whole deposit instead of topping up the target's storage staking (§7). Total-supply arithmetic (§3) and the reward/refund machinery (§6) are unchanged.

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
- **`BalanceStatsV1`** — `core/primitives/src/chunk_apply_stats.rs:243` — the per-chunk money ledger: `tx_burnt_amount`, `slashed_burnt_amount`, `other_burnt_amount`, `gas_deficit_amount` (informational only, `chunk_apply_stats.rs:249`), and `subsidized_amount` (effectively minted; `chunk_apply_stats.rs:253`).
- **`UniversalStateInitCounts`** / **`DecodeMode`** — `core/primitives-core/src/universal_state_init.rs:98`, `core/primitives-core/src/universal_state_init.rs:78` — the `(num_bytes, num_entries, num_keys)` triple a `UniversalStateInit` payload is priced on, and whether it is decoded leniently or only in canonical borsh (§9).

## Behavior

### 1. Per-epoch issuance (inflation)
`RewardCalculator::calculate_reward` computes the newly minted tokens for an epoch (`chain/epoch-manager/src/reward_calculator.rs:51`). It is called once at each epoch boundary from `EpochManager` with the previous block's total supply and the epoch's `max_inflation_rate` (`chain/epoch-manager/src/lib.rs:1185`, result bound at `lib.rs:1158`).

1. The epoch total reward is a *time-proportional slice* of the annual max-inflation cap (`reward_calculator.rs:69`):
   `epoch_total_reward = max_inflation_rate.numer * total_supply * epoch_duration / (num_seconds_per_year * max_inflation_rate.denom * NUM_NS_IN_SECOND)`.
   `epoch_duration` is in nanoseconds (actual wall-clock epoch length, `chain/epoch-manager/src/lib.rs:1163`), so a shorter/longer epoch mints proportionally less/more. `max_inflation_rate` on mainnet is `1/40` (2.5%) since PV 81 (`core/primitives/res/epoch_configs/mainnet/81.json`), reduced from `1/20` (5%; `.../80.json`); the newest mainnet epoch config at or below PV 88 is still `.../85.json`, which keeps `1/40`.
2. `protocol_reward_rate` is forced to `1/10` when `genesis_protocol_version == PROD_GENESIS_PROTOCOL_VERSION` (29, `core/primitives-core/src/version.rs:667`), otherwise taken from `GenesisConfig` (`reward_calculator.rs:63`).
3. The treasury slice is `epoch_protocol_treasury = epoch_total_reward * protocol_reward_rate` and is credited to `protocol_treasury_account` (`reward_calculator.rs:78`).
4. If there are no validators, the function returns early with the treasury entry in the map but an `epoch_actual_reward` of `Balance::ZERO` (`reward_calculator.rs:85`), so nothing is added to supply that epoch.
5. The validator pool is the remainder `epoch_total_reward - epoch_protocol_treasury` (`reward_calculator.rs:88`), distributed stake-weighted and uptime-scaled (see §2).
6. Returns `(rewards_map, epoch_actual_reward)` where `epoch_actual_reward` is the *actual* sum minted — treasury plus each validator's earned reward (`reward_calculator.rs:145`). Validators below the online-min threshold earn 0, so `epoch_actual_reward` can be less than `epoch_total_reward`; only `epoch_actual_reward` (stored as the epoch's `minted_amount`) is added to supply.
7. Validators kicked out for `NotEnoughBlocks`/`NotEnoughChunks`/`NotEnoughChunkEndorsements` are removed from the stats map *before* the call, so they earn nothing (`chain/epoch-manager/src/lib.rs:1165`); the endorsement cutoff threshold is the epoch config's `chunk_validator_only_kickout_threshold` (`lib.rs:1181`).

### 2. Validator reward (uptime ramp)
For each validator (`reward_calculator.rs:94`):
1. `production_ratio` = its combined online ratio across blocks/chunks/endorsements (`get_validator_online_ratio`, `chain/epoch-manager/src/validator_stats.rs:16`; the endorsement ratio is first cut to 0/1 by `endorsement_cutoff_threshold` when set).
2. If `production_ratio < online_min_threshold`, or all of expected blocks/chunks/endorsements are 0, reward is 0 (`reward_calculator.rs:109`).
3. Otherwise the online multiplier is `min(1, (uptime − online_min)/(online_max − online_min))`, and the reward is `epoch_validator_reward * multiplier * stake / total_stake` (`reward_calculator.rs:133`). All arithmetic is exact integer `U256`/`U512` rational math. Mainnet thresholds are `online_min = 90/100`, `online_max = 99/100` (`core/primitives/res/epoch_configs/mainnet/85.json`).

### 3. Total-supply change per block
`new_total_supply = prev.total_supply + minted_amount − balance_burnt` (`core/primitives/src/block.rs:195`; the verification counterpart is `Block::verify_total_supply_checked`, `block.rs:336`). `minted_amount` is `Some` only on the first block of an epoch, taken from the epoch info populated by `calculate_reward` (produced at `chain/client/src/client.rs:1112`, verified at `chain/chain/src/chain.rs:2555`). `balance_burnt` is the sum of each included chunk's `prev_balance_burnt()` (`block.rs:154`). Thus inflation *adds* to supply once per epoch, and burnt fees *subtract* every block; the difference is net issuance.

The per-chunk `balance_burnt` is assembled by the runtime adapter: `tx_burnt_amount + other_burnt_amount + slashed_burnt_amount` minus `subsidized_amount` (`chain/chain/src/runtime/mod.rs:392`, `mod.rs:405`), surfaced as `ApplyChunkResult::total_balance_burnt` (`chain/chain/src/types.rs:119`) and written into the chunk extra / chunk header by `to_chunk_extra` (`types.rs:166`). `subsidized_amount` is balance the protocol effectively **mints**. When `one_yocto_on_promise` is set (`85.yaml:1`, so PV ≥ 85; no `ProtocolFeature` variant) and the calling contract's balance is exactly zero, attaching exactly 1 yoctoNEAR to a promise function call skips the balance deduction entirely: the account is *not* debited, and the skipped yocto is added to `subsidized_amount` instead. Two sites do this, with identical conditions (`amount == 1yN && one_yocto_on_promise && current_account_balance.is_zero()`):

| Site | Location |
|---|---|
| `promise_batch_action_function_call_weight` (`host.rs:3517`, which `promise_batch_action_function_call` at `host.rs:3456` delegates to) | `runtime/near-vm-runner/src/logic/host.rs:3583` |
| `promise_yield_create_with_id` (`host.rs:4330`, reached from `promise_yield_create`, `host.rs:4245`) | `runtime/near-vm-runner/src/logic/host.rs:4423` |

The receipt still carries the 1 yoctoNEAR deposit to the callee, so the yocto appears in circulation without ever having been debited — i.e. it is minted. It propagates `VMOutcome → ActionResult` (`runtime/runtime/src/function_call.rs:223`) → `stats.balance.subsidized_amount` (`runtime/runtime/src/lib.rs:1143`), and subtracting it from the chunk burn (`chain/chain/src/runtime/mod.rs:405`) is what keeps `new_total_supply` exact: the mint is netted against the same block's burn rather than added to `minted_amount`. `gas_deficit_amount` is recorded for observability only (`runtime/runtime/src/lib.rs:1072`) and does not enter the supply equation.

> These two host functions moved from `runtime/near-vm-runner/src/wasmtime_runner/logic.rs` to `runtime/near-vm-runner/src/logic/host.rs` in this release (#16428, host functions usable without wasmtime). The subsidy logic itself is byte-for-byte unchanged.

### 4. Gas-price adjustment
The next block's gas price is a load-feedback controller (`core/primitives/src/block.rs:456` — `compute_next_gas_price_checked`):
- Formula (documented at `block.rs:434`): `next_gas_price = gas_price * (1 + (gas_used/gas_limit − 1/2) * adjustment_rate)`. Implemented as the exact integer ratio `numerator/denominator` at `block.rs:476`. When utilization is exactly 50% the price is unchanged; above 50% it rises, below it falls.
- If the block was skipped (`gas_limit == 0`) the price is unchanged (`block.rs:465`).
- The result is clamped to `[min_gas_price, max_gas_price]` (`block.rs:487`).
- `min_gas_price` is `MIN_GAS_PRICE_NEP_92_FIX` (`100_000_000` yN) for chains whose genesis is `PROD_GENESIS_PROTOCOL_VERSION`, else the genesis value (`chain/chain/src/types.rs:199`, const at `core/primitives/src/version.rs:29`). `max_gas_price` is `min(genesis_max_gas_price, min_gas_price * 20)` (`types.rs:207`, `MAX_GAS_MULTIPLIER = 20` at `types.rs:191`).
- The chain applies this over one block at block construction (`block.rs:175`); under Spice (nightly only) it folds over certified results (`compute_gas_price_from_certified_results_checked`, `block.rs:495`), and `balance_burnt` likewise folds over certified results (`block.rs:516`).

### 5. Transaction cost & what is burnt at conversion
`calculate_tx_cost` (`runtime/runtime/src/config.rs:481`; `tx_cost` at `config.rs:464` is the thin wrapper over a `Transaction`) splits a transaction's cost:
1. `burnt` = `new_action_receipt` send fee + `total_send_fees` of the actions (`config.rs:493`) + the signature-verification surcharge (`config.rs:504`). This gas is burnt *immediately* at `current_gas_price` (`burnt_amount`, `config.rs:527`).
2. `gas_remaining` = attached (prepaid) function-call gas + prepaid send fees + the wrapping receipt's exec fee + prepaid exec fees (`config.rs:521`). This gas is *purchased* (not yet burnt) at `receipt_gas_price = max(current_gas_price, min_gas_purchase_price)` (`config.rs:532`).
3. `gas_cost = burnt_amount + remaining_gas_amount` (`config.rs:534`) and `total_cost = gas_cost + total_deposit` (`config.rs:536`).

Send/exec fees per action are computed by `total_send_fees` (`config.rs:86`) and `exec_fee` (`config.rs:334`), each reading the `Fee` for the action's `ActionCosts` and adding per-byte components. Transfers add implicit-account-creation fees via `transfer_send_fee`/`transfer_exec_fee` (`core/parameters/src/cost.rs:812`/`775`) — see §9 for the `0u` receiver case that turns on at PV 88. Gas-key transfers price the exec fee with `gas_key_transfer_exec_fee` (`cost.rs:882`, called at `config.rs:411`/`config.rs:415`) and `AddKey` with gas-key permissions adds `gas_key_add_key_exec_fee` (`cost.rs:928`). Universal-account state inits are priced by `universal_state_init_fee` (`config.rs:242`).

The signature-verification surcharge is `signature_verification_cost` (`config.rs:589`): the signer's own scheme cost plus, for every delegate action, the inner signer's scheme cost. Only ML-DSA-65 is non-zero (`100 Ggas`). It raises the burnt gas (and therefore `total_cost`) but never `gas_remaining`, so on-chain function-call budgets are unaffected (comment at `config.rs:500`).

**Gas-key and delegate fee basis on the stable path.** `signature_verification_cost` takes a `meter_inner_verify_on_receiver` flag and `gas_key_send_pk_len` (`config.rs:73`) branches on `config.wasm_config.fix_ml_dsa_cost_charging`. At PV 88 that flag is **`false`** (§8), because `FixMlDsaCostCharging` is nightly-only in this release. So on the stable path:
- a gas-key `TransferToGasKey` / `WithdrawFromGasKey` **send** fee is priced on `public_key.trie_id_len()` (the on-trie identifier length), the same basis as the exec fee, not on the wire length `public_key.len()` (`config.rs:73`, called at `config.rs:133`, `config.rs:212`). For ed25519/secp256k1 the two coincide; for ML-DSA-65 they do not, and the send fee is the (acknowledged-as-incorrect) storage-length one.
- the inner `DelegateAction` signature-verification **compute** is charged on the signer shard at conversion (`config.rs:601` is not taken), and `apply_delegate_action` does **not** add it on the receiver shard (`runtime/runtime/src/actions.rs:467` is gated on the same flag). Gas is identical either way; only which shard's `compute_limit` the work debits differs, so token flows are unaffected.

### 6. Gas burning, rewards, and refunds during receipt execution
In `Runtime::apply_action_receipt` (`runtime/runtime/src/lib.rs`), after actions run:
1. **Gas burn price** — `gas_purchase_price` is the receipt's own price (`lib.rs:1034`); with `AccountCostIncrease` enabled, gas is burnt at `min(gas_purchase_price, apply_state.gas_price)`, before it at `apply_state.gas_price` (`lib.rs:1038`).
2. **Refunds** — `refund_unspent_gas_and_deposits` (`lib.rs:1284`) refunds unspent gas and, on failure, the full deposit. `gross_gas_refund` = prepaid gas + prepaid exec gas − gas actually used/burnt (`lib.rs:1304`). It then:
   - Computes the NEP-536 penalty `gas_penalty_for_gas_refund(gross)` = `min(gross, max(gross * gas_refund_penalty, min_gas_refund_penalty))` (`core/parameters/src/cost.rs:736`), priced at `gas_burn_price` post-`AccountCostIncrease` or `gas_purchase_price` before (`lib.rs:1319`). The unused-gas refund is issued at `gas_purchase_price` minus this penalty (`lib.rs:1328`).
   - Records `price_deficit` if the burn price rose above purchase price, else `price_surplus` (`lib.rs:1338`). With `AccountCostIncrease`, the surplus is refunded to the signer (`burned_gas_refund`, `lib.rs:1353`); before, it was retained as burnt.
   - If a new account was created (post-`AccountCostIncrease`), an extra `create_account_charge` = `min(account_creation_charge − already-burned, burned_gas_refund)` is subtracted from the refund (`lib.rs:1377`, stored at `lib.rs:1394`).
   - System-originated (refund) receipts skip all of this and use `GasRefundResult::default()` (`lib.rs:1055`); if such a refund itself fails, its deposit is added to `other_burnt_amount` (`lib.rs:1047`).
3. **tx_burnt_amount** — burnt tokens for the receipt = `gas_burn_price * gas_burnt − price_deficit` (`lib.rs:1093`), plus `refund_penalty` (`lib.rs:1099`), `create_account_charge` (`lib.rs:1100`) and `result.tokens_burnt` (`lib.rs:1102`); pre-`AccountCostIncrease` also plus `price_surplus` (`lib.rs:1097`). System/refund receipts burn 0 gas. The `ExecutionOutcome.tokens_burnt` reported to clients is snapshotted *here*, before any contract reward is deducted (`lib.rs:1105`, used at `lib.rs:1269`).
4. **Contract reward (zero at PV 87+)** — `receiver_gas_reward = gas_burnt_for_function_call * burnt_gas_reward` (`lib.rs:1108`), converted to tokens at `gas_burn_price` post-`AccountCostIncrease` (`lib.rs:1118`), else at `gas_purchase_price` (`lib.rs:1123`). If it is positive and the receiver account still exists, it is *subtracted* from `tx_burnt_amount` and credited to the account (`lib.rs:1126`–`lib.rs:1132`). At PV 87 and PV 88 `burnt_gas_reward` is `0/1` (`core/parameters/res/runtime_configs/87.yaml:3`; confirmed at the v88 table, `core/parameters/res/runtime_configs/parameters.snap:6`), so `receiver_gas_reward` is always 0, the branch is never taken, and the full `tx_burnt_amount` stays burnt. Consequences:
   - Function-call gas that used to be redistributed to the contract is now removed from total supply via §3, benefiting all holders proportionally rather than the callee. (The in-code comment at `lib.rs:1129` saying "validators receive the remaining execution reward" is stale: burnt tokens are destroyed, not paid out; validator income comes from the §1 inflation pool.)
   - `ExecutionOutcome.gas_burnt` and `ExecutionOutcome.tokens_burnt` are **unchanged** by the switch. `gas_burnt` is `result.gas_burnt`, which the reward path never touches (`lib.rs:1267`), and `tokens_burnt` is the snapshot taken at `lib.rs:1105` and moved into the outcome at `lib.rs:1269` — the reward is subtracted only from the local `tx_burnt_amount` afterwards (`lib.rs:1132`). So the only observable differences are the chunk-level `balance_burnt` (larger by the former reward) and the receiver account's balance (smaller by it).
   - `ActionResult::gas_burnt_for_function_call` (`lib.rs:414`, accumulated in `runtime/runtime/src/function_call.rs:141`) is still tracked; it simply no longer has an effect.
5. **Accumulation** — the remaining `tx_burnt_amount` accumulates into `stats.balance.tx_burnt_amount` (`lib.rs:1141`) and `result.subsidized_amount` into `stats.balance.subsidized_amount` (`lib.rs:1143`). Transaction-conversion burn is accumulated separately from the transaction's own outcome (`lib.rs:2297`, `lib.rs:2339`).
6. **Global-contract storage burn** — `DeployGlobalContract` burns `global_contract_storage_amount_per_byte * code.len()` straight out of the deployer's balance into `result.tokens_burnt` (`runtime/runtime/src/global_contracts.rs:35`, `global_contracts.rs:49`), failing with `ActionErrorKind::LackBalanceForState` if the balance is short (`global_contracts.rs:42`). The rate is `0.0001 N` per byte since PV 77 (`core/parameters/res/runtime_configs/77.yaml:2`).

### 7. Storage staking
`check_storage_stake` (`runtime/runtime/src/verifier.rs:48`) enforces that an account backs its storage: `required = storage_amount_per_byte * account.storage_usage()` (`verifier.rs:54`), `available = account_balance + account.locked()` (`verifier.rs:64`). If `available < required` and the account is not a zero-balance account (`storage_usage() <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT = 770`, `verifier.rs:25`, `verifier.rs:89`, NEP-448), it returns `LackBalanceForStorageStaking(required − available)` (`verifier.rs:80`). Call sites:

| Site | Location | Failure surfaced as |
|---|---|---|
| ordinary tx verification | `runtime/runtime/src/verifier.rs:375` | `InvalidTxError::LackBalanceForState` |
| universal-account bootstrap tx (`verify_and_charge_bootstrap_tx_ephemeral`, `verifier.rs:424`) | `runtime/runtime/src/verifier.rs:493` | `InvalidTxError::LackBalanceForState` |
| deposit (gas-key) path | `runtime/runtime/src/verifier.rs:659` | `InvalidTxError::NotEnoughBalanceForDeposit` with `DepositCostFailureReason::LackBalanceForState` (`verifier.rs:668`) |
| end of receipt application | `runtime/runtime/src/lib.rs:1011` | `ActionErrorKind::LackBalanceForState`, rolling the receipt back |
| state-init deposit settlement (deterministic *and* universal, first init only) | `runtime/runtime/src/deterministic_account_id.rs:83` — `settle_state_init_deposit` (`deterministic_account_id.rs:73`) | tops up exactly the missing stake and refunds the rest; fails the action with `LackBalanceForState` if the deposit cannot cover it |

`settle_state_init_deposit` is the only place a *deposit* is converted into storage stake: it computes `deposit_refund` and, when positive, emits a `Receipt::new_balance_refund` back to the receipt's refund receiver (`deterministic_account_id.rs:108`–`deterministic_account_id.rs:111`). Both `action_deterministic_state_init` (`deterministic_account_id.rs:15`) and `action_universal_state_init` (`runtime/runtime/src/universal_account_id.rs:22`) end in it on the path that actually initializes the account (`universal_account_id.rs:107`).

**`RefundRepeatedUniversalStateInitDeposit` (PV 88).** A `UniversalStateInit` whose target account is already initialized — which happens because a transfer to a `0u` id creates the account uninitialized before its state init lands — now takes an early-out: the installed state is left alone and the **whole** deposit is refunded with a `new_balance_refund` receipt (`universal_account_id.rs:52`, `universal_account_id.rs:65`–`universal_account_id.rs:69`). Below PV 88 the repeat instead fell through to `settle_state_init_deposit` (`universal_account_id.rs:56`), so the deposit first topped up the existing account's storage staking and only the remainder was refunded — and a deposit short of the missing stake failed the action with `LackBalanceForState`. Net economic effect at PV 88: a repeated state init can no longer move balance into someone else's account, and can no longer fail for the target's storage shortfall. (Asserted by `repeated_universal_state_init_refunds_whole_deposit` and `repeated_universal_state_init_settles_storage_before_refund_feature`, `runtime/runtime/src/actions.rs`.)

`storage_amount_per_byte` is `1e19` yN (1 NEAR backs 100,000 bytes ≈ 100 KB) — base value at `core/parameters/res/runtime_configs/parameters.yaml:35`, unchanged at PV 88 (`parameters.snap:20`). `num_bytes_account = 100` and `num_extra_bytes_record = 40` (`parameters.yaml:36`, `parameters.yaml:37`).

### 8. Where the numbers live
Runtime parameters are the base table `core/parameters/res/runtime_configs/parameters.yaml` plus an ordered list of per-version diffs applied by `RuntimeConfigStore::new` (`core/parameters/src/config_store.rs:26` — `CONFIG_DIFFS`). **At this release the diff list starts at `53.yaml` and ends, for the stable path, at `88.yaml`** (`config_store.rs:59`); the 46/48/49/50/52 diffs were deleted along with `MIN_SUPPORTED_PROTOCOL_VERSION` rising to 84, so the store no longer reproduces configs for those versions (it still holds the base config under key 0 and every retained diff version). `129.yaml`/`155.yaml`/`157.yaml` are nightly-only.

`core/parameters/res/runtime_configs/parameters.snap` is the authoritative human-readable table: it is generated as base + every diff with `version <= PROTOCOL_VERSION` on a non-nightly build (`config_store.rs:400`–`config_store.rs:417`), i.e. exactly the PV-88 stable parameter set. Economic parameters in force at PV 88:

| Parameter | Value at PV 88 | Set by | Verified at |
|---|---|---|---|
| `burnt_gas_reward` | `0 / 1` | `87.yaml:3` (was `3/10` from `parameters.yaml:6`) | `parameters.snap:6` |
| `pessimistic_gas_price_inflation` | `1 / 1` (no-op) | `78.yaml:2` | `parameters.snap:7` |
| `gas_refund_penalty` | `0 / 100` | `parameters.yaml:14` | `parameters.snap:8` |
| `min_gas_refund_penalty` | `0` | `parameters.yaml:18` | `parameters.snap:9` |
| `min_gas_purchase_price` | `1_000_000_000` yN | `85.yaml:41` | `parameters.snap:10` |
| `account_creation_charge` | `0.007 N` | `85.yaml:43` | `parameters.snap:11` |
| `storage_amount_per_byte` | `1e19` yN | `parameters.yaml:35` | `parameters.snap:20` |
| `storage_num_bytes_account` / `storage_num_extra_bytes_record` | `100` / `40` | `parameters.yaml:36`, `:37` | `parameters.snap:21`, `:22` |
| `global_contract_storage_amount_per_byte` | `0.0001 N` | `77.yaml:2` | `parameters.snap:301` |
| `ml_dsa_65_verification_cost` (tx-conversion surcharge) | `100 Ggas` | `85.yaml:15` | `parameters.snap:304` |
| `action_create_account` | send `0.5 Tgas` / exec `7.2 Tgas` | `85.yaml:16` | `parameters.snap:35`–`:38` |
| `action_universal_state_init` | send `0.5 Tgas` / exec `7.43 Tgas` | `parameters.yaml:143` | `parameters.snap:99`–`:102` |
| `action_universal_state_init_per_entry` | send `3_100_000_000` / exec `200_000_000_000` | **`88.yaml:18`** (send was `0`) | `parameters.snap:103`–`:106` |
| `action_universal_state_init_per_byte` | send `78_000_000` / exec `74_000_000` | **`88.yaml:38`** (was `72M`/`70M`) | `parameters.snap:107`–`:110` |
| `max_state_init_entries` | `1_500` (per **receipt**) | `87.yaml:43` | `parameters.snap:251` |
| `max_universal_state_init_keys` | `1_024` (per **receipt**) | `parameters.yaml:316` | `parameters.snap:250` |
| `max_receipt_total_input_size` | `4_294_967_295` (**unbounded in practice**) | base; the `4_194_944` cap moved to `157.yaml` | `parameters.snap:249` |

Feature flags in the PV-88 table (these are yaml booleans, not all of which have a `ProtocolFeature` variant):

| Flag | Value at PV 88 | Set by | Verified at |
|---|---|---|---|
| `one_yocto_on_promise` | `true` | `85.yaml:1` | `parameters.snap:315` |
| `ml_dsa_verify_host_fn` | `true` | `87.yaml:14` | `parameters.snap:317` |
| `sha3_host_fns` | `true` | `87.yaml:22` | `parameters.snap:318` |
| `fix_contract_loading_error` | `true` | `87.yaml:19` | `parameters.snap:270` |
| `universal_accounts` | `true` | **`88.yaml:3`** (moved out of `87.yaml`) | `parameters.snap:273` |
| `canonical_universal_state_init` | `true` | **`88.yaml:10`** (new) | `parameters.snap:274` |
| `fix_ml_dsa_cost_charging` | **`false`** | moved out of `87.yaml` into `157.yaml` | `parameters.snap:275` |

Host-function gas costs charged as ordinary burnt wasm gas inside `FunctionCall` (see [runtime-execution](runtime-execution.md)):

| Host function | Base gas | Per-byte gas | Stable since | Flag |
|---|---|---|---|---|
| `sha3_256` | `5_879_491_275` | `21_471_105` | PV 87 | `sha3_host_fns` (`87.yaml:22`) |
| `sha3_384` / `sha3_512` | `5_811_388_236` | `36_649_701` | PV 87 | `sha3_host_fns` |
| `ml_dsa_verify` | `540_000_000_000` | `11_000_000` | PV 87 | `ml_dsa_verify_host_fn` (`87.yaml:14`) |
| `universal_state_init_to_account_id` | `8_900_000_000` | `83_100_000` | PV 88 | `universal_accounts` (`88.yaml:3`); repriced by `canonical_universal_state_init` (`88.yaml:67`, `88.yaml:68`) |

(Base values live at `core/parameters/res/runtime_configs/parameters.yaml:201`–`parameters.yaml:227`; effective values at PV 88 are `parameters.snap:148`–`parameters.snap:164` and `core/parameters/src/snapshots/near_parameters__config_store__tests__88.json.snap`. The `ExtCostsConfig` compile-time defaults at `core/parameters/src/cost.rs:185`–`cost.rs:186` are a separate Rust table and are not what a real chain runs on.)

### 9. Universal-account pricing at PV 88
`UniversalAccounts` (PV 88) makes `0u…` account ids implicit and enables the `UniversalStateInit` action. Three pricing consequences:

**9.1 A transfer to a `0u` id now pays an implicit account creation.** `transfer_send_fee` / `transfer_exec_fee` take a `universal_accounts_enabled` flag; with it set, `AccountType::UniversalAccount` adds the `create_account` fee (`0.5 Tgas` send + `7.2 Tgas` exec) on top of the plain transfer fee, because the transfer is what creates the account — its keys arrive later with the state init (`core/parameters/src/cost.rs:788`, `cost.rs:826`). Below PV 88 the same receiver was charged a plain transfer. The gap is closed by `RejectUniversalAccountTransfers` (PV 87): from PV 87 until `UniversalAccounts` turns on, a *new* receipt carrying a transfer to a `0u` receiver is rejected with `ActionsValidationError::TransferToUniversalAccountNotAllowed` (`runtime/runtime/src/action_validation.rs:241`), so no receipt that was funded at the cheaper price can execute once the fee applies. Receipts already in flight keep executing (`ValidateReceiptMode::NewReceipt` only).

**9.2 `CanonicalUniversalStateInit` adds a decode/re-encode check, and the new fees pay for it.** `UniversalStateInit::from_raw_strict` decodes the payload leniently and then re-encodes it, rejecting anything that does not round-trip to the same bytes — unsorted or duplicate map keys and set elements, and trailing bytes (`core/primitives/src/universal_state_init.rs:135`, error `StateInitError::NonCanonical`, "state init is not in canonical borsh form"). `Config::state_init_decode_mode()` picks `Strict` once the flag is on and `Lenient` before (`core/parameters/src/vm.rs:328`). Two independently priced consumers run it:
- **The action**, charged by `universal_state_init_fee` (`runtime/runtime/src/config.rs:242`) at transaction conversion, as `universal_state_init_size_terms` (base × 1 + per-byte × `num_bytes`, `core/parameters/src/cost.rs:958`) plus `universal_state_init_content_terms` (per-entry × `num_entries` + **`add_full_access_key`** × `num_keys` — each committed key is priced as a full `AddKey`, `cost.rs:967`). `88.yaml` raises the send side of the byte term `72M → 78M`, the exec side `70M → 74M`, and the entry term's send side `0 → 3_100_000_000`; the entry exec term stays `200 Ggas`. The `88.yaml` comments record the derivation: icount measurement of the extra decode/re-encode work against the `sha3_256` anchor's 4.94× safety margin at 125_000 gas/instruction (send 42.89 vs 34.27 instructions/byte, exec 7.52 vs 1.48, against the deterministic action in the same run; 624M gas/entry raw after removing the 15 bytes already paid per byte, → 3.08G at the margin, rounded up).
- **The contract-created path**, `promise_batch_action_universal_state_init` (`runtime/near-vm-runner/src/logic/host.rs:3377`). It charges the size terms *before* handing the payload to the host, because decoding is the work the per-byte fee pays for, then the content terms as soon as the counts come back and still before the action exists (`host.rs:3410`–`host.rs:3413`, via `pay_universal_state_init_terms`, `host.rs:3428`).

**9.3 Counts differ between the two paths, and `Strict` mode charges an upper bound for rejected bytes.** `state_init_counts` (`core/primitives/src/universal_state_init.rs:168`) always decodes leniently and reports `(len, 0, 0)` for bytes that do not decode — it must give the same answer on every protocol version, because receipts created under earlier versions are re-priced from it (for refunds among other things). That is what `universal_state_init_fee` uses, so **transaction-conversion pricing is version-independent**. `state_init_send_counts(raw, mode)` (`universal_state_init.rs:184`) is used only by the VM for a contract-supplied payload: in `Lenient` mode it equals `state_init_counts`, but in `Strict` mode a payload that fails to decode *or* is non-canonical is charged `upper_bound_counts` — as many entries and keys as its length could possibly hold (`num_bytes.saturating_sub(6).div_ceil(8)` entries and `num_bytes.saturating_sub(10).div_ceil(33)` keys), because the decoder has to walk every byte before it can reject. From PV 88, a contract that passes garbage to the host therefore pays the worst-case content fee rather than the byte fee alone.

**9.4 The `universal_state_init_to_account_id` host function.** From PV 88 it decodes the bytes, checks that they re-encode to the same bytes, and only then hashes with SHA3-256 and base32-encodes the digest, returning a non-zero status on a non-canonical payload instead of an id (`runtime/near-vm-runner/src/logic/host.rs:3299`, strict branch at `host.rs:3321` calling `External::check_state_init`, `runtime/runtime/src/ext.rs:480`). The per-byte fee is charged *before* the check runs (`host.rs:3318`), which is what pays for it. The repricing from `8_520_000_000 / 21_471_105` to `8_900_000_000 / 83_100_000` (`88.yaml:67`, `88.yaml:68`) is calibrated by icount against `sha3_256` measured in the same run, inheriting its safety margin: base `5_879_491_275 × 12_978.61 / 8_579.23 = 8_894_460_721.6`, byte `21_471_105 × 134.94 / 34.87 = 83_088_927.7`, both rounded up. The byte figure is set by the worst measured shape (a dense payload of 852 tiny map entries in 10_234 bytes); denser shapes are acknowledged in the comment as unmeasured. Below PV 88 the function took any bytes and always returned status 0.

### 10. Upgrade scheduling
`get_protocol_upgrade_schedule` (`core/primitives/src/version.rs:58`) now carries a **two-step** schedule on both production chains:

| Chain | PV 87 vote | PV 88 vote |
|---|---|---|
| mainnet (`version.rs:59`) | 2026-11-04 00:00 UTC | 2026-11-06 00:00 UTC |
| testnet (`version.rs:72`) | 2026-09-23 00:00 UTC | 2026-10-21 00:00 UTC |

Two constraints bound when a scheduled vote actually takes effect, both stated in the comment above the schedule (`core/primitives/src/version.rs:38`–`version.rs:49`): an upgrade lands **1–2 epochs after the set date**, and **at most one upgrade vote is counted per epoch** (a second vote falling in the same epoch is postponed). The epoch-length basis for the comment's "15h-30h" is mainnet's genesis `epoch_length = 43_200` blocks — nominally 12 h at one block per second (`docs/architecture/how/epoch.md:35`–`:38`, `docs/ChainSpec/EpochAndStaking/Epoch.md:20`); 15 h per epoch is that same 43_200 blocks at the ~1.25 s block time mainnet actually produces at.

The two mainnet steps are 48 h apart, i.e. **three to four epochs** — comfortably outside both bounds rather than tight. The votes cannot share an epoch, so neither is postponed; and PV 87 is live at worst 30 h after 2026-11-04, i.e. by 2026-11-05 06:00 UTC, at least one full epoch before the PV 88 vote date. The 87 step is what arms `RejectUniversalAccountTransfers` before `UniversalAccounts` turns on at 88 (§9.1), so the window in which `0u` transfers are banned outright runs from roughly 2026-11-05 to 2026-11-07. As of this commit's date mainnet has voted neither.

## Interactions
- **Issuance ← epoch manager.** `EpochManager` calls `calculate_reward` at epoch boundaries and stores `validator_reward`/`minted_amount` in the next-next epoch info; assignment of who validates is [epoch-validators-staking](epoch-validators-staking.md).
- **Fees ← runtime execution.** Per-action gas/compute burning happens in [runtime-execution](runtime-execution.md); this spec covers the resulting burn/reward/refund token flows.
- **Gas price ← block processing.** [chain-block-processing](chain-block-processing.md) supplies `gas_used`/`gas_limit` and stores the resulting `next_gas_price` in the block header; [consensus-finality](consensus-finality.md) verifies total supply.
- **Storage cost ↔ accounts.** Storage-usage byte counts come from [accounts-keys](accounts-keys.md) / [state-storage](state-storage.md); this spec prices them. Universal/deterministic account creation and the `0u` id derivation live in [accounts-keys](accounts-keys.md); §9 prices them.
- **Parameters ↔ genesis.** `max_inflation_rate`, `protocol_reward_rate`, `num_blocks_per_year`, and gas-price bounds come from genesis / the versioned config stores ([genesis-configuration](genesis-configuration.md), [protocol-versioning](protocol-versioning.md)).

## Protocol-version-gated behavior
Verified against `core/primitives-core/src/version.rs` at this commit (`STABLE_PROTOCOL_VERSION = 88`, `version.rs:698`; `MIN_SUPPORTED_PROTOCOL_VERSION = 84`, `version.rs:670`). Features folded into the ≤84 baseline are now `_Deprecated*` and their behavior is unconditional.

| Feature | Activates | Effect on economics |
|---|---|---|
| **`RemoveGasRewards`** | **PV 87** (`version.rs:477`, mapped at `version.rs:637`) | Purely a parameter change: `burnt_gas_reward` `3/10` → `0/1` (`core/parameters/res/runtime_configs/87.yaml:3`). No code gate — the reward site at `runtime/runtime/src/lib.rs:1108` computes 0 and the credit branch (`lib.rs:1126`) is skipped. Function-call gas that previously went to the callee is now burnt (removed from supply). `ExecutionOutcome.gas_burnt` / `tokens_burnt` are unchanged; only the chunk `balance_burnt` and the receiver's balance differ. Unchanged at PV 88. |
| **`UniversalAccounts`** | **PV 88** (`version.rs:491`, mapped at `version.rs:643`) — **moved from 87 in this release** | Enables `0u` implicit account ids and the `UniversalStateInit` action, priced by `universal_state_init_fee` (`runtime/runtime/src/config.rs:242`), plus a bootstrap transaction path with its own balance/storage-stake checks (`runtime/runtime/src/verifier.rs:424`, storage check at `verifier.rs:493`). Also makes a transfer to a `0u` receiver pay the implicit `create_account` fee (`core/parameters/src/cost.rs:788`, `cost.rs:826`). Flag `universal_accounts` moved from `87.yaml` to `88.yaml:3`. |
| **`CanonicalUniversalStateInit`** | **PV 88** (`version.rs:502`, mapped at `version.rs:645`) | A `UniversalStateInit` is accepted only in canonical borsh, so each state init has exactly one encoding and one account id. Comes with the fees that pay for the decode/re-encode check: `action_universal_state_init_per_entry` send `0 → 3_100_000_000`, `action_universal_state_init_per_byte` send `72M → 78M` / exec `70M → 74M`, `wasm_universal_state_init_to_account_id_base` `8_520_000_000 → 8_900_000_000` and `_byte` `21_471_105 → 83_100_000` (`88.yaml:10`, `:18`, `:38`, `:67`, `:68`). Also makes contract-supplied payloads that fail the check pay the worst-case entry/key count (`state_init_send_counts`, `core/primitives/src/universal_state_init.rs:184`). See §9. |
| **`RefundRepeatedUniversalStateInitDeposit`** | **PV 88** (`version.rs:497`, mapped at `version.rs:644`) | A `UniversalStateInit` on an already-initialized account refunds the **whole** deposit (`runtime/runtime/src/universal_account_id.rs:65`) instead of first settling it against that account's storage staking (`universal_account_id.rs:56`). A repeat can therefore no longer top up a foreign account's stake, and can no longer fail with `LackBalanceForState` for the target's shortfall. See §7. |
| `RejectUniversalAccountTransfers` | PV 87 (`version.rs:494`, mapped at `version.rs:641`) | No fee change of its own. Rejects *new* receipts transferring to a `0u` id while `UniversalAccounts` is off, so no receipt funded at the plain-transfer price survives into PV 88 where the same transfer costs an extra `create_account` (`runtime/runtime/src/action_validation.rs:241`). |
| `FixContractLoadingError` | PV 87 (`version.rs:448`, mapped at `version.rs:633`) | A failing `Module::deserialize` is returned as a cached `FunctionCallError::LoadingError` so it flows through the fee-charge points and finalizes as a gas-bearing abort, instead of a zero-gas runner error (flag `fix_contract_loading_error`, `87.yaml:19`). Economic effect: the contract-loading fee is burnt rather than the receipt costing nothing. |
| `AccountCostIncrease` | PV 85 (`version.rs:434`, mapped at `version.rs:630`) | Gas is burnt at `min(purchase, block)` price; the surplus from a *dropped* gas price is refunded to the signer instead of retained as burnt; contract reward (when non-zero) priced at the burn price; a `create_account_charge` (`account_creation_charge`, `0.007 N`) is levied on account creation. Also raises the `create_account` action fee and sets `min_gas_purchase_price = 1e9 yN` (`85.yaml:16`, `85.yaml:41`, `85.yaml:43`). Gated at `lib.rs:1038`, `lib.rs:1097`, `lib.rs:1118`, `lib.rs:1319`, `lib.rs:1361`. |
| `PostQuantumSignatures` | PV 85 (`version.rs:408`, mapped at `version.rs:623`) | Adds the ML-DSA-65 `SignatureKind` and its non-zero `ml_dsa_65_verification_cost` (`100 Ggas`, `85.yaml:15`) charged as extra *burnt* gas at tx conversion (`runtime/runtime/src/config.rs:504`); ed25519/secp256k1 stay 0. |
| `FixDeleteAccountGlobalContractStorageUsage` | PV 85 (`version.rs:360`, mapped at `version.rs:616`) | `action_delete_account` now also subtracts the global-contract identifier's storage usage, which previously overstated an account's `storage_usage()` and therefore its storage-staking requirement (§7) (`runtime/runtime/src/actions.rs:342`). Active at PV 88. |
| `GasKeys` | PV 85 (`version.rs:344`, mapped at `version.rs:618`) | Adds `TransferToGasKey` / `WithdrawFromGasKey`, which move balance between an account and a gas key, priced by `gas_key_transfer_exec_fee` (`core/parameters/src/cost.rs:882`, called at `runtime/runtime/src/config.rs:411`/`:415`), and `AddKey` with gas-key permissions by `gas_key_add_key_exec_fee` (`cost.rs:928`). Also adds the gas-key storage-stake path in the deposit verifier (`runtime/runtime/src/verifier.rs:659`). |
| `one_yocto_on_promise` (flag, no `ProtocolFeature`) | PV 85 (`85.yaml:1`; param `Parameter::OneYoctoOnPromise`, `core/parameters/src/parameter.rs:320`) | A zero-balance contract may attach exactly 1 yoctoNEAR to a promise function call without being debited. The skipped yocto is effectively minted and tracked in `subsidized_amount` (`runtime/near-vm-runner/src/logic/host.rs:3583`, `host.rs:4423`), then netted out of the chunk burn (§3). Effective at PV 88 per `parameters.snap:315`. |
| Compute costs (NEP-455) | baseline (`_DeprecatedComputeCosts`, PV 61, `version.rs:168`) | Each `Fee`/`ParameterCost` carries a `compute` alongside `gas`. Compute debits the chunk wall-clock budget, not tokens. |
| Reduced gas refunds (NEP-536) | baseline at PV 78 (`_DeprecatedReducedGasRefunds`, `version.rs:315`) | `gas_refund_penalty` / `min_gas_refund_penalty` exist and are applied at `lib.rs:1319`, but the values are `0/100` and `0`, so the penalty is currently 0. `pessimistic_gas_price_inflation` was also set to `1/1` at PV 78 (`78.yaml:2`), making the pessimistic multiplier a no-op. |
| Global-contract deploy burn in outcome | baseline (`_DeprecatedIncludeDeployGlobalContractOutcomeBurntStorage`, PV 83, `version.rs:377`) | Global-contract deploy storage tokens are unconditionally folded into the outcome's `tokens_burnt` (`runtime/runtime/src/global_contracts.rs:49`). |

**Nightly-only, and therefore NOT in effect at PV 88** (both were de-stabilized from 87 during this release cycle):

| Feature | Nightly version | What stable PV 88 does instead |
|---|---|---|
| `FixMlDsaCostCharging` | 157 (`version.rs:487`, mapped at `version.rs:652`); flag moved from `87.yaml` into `157.yaml` | `fix_ml_dsa_cost_charging` is `false` (`parameters.snap:275`). Gas-key **send** fees stay priced on `public_key.trie_id_len()` rather than the wire length `len()` (`runtime/runtime/src/config.rs:73`), and the inner-delegate signature-verification **compute** stays charged on the signer shard (`config.rs:601` not taken; `runtime/runtime/src/actions.rs:467` not taken). Gas totals are identical either way; only the gas-key send *gas* for ML-DSA-65 keys and the shard whose `compute_limit` is debited would change. See §5. |
| `ReceiptPromiseInputSizeLimit` | 157 (`version.rs:450`, mapped at `version.rs:653`); `max_receipt_total_input_size` moved from `87.yaml` into `157.yaml` | `max_receipt_total_input_size` is `4_294_967_295` (`parameters.snap:249`), i.e. effectively unbounded, so the promise-input bound is not active and `TotalPromiseInputSizeExceeded` is unreachable on the stable path. No direct fee effect. |

> `CHANGELOG.md` under `## [2.14.0]` → `### Protocol Changes` still lists "Fixed two ML-DSA-65 cost-charging issues … (#16064)". That bullet is **wrong for this release**: the code and the parameter table are authoritative and put the fix on nightly only.

Other PV-87/88 features (`EnforceStorageProofLimitForAllActions` `version.rs:473`, `RejectDelegateV2` `version.rs:460`, `RejectEmptyMethodName` `version.rs:452`, `RejectWithdrawFromGasKeyInDelegate` `version.rs:465`, `EarlyKickout` `version.rs:395`, `GlobalContractSameChunkCallFix` `version.rs:413`, `RejectBlockHeaderV7BeforeSpice` `version.rs:505`) and the PV-86 `EnforcePerReceiptStorageProofLimit` (`version.rs:466`) do not change token flows. `ClampOutgoingGasAdmission` (PV 85, `version.rs:444`) bounds gas admission, not tokens, and `FixDelegateActionDepositWithFunctionCallError` (PV 85, `version.rs:353`) only fixes which validation error wins (`runtime/runtime/src/actions.rs:689`).

## Invariants & failure modes
- **Non-negative supply arithmetic.** `new_total_supply` uses checked add/sub and panics on overflow/underflow at block construction (`core/primitives/src/block.rs:195`); the verification path reports failure instead of panicking — arithmetic overflow short-circuits to `None` via `?`, and `balance_burnt > prev_total_supply + minted_amount` returns `Some(false)` (`block.rs:347`–`block.rs:353`).
- **Burn ≥ subsidy.** `total_balance_burnt = burnt − subsidized_amount` errors with "subsidized amount exceeds total burnt balance" if the subsidy would exceed the burn (`chain/chain/src/runtime/mod.rs:405`).
- **Minted ≤ cap.** `epoch_actual_reward` ≤ `epoch_total_reward` because per-validator rewards are fractions of the pool and below-threshold validators earn 0 (`reward_calculator.rs:109`). Asserted by `test_adjust_max_inflation` (`reward_calculator.rs:591`).
- **Burn price ≤ purchase price (post-`AccountCostIncrease`).** `min(purchase, block)` guarantees refunds (and any contract reward) never mint new tokens or underflow (comment at `runtime/runtime/src/lib.rs:1040`).
- **Refund penalty ≤ refund.** `gas_penalty_for_gas_refund` clamps to `min(penalty, gas_refund)` (`core/parameters/src/cost.rs:745`).
- **`gas_burnt_for_function_call ≤ gas_burnt`.** Asserted when merging action results (`runtime/runtime/src/lib.rs:488`).
- **Gas-price bounds.** Always clamped to `[min_gas_price, max_gas_price]` and unchanged on skipped blocks (`block.rs:465`, `block.rs:487`).
- **Storage staking.** An account failing `check_storage_stake` and not zero-balance cannot transact or receive deposits that push it under the threshold (`runtime/runtime/src/verifier.rs:375`, `verifier.rs:659`), and a receipt that leaves it under the threshold is rolled back with `ActionErrorKind::LackBalanceForState` (`runtime/runtime/src/lib.rs:1011`).
- **A state-init deposit is never silently kept.** Every path through `settle_state_init_deposit` either converts exactly the missing stake and refunds the remainder, or fails the action (`runtime/runtime/src/deterministic_account_id.rs:83`–`deterministic_account_id.rs:111`); at PV 88 a repeated universal state init refunds all of it (`runtime/runtime/src/universal_account_id.rs:65`).
- **Conversion-time state-init pricing is version-stable.** `universal_state_init_fee` always uses lenient `state_init_counts`, explicitly so that receipts created under an earlier protocol version re-price identically (doc comment at `core/primitives/src/universal_state_init.rs:162`–`universal_state_init.rs:167`). Only the VM's send-time charge takes a `DecodeMode`.
- **Prepaid = charged for contract-created state inits.** The VM walks the same `universal_state_init_size_terms` / `universal_state_init_content_terms` lists that `node_runtime::config` sums into the send and exec fee, so what a contract prepays cannot drift from what the action is charged when it runs (doc comment at `core/parameters/src/cost.rs:952`–`cost.rs:957`; the two lists at `cost.rs:958` and `cost.rs:967`, walked by the VM at `runtime/near-vm-runner/src/logic/host.rs:3410`/`host.rs:3413` and by the runtime at `runtime/runtime/src/config.rs:248`–`config.rs:250`).
- **Per-receipt state-init bounds.** `validate_state_init_counts` (`runtime/runtime/src/action_validation.rs:48`) folds the counts over *all* actions in a receipt — recursing into `Delegate`/`DelegateV2` inner actions, whose fees the outer receipt prepays (`core/primitives/src/action/mod.rs:518` — `Action::state_init_counts`) — and rejects over `max_state_init_entries` (1_500) or `max_universal_state_init_keys` (1_024), because a receipt can carry many byte-identical copies of a state init and pays for each. Each entry lands in the receipt's congestion gas whether or not it is burnt, and each key is charged at the `add_full_access_key` send rate, burnt at conversion.
- **Contract reward requires a live account.** If the receiver account is gone at the end of execution, its reward is not credited and remains burnt (`runtime/runtime/src/lib.rs:1126`). Vacuous at PV 87+ since the reward is always 0.
- **Config store no longer covers pre-84 versions.** `CONFIG_DIFFS` starts at 53 and `MIN_SUPPORTED_PROTOCOL_VERSION` is 84 (`core/parameters/src/config_store.rs:26`); replaying blocks from removed versions is out of scope for this binary.

## Code anchors
| Location | Symbol | What happens here |
|---|---|---|
| `chain/epoch-manager/src/reward_calculator.rs:51` | `RewardCalculator::calculate_reward` | Per-epoch issuance + treasury/validator split |
| `chain/epoch-manager/src/reward_calculator.rs:69` | `epoch_total_reward` | Time-proportional slice of max-inflation cap |
| `chain/epoch-manager/src/reward_calculator.rs:63` | `use_hardcoded_value` | `protocol_reward_rate` forced to 1/10 for prod-genesis chains |
| `chain/epoch-manager/src/reward_calculator.rs:109` | uptime gate | Below `online_min_threshold` ⇒ zero reward |
| `chain/epoch-manager/src/lib.rs:1185` | `calculate_reward` call site | Feeds `total_supply`, `epoch_duration`, `max_inflation_rate` at epoch boundary |
| `chain/epoch-manager/src/lib.rs:1165` | kickout filter | Kicked-out validators dropped from the stats map before reward |
| `core/primitives/src/block.rs:195` | `new_total_supply` | supply += minted − burnt |
| `core/primitives/src/block.rs:336` | `verify_total_supply_checked` | Verification counterpart; `Some(false)` on over-burn (`block.rs:350`) |
| `core/primitives/src/block.rs:456` | `compute_next_gas_price_checked` | Load-feedback gas-price adjustment + clamp |
| `chain/chain/src/types.rs:199` | `BlockEconomicsConfig::min_gas_price` | Effective min gas price; NEP-92-fix override |
| `chain/chain/src/types.rs:207` | `BlockEconomicsConfig::max_gas_price` | `min(genesis_max, min*20)` |
| `chain/chain/src/runtime/mod.rs:405` | `total_balance_burnt` | Burn total minus the 1-yN subsidy |
| `core/primitives/src/chunk_apply_stats.rs:243` | `BalanceStatsV1` | Per-chunk burn/subsidy/deficit ledger |
| `runtime/runtime/src/config.rs:481` | `calculate_tx_cost` | Splits tx into burnt vs purchased gas |
| `runtime/runtime/src/config.rs:532` | `receipt_gas_price` | `max(current, min_gas_purchase_price)` |
| `runtime/runtime/src/config.rs:589` | `signature_verification_cost` | ML-DSA-65 surcharge; inner-delegate compute stays on the signer shard at PV 88 |
| `runtime/runtime/src/config.rs:73` | `gas_key_send_pk_len` | Send fee on `trie_id_len()` at PV 88 (`len()` only under nightly `FixMlDsaCostCharging`) |
| `runtime/runtime/src/actions.rs:467` | delegate verify compute | Receiver-shard metering, gated off at PV 88 |
| `runtime/runtime/src/config.rs:242` | `universal_state_init_fee` | Conversion-time state-init fee, always lenient counts |
| `core/parameters/src/cost.rs:958` / `cost.rs:967` | `universal_state_init_size_terms` / `..._content_terms` | The two term lists both the runtime and the VM walk |
| `core/primitives/src/universal_state_init.rs:135` | `UniversalStateInit::from_raw_strict` | Decode + re-encode canonical check |
| `core/primitives/src/universal_state_init.rs:184` | `state_init_send_counts` | VM send-time counts; upper bound for rejected payloads in `Strict` |
| `runtime/near-vm-runner/src/logic/host.rs:3299` | `universal_state_init_to_account_id` | Canonical check (PV 88) then SHA3-256 + base32 |
| `runtime/near-vm-runner/src/logic/host.rs:3377` | `promise_batch_action_universal_state_init` | Contract-created state init; size terms charged before decode |
| `runtime/runtime/src/lib.rs:1038` | `gas_burn_price` | `min(purchase, block)` under `AccountCostIncrease` |
| `runtime/runtime/src/lib.rs:1093` | `tx_burnt_amount` | Tokens burnt for a receipt |
| `runtime/runtime/src/lib.rs:1105` | `tokens_burnt` | Outcome value, snapshotted before any contract reward |
| `runtime/runtime/src/lib.rs:1108` | `receiver_gas_reward` | `burnt_gas_reward` share — **0 at PV 87 and 88** |
| `runtime/runtime/src/lib.rs:1284` | `refund_unspent_gas_and_deposits` | Refund of unused gas + deposits, penalties, account charge |
| `runtime/runtime/src/global_contracts.rs:35` | global-contract storage burn | `0.0001 N`/byte burnt from the deployer |
| `runtime/runtime/src/deterministic_account_id.rs:73` | `settle_state_init_deposit` | Deposit → storage stake, remainder refunded |
| `runtime/runtime/src/universal_account_id.rs:52` | repeated state init | PV 88: refund the whole deposit instead of settling it |
| `runtime/runtime/src/action_validation.rs:48` | `validate_state_init_counts` | Per-receipt entry/key caps |
| `runtime/runtime/src/action_validation.rs:241` | `validate_transfer_action` | `0u` transfers banned between PV 87 and PV 88 |
| `core/parameters/src/cost.rs:736` | `gas_penalty_for_gas_refund` | NEP-536 refund penalty formula |
| `core/parameters/src/cost.rs:568` | `RuntimeFeesConfig` | Fee map, reward/penalty ratios, storage config |
| `core/parameters/src/cost.rs:618` | `StorageUsageConfig` | `storage_amount_per_byte` and friends |
| `core/parameters/src/cost.rs:788` / `cost.rs:826` | `transfer_exec_fee` / `transfer_send_fee` | `0u` receiver adds the `create_account` fee at PV 88 |
| `core/parameters/src/config_store.rs:26` | `CONFIG_DIFFS` | Per-version parameter diffs; `53.yaml` … `88.yaml` stable, `129/155/157` nightly |
| `core/parameters/src/config_store.rs:59` | `(88, include_config!("88.yaml"))` | The PV-88 diff entry added in this release |
| `core/parameters/src/config_store.rs:153` | genesis override | Genesis runtime config replaces **only** the key-0 entry |
| `core/parameters/src/config_store.rs:238` | `get_config` | Greatest stored version ≤ requested ⇒ `88.yaml` wins at PV 88 |
| `core/parameters/res/runtime_configs/87.yaml:3` | `burnt_gas_reward` | 3/10 → 0/1 |
| `core/parameters/res/runtime_configs/88.yaml:18` | `action_universal_state_init_per_entry` | send 0 → 3_100_000_000 |
| `core/parameters/res/runtime_configs/88.yaml:38` | `action_universal_state_init_per_byte` | send 72M → 78M, exec 70M → 74M |
| `core/parameters/res/runtime_configs/88.yaml:67` | `wasm_universal_state_init_to_account_id_base` | 8_520_000_000 → 8_900_000_000 |
| `core/parameters/res/runtime_configs/parameters.snap` | PV-88 parameter table | Authoritative effective values (base + diffs ≤ 88) |
| `runtime/runtime/src/verifier.rs:48` | `check_storage_stake` | Storage-staking enforcement |
| `runtime/near-vm-runner/src/logic/host.rs:3583` | one-yocto exemption | Zero-balance contract subsidy (effectively minted) |
| `runtime/runtime/src/function_call.rs:223` | `result.subsidized_amount` | VM outcome subsidy folded into the action result |
| `runtime/runtime/src/lib.rs:1269` | `ExecutionOutcome.tokens_burnt` | Snapshot moved into the outcome, pre-reward |
| `core/primitives-core/src/version.rs:477` | `RemoveGasRewards` (PV 87) | Headline change: no contract gas reward |
| `core/primitives-core/src/version.rs:491` | `UniversalAccounts` (PV 88) | Moved from 87; `0u` accounts and their fees |
| `core/primitives-core/src/version.rs:502` | `CanonicalUniversalStateInit` (PV 88) | Canonical encoding check + repriced fees |
| `core/primitives-core/src/version.rs:497` | `RefundRepeatedUniversalStateInitDeposit` (PV 88) | Whole-deposit refund on a repeat |
| `core/primitives-core/src/version.rs:487` | `FixMlDsaCostCharging` (**nightly 157**) | De-stabilized; stable keeps the unfixed pricing |
| `core/primitives/src/version.rs:58` | `get_protocol_upgrade_schedule` | Two-step 87→88 vote schedule |

## Open questions
- **Could a custom parameter table still pay gas rewards at PV ≥ 87?** *Not on mainnet or testnet.* `RemoveGasRewards` is parameter-only, but the parameter table is not chain-configurable: `RuntimeConfigStore::new`'s `genesis_runtime_config` override replaces **only the key `0`** entry (`core/parameters/src/config_store.rs:153`), and `get_config` returns the greatest key ≤ the protocol version (`config_store.rs:238`), which at PV 88 is the compiled-in `88.yaml` entry (`config_store.rs:59`). Testnet's `initial_testnet_config` therefore affects nothing at PV 88 (`config_store.rs:171`). The only ways to get a non-zero `burnt_gas_reward` are (a) a recompiled binary with a patched `parameters.yaml`/diff (they are compile-time `include_config!`s), (b) a cargo-feature variant such as `calimero_zero_storage` (`config_store.rs:101`) or a hardcoded chain override like `BENCHMARKNET` (`config_store.rs:174`), or (c) test-only constructors (`with_one_config`/`new_custom`, `config_store.rs:210`/`:215`). All of those are custom chains running a forked binary — a node doing that on mainnet/testnet would simply fall out of consensus. Remaining uncertainty is only editorial: whether the authors *intended* the stabilization to be parameter-only rather than also code-gated.
- The comment at `runtime/runtime/src/lib.rs:1129` ("Validators receive the remaining execution reward…") is **stale and wrong** for this tree: the un-credited remainder stays in `tx_burnt_amount`, flows into the chunk's `balance_burnt` (`chain/chain/src/runtime/mod.rs:405`) and is *subtracted from total supply* (`core/primitives/src/block.rs:195`) — destroyed, not paid to anyone. Validator income is exclusively the §1 inflation pool. Not corrected in code at this commit.
- `gas_deficit_amount` is accumulated (`runtime/runtime/src/lib.rs:1072`) but never consumed by supply accounting or block validation in this tree; its only consumer appears to be chunk-apply stats/telemetry.
- **Is the `FixMlDsaCostCharging` de-stabilization intentional for 2.14.0, or a release-branch accident?** The flag and `max_receipt_total_input_size` were both moved out of `87.yaml` into `157.yaml` (#16527, #16557, #16561) after rc.1, but `CHANGELOG.md`'s `## [2.14.0]` section still advertises the ML-DSA fix as shipped while the matching promise-input bullet was removed. Code is authoritative here (the fix is off at PV 88), but which of the two documents reflects the intent cannot be settled from the tree.
- **Was the mainnet 87→88 gap sized deliberately at two days?** The ordering itself is safe: 48 h is three to four mainnet epochs, so the two votes cannot share an epoch and PV 87 is live a full epoch before the PV 88 vote date (§10). What the tree cannot settle is *why* two days — whether the margin was picked with the `RejectUniversalAccountTransfers` → `UniversalAccounts` hand-off (§9.1) in mind, or just as the next convenient weekday.
- **Upper-bound send pricing for rejected payloads (§9.3) has no stated worst-case gas bound.** `upper_bound_counts` scales linearly with payload length, so the entry term alone is `3.1 Ggas × (len − 6) / 8`. For a payload near the transaction size limit this is a large send charge on a contract that passed garbage. Whether that is bounded elsewhere (by `max_state_init_entries` validation, which runs on the resulting receipt rather than on the charge) was not traced end-to-end.
