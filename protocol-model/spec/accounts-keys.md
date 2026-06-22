# Accounts, keys & access control

> Protocol version: 86 (stable) · Derived from commit: f0c7706f · Generated: 2026-06-22
> Primary crates/files: `core/primitives-core/src/account.rs`, `near-account-id` (crate `near-account-id` 2.0.0), `runtime/runtime/src/access_keys.rs`, `runtime/runtime/src/verifier.rs`, `core/primitives/src/transaction.rs`, `core/primitives-core/src/deterministic_account_id.rs`

## Role

This component defines the **account model and authorization layer**: the on-chain `Account` record (balance, stake, storage usage, deployed contract), the access keys that authorize transactions against an account, gas keys (prepaid-balance access keys), the nonce rules that prevent replay, the validity/typing rules for account IDs, and the storage-staking accounting that ties an account's balance to its storage footprint. It is a *data + rules* component: it owns the structures and the authorization/validity predicates, but the state *transitions* that mutate accounts and keys live in [runtime-execution](runtime-execution.md) (the action handlers call into `access_keys.rs`/`actions.rs`), the transaction verifier path that *uses* these rules at conversion time also lives there, and the trie layout where accounts/keys are persisted lives in [state-storage](state-storage.md). Stake held in `Account::locked` is read and rewritten by [epoch-validators-staking](epoch-validators-staking.md).

## Key data structures

- **`Account`** — `core/primitives-core/src/account.rs:39` — the per-account state record. An enum over `V1(AccountV1)` / `V2(AccountV2)`. Accessors (`amount`, `locked`, `storage_usage`, `contract`, `version`) erase the version (`:186`–`:226`). `Account::new` (`:166`) picks the *minimum* version that can represent the contract: `AccountContract::None`/`Local` stay V1; anything else forces V2.
- **`AccountV1`** — `core/primitives-core/src/account.rs:55` — original layout: `amount`, `locked`, `code_hash: CryptoHash`, `storage_usage`. The contract is implicit: `code_hash == default` means no contract, otherwise a `Local` contract (`AccountContract::from_local_code_hash`, `:106`).
- **`AccountV2`** — `core/primitives-core/src/account.rs:146` — replaces the bare `code_hash` field with an explicit `contract: AccountContract`. Same `amount`/`locked`/`storage_usage`.
- **`AccountContract`** — `core/primitives-core/src/account.rs:89` — the contract-association mode: `None` | `Local(CryptoHash)` (code stored under this account) | `Global(CryptoHash)` (shared code addressed by its hash) | `GlobalByAccount(AccountId)` (shared code addressed by the publisher account). `identifier_storage_usage` (`:126`) charges 0 bytes for `None`/`Local`, 32 for `Global`, and `id.len()` for `GlobalByAccount`.
- **`AccessKey`** — `core/primitives-core/src/account.rs:467` — `{ nonce: Nonce, permission: AccessKeyPermission }`. Identified in state by `(account_id, public_key)`; one account can hold many. For a fresh regular key `nonce` is seeded to `(block_height-1) * 1_000_000` to avoid tx-hash collisions on re-creation (`ACCESS_KEY_NONCE_RANGE_MULTIPLIER = 1_000_000`, `:478`). For gas keys the on-key `nonce` is unused and forced to 0; nonces live in separate per-index trie entries.
- **`AccessKeyPermission`** — `core/primitives-core/src/account.rs:575` — `FunctionCall(FunctionCallPermission)` | `FullAccess` | `GasKeyFunctionCall(GasKeyInfo, FunctionCallPermission)` | `GasKeyFullAccess(GasKeyInfo)`. `MAX_NONCES_FOR_GAS_KEY = 1024` (`:589`). Helpers `function_call_permission` (`:591`) and `gas_key_info` (`account.rs:516`) project the relevant inner data regardless of variant.
- **`FunctionCallPermission`** — `core/primitives-core/src/account.rs:625` — restricts a key to function-call use: `allowance: Option<Balance>` (`None` = unlimited; spent in lockstep with account balance), `receiver_id: String` (the only allowed receiver; a `String` not `AccountId` because legacy testnet genesis holds invalid values), `method_names: Vec<String>` (allowed methods; empty = any).
- **`GasKeyInfo`** — `core/primitives-core/src/account.rs:546` — `{ balance: Balance, num_nonces: NonceIndex }`. `balance` is a prepaid pot used to pay gas; `num_nonces` is the count of independent nonce slots. `MAX_BALANCE_TO_BURN = 1 NEAR` (`:554`) caps the balance that may be burned when deleting the key/account.
- **`TransactionNonce`** — `core/primitives/src/transaction.rs:63` — `Nonce { nonce }` (regular keys) | `GasKeyNonce { nonce, nonce_index }` (gas keys). `nonce_index()` returning `Some` is what routes a tx down the gas-key path. `TransactionV0` carries a bare `Nonce`; `TransactionV1` carries a `TransactionNonce` plus a `nonce_mode` (`transaction.rs:128`,`:136`).
- **`NonceMode`** — `core/primitives/src/transaction.rs:110` — `Monotonic` (default; any nonce strictly greater) | `Strict` (`tx_nonce == ak_nonce + 1`). `Transaction::nonce_mode` forces V0 txs to `Monotonic` (`transaction.rs:211`).
- **`AccountType`** — `near-account-id` `account_id_ref.rs:39` — `NamedAccount` | `NearImplicitAccount` | `EthImplicitAccount` | `NearDeterministicAccount`. `is_implicit()` is true for the latter three (`:53`).

## Behavior

### Account versioning & serialization

1. `Account` chooses the lowest version that fits the contract mode: `None`/`Local` → V1, everything else → V2 (`account.rs:166` — `Account::new`). `set_contract` upgrades a V1 account to V2 in place the moment a `Global`/`GlobalByAccount` contract is assigned (`account.rs:276`).
2. **Borsh**: the first serialized field of every format is a `u128`. V1 serializes as a bare `AccountV1`. V2 is detected by a sentinel `u128::MAX` (`SERIALIZATION_SENTINEL`, `account.rs:164`) prepended before a `BorshVersionedAccount` enum (`:405`); the sentinel is chosen because total supply can never reach `u128::MAX`, so it is unambiguous (`BorshDeserialize for Account`, `account.rs:410`).
3. **Serde** uses `SerdeAccount` (`account.rs:307`) carrying both `code_hash` and optional `global_contract_hash`/`global_contract_account_id`. Deserialization rejects an account that holds *both* a local and a global contract, or both global-contract forms (`account.rs:322`).

### Contract-association modes

A receiver account's `AccountContract` tells the runtime where to fetch code for a `FunctionCall`. `Local(hash)` reads code stored under this account; `Global(hash)` and `GlobalByAccount(id)` reference a single shared deployment so many accounts can run the same code without each paying to store it (global contracts; the deploy/use transitions and `GlobalContractIdentifier`, `core/primitives-core/src/global_contract.rs:25`, are in [runtime-execution](runtime-execution.md)). ETH-implicit accounts are created already pointing at a `Global` wallet contract (`actions.rs:243`). Only `Local` code counts toward this account's own contract storage; `Global` costs a fixed 32-byte identifier and `GlobalByAccount` costs the publisher id's length (`account.rs:126`).

### Access keys: add / delete / update

`action_add_key` (`access_keys.rs:149`): rejects a duplicate public key with `AddKeyAlreadyExists` (`:157`), then branches on whether the new key's permission carries a `GasKeyInfo`:

1. **Regular key** (`add_regular_key`, `:230`): seeds `access_key.nonce = (block_height-1)*1_000_000` (`initial_nonce_value`, `:46`), writes it with `set_access_key`, and adds `access_key_storage_usage` (`:17`) to the account's `storage_usage`. Storage usage uses `public_key.trie_id_len()` (the on-trie identifier length), so an ML-DSA-65 key — stored as a 33-byte SHA3-256 hash — costs the same as ed25519 rather than its ~1953-byte raw form (`:26`, asserted by `test_ml_dsa_65_access_key_storage_scales`).
2. **Gas key** (`add_gas_key`, `:194`): forces the on-key `nonce` to 0, writes the access key, then writes `num_nonces` separate nonce entries each initialized to `initial_nonce_value(block_height)` via `set_gas_key_nonce` (`:212`). Storage usage uses `gas_key_storage_cost` (`:31`) = the access-key cost plus, per nonce, key length (`trie_id_len + size_of::<NonceIndex>`) + value length (`size_of::<Nonce>`) + `num_extra_bytes_record`.

`action_delete_key` (`access_keys.rs:52`) looks the key up; missing → `DeleteKeyDoesNotExist` (`:84`). Otherwise:
- **Regular** (`delete_regular_key`, `:136`): removes the key and `saturating_sub`s its storage cost.
- **Gas key** (`delete_gas_key`, `:93`): if `balance > MAX_BALANCE_TO_BURN` (1 NEAR) it errors `GasKeyBalanceTooHigh` and leaves the key intact (`:103`); otherwise it adds the balance to `result.tokens_burnt` (the prepaid pot is **burned**, not refunded), removes every nonce entry, charges removal compute, removes the access key, and `saturating_sub`s the gas-key storage cost.

Gas-key balance is moved (never via AddKey): `action_transfer_to_gas_key` (`:257`) adds a deposit to `GasKeyInfo.balance`; `action_withdraw_from_gas_key` (`:290`) subtracts from the gas-key balance (erroring `InsufficientGasKeyBalance` on underflow, `:316`) and credits the account `amount`. Both error `GasKeyDoesNotExist` if the key is absent or not a gas key (`:264`,`:271`).

### Transaction authorization (verifier — used by [runtime-execution](runtime-execution.md))

These functions are pure (no state mutation); they return a verdict that the runtime applies.

1. **Signer & key lookup**: `get_signer_and_access_key` (`verifier.rs:133`) → `SignerDoesNotExist` if no account, `AccessKeyNotFound` if no key for the public key.
2. **Nonce** (`verify_nonce`, `verifier.rs:211`): `Monotonic` requires `tx_nonce > current_nonce`; `Strict` requires `tx_nonce == current_nonce + 1`; either failure is `InvalidNonce { tx_nonce, ak_nonce }`. An upper bound `block_height * 1_000_000` rejects `NonceTooLarge` (`:229`). The "current" nonce is `max(stored_nonce, pending.max_nonce)` to account for in-flight txs (`:302`,`:416`).
3. **FunctionCall permission** (`verify_function_call_permission`, `verifier.rs:166`): the tx must be exactly one action and it must be a `FunctionCall` (else `RequiresFullAccess`), with **zero** deposit (else `DepositWithFunctionCall`), `tx.receiver_id == permission.receiver_id` (else `ReceiverMismatch`), and method in `method_names` when non-empty (else `MethodNameMismatch`).
4. **Allowance**: `check_and_compute_new_allowance` (`verifier.rs:239`) — for a FunctionCall key with a finite `allowance`, subtracts `total_cost`; underflow → `NotEnoughAllowance`. Allowance is decremented in lockstep with the account balance.
5. **Regular path** (`verify_and_charge_tx_ephemeral`, `verifier.rs:269`): asserts no `nonce_index`; if the key is actually a gas key it is rejected (`InvalidNonceIndex { tx_nonce_index: None }`, `:285`) — gas keys *must* use the gas-key path. Checks balance (`NotEnoughBalance`), debits `total_cost`, runs allowance + `check_storage_stake`, and returns `AccessKeyUpdate::Regular { nonce: tx_nonce, new_allowance }`.
6. **Gas-key path** (`verify_and_charge_gas_key_tx_ephemeral`, `verifier.rs:370`): requires a `nonce_index`; the key must be a gas key (else `AccessKeyNotFound`, `:398`); `nonce_index` must be `< num_nonces` (else `InvalidNonceIndex`, `:408`). Then it **splits the cost**: the gas portion (`gas_cost`) is drawn from the gas-key `balance` (`NotEnoughGasKeyBalance` on shortfall, `:440`); the deposit portion (`deposit_cost`) is drawn from the account `amount`. If the account cannot cover the deposit, it returns `DepositFailed` (`:492`) — gas is still charged to the gas key (charging only `burnt_amount`), the deposit is dropped, no receipt deposit is sent. Success returns `AccessKeyUpdate::GasKey { new_balance, nonce_index, nonce }`, persisted to the indexed nonce entry.

### Account-id validity, creation, and typing

Validity (`near-account-id` `validation.rs:43` — `validate`): length 2..=64 (`MIN_LEN`/`MAX_LEN`, `validation.rs:4`,`:6`); allowed chars `a-z 0-9 - _ .`; separators (`-_.`) may not lead, trail, or repeat. `validate_receipt` re-validates predecessor/receiver ids on receipts (`verifier.rs:547`,`:552`); `validate_action_receipt` additionally re-validates the `refund_to` id (`verifier.rs:602`).

Typing (`get_account_type`, `account_id_ref.rs:203`), checked in priority order:
- **EthImplicit** — `0x` + 40 lowercase hex (`is_eth_implicit`, `validation.rs:96`).
- **NearImplicit** — exactly 64 lowercase hex chars (`is_near_implicit`, `validation.rs:114`).
- **NearDeterministic** (NEP-616) — `0s` + 40 lowercase hex (`is_near_deterministic`, `validation.rs:105`).
- **Named** — everything else. `is_top_level` = not `system` and contains no `.` (`account_id_ref.rs:146`); `is_sub_account_of` checks the id is `<label>.<parent>` with a single extra label (`:172`).

Creation (`action_create_account`, `runtime/runtime/src/actions.rs:152`): a top-level id shorter than `min_allowed_top_level_account_length` may only be created by the `registrar_account_id` (else `CreateAccountOnlyByRegistrar`, `:166`); a non-top-level id must be a direct sub-account of the predecessor (else `CreateAccountNotAllowed`, `:176`). The new account starts with zero balance/stake, `AccountContract::None`, and `storage_usage = num_bytes_account`.

Implicit/deterministic creation by transfer (`action_implicit_account_creation_transfer`, `actions.rs:198`): branches on `get_account_type`. **NearImplicit** derives a `FullAccess` key from the hex (the account id *is* the ed25519 public key) and adds the key's storage. **EthImplicit** is created with a `Global` wallet contract addressed by `eth_wallet_global_contract_hash(chain_id)`. **NearDeterministic** calls `create_deterministic_account` (`runtime/runtime/src/deterministic_account_id.rs:92`), starting `None`/`num_bytes_account`; the state-init payload (`DeterministicAccountStateInit`, `core/primitives-core/src/deterministic_account_id.rs:22`) is applied separately. A `NamedAccount` here is unreachable.

### Storage staking

`check_storage_stake` (`verifier.rs:47`) requires `amount + locked >= storage_amount_per_byte * storage_usage`; shortfall returns `LackBalanceForStorageStaking(needed)`, surfaced to transactions as `LackBalanceForState`. **Exception**: a *zero-balance account* (NEP-448) — `storage_usage <= ZERO_BALANCE_ACCOUNT_STORAGE_LIMIT = 770` bytes (`verifier.rs:24`,`:87`) — always passes regardless of balance. `storage_usage` is maintained incrementally by every action that adds/removes keys, code, or data (e.g. `access_key_storage_usage`, `action_deploy_contract` at `actions.rs:272`).

## Interactions

- **Consumes**: `RuntimeConfig` fees (`storage_amount_per_byte`, `num_bytes_account`, `num_extra_bytes_record`), `AccountCreationConfig` (registrar id, min top-level length), block height (for nonce seeding), and the trie via `TrieUpdate` getters/setters in [state-storage](state-storage.md) (`get_account`/`set_access_key`/`set_gas_key_nonce`).
- **Produces / mutated by**: action handlers in [runtime-execution](runtime-execution.md) (`AddKey`/`DeleteKey`/`Transfer`/`CreateAccount`/`DeployContract`/`Stake`/`TransferToGasKey`/`WithdrawFromGasKey`) mutate the `Account` and keys; the verifier path there authorizes transactions using `AccessKey`/`FunctionCallPermission`/nonce rules defined here.
- **Touches**: `Account::locked` is the staked balance read/rewritten by [epoch-validators-staking](epoch-validators-staking.md); global-contract code referenced by `AccountContract::Global`/`GlobalByAccount` is managed by [runtime-execution](runtime-execution.md); persisted layout in [state-storage](state-storage.md).

## Protocol-version-gated behavior

All versions cited from `core/primitives-core/src/version.rs` (`protocol_version`, `:453`).

| Feature | Activates | Effect on this component |
|---|---|---|
| `GasKeys` | v85 (`version.rs:557`) | Enables `TransactionV1` with `GasKeyNonce`, the `GasKeyFunctionCall`/`GasKeyFullAccess` permissions, and `GasKeyInfo` balance/nonce model. `Transaction::gas_keys_required()` is true for V1 (`transaction.rs:204`). |
| `StrictNonce` | v85 (`version.rs:561`) | Allows `NonceMode::Strict` on `TransactionV1` requiring `tx_nonce == ak_nonce + 1`; pre-feature txs are effectively `Monotonic` (`verify_nonce`, `verifier.rs:223`). |
| `AccountCostIncrease` | v85 (`version.rs:569`) | Raises account-creation cost and changes gas-refund/penalty pricing for created accounts; gating sites in `runtime/runtime/src/lib.rs:906,963,984,1187,1228`. (Pricing detail in [runtime-execution](runtime-execution.md).) |
| `FixDeleteAccountGlobalContractStorageUsage` | v85 (`version.rs:555`) | `action_delete_account` now subtracts the global-contract *identifier* storage too, not just local code (`actions.rs:311`). |
| `PostQuantumSignatures` | v85 (`version.rs:562`) | Adds ML-DSA-65 as a third key/signature scheme; `AddKey`/txs carrying such keys are rejected pre-feature. Storage usage uses `trie_id_len()` so PQ access keys cost the same as ed25519 (`access_keys.rs:26`). |
| Global contracts | base by v77/v82/v83 (deprecated flags `_DeprecatedGlobalContracts` etc., `version.rs:533`,`:547`) | `AccountContract::Global`/`GlobalByAccount` (hence `Account` V2) are part of the v86 base protocol; the original feature gates are deprecated/folded in. |
| Deterministic account ids (NEP-616) | base by v82 (`_DeprecatedDeterministicAccountIds`, `version.rs:543`) | The `0s…` `NearDeterministicAccount` type and `create_deterministic_account` are in the v86 base. `FixDelegatedDeterministicStateInit` (v85, `version.rs:556`) fixes the delegated-action path. |

Account V2 itself has no live gating flag at v86 — the historical `_DeprecatedAccountVersions` was v46 (`version.rs:471`); V1/V2 coexist purely as a serialization concern.

## Invariants & failure modes

- **One contract kind per account**: an account cannot hold both a local and a global contract, nor two global forms — enforced at serde deserialization (`account.rs:328`–`:342`) and structurally by `AccountContract` having a single variant.
- **Gas-key on-key nonce is always 0**: `add_gas_key` forces it (`access_keys.rs:206`); the real nonces live in indexed entries.
- **Storage stake backs storage usage** unless zero-balance: `check_storage_stake` (`verifier.rs:47`); violation → `LackBalanceForStorageStaking`/`LackBalanceForState`. An arithmetic-overflow inconsistency (`storage_amount_per_byte * storage_usage` or `amount + locked` overflows, `verifier.rs:53`,`:63`) returns `StorageStakingError::StorageError`, surfaced as `StorageInconsistentState`.
- **Gas-key deletion burns ≤ 1 NEAR**: `delete_gas_key` errors `GasKeyBalanceTooHigh` and aborts if `balance > MAX_BALANCE_TO_BURN`; otherwise the balance is burned (added to `tokens_burnt`, not refunded) (`access_keys.rs:103`,`:112`). Account deletion sums all gas-key balances against the same threshold (asserted by `test_delete_account_gas_key_balance_too_high`).
- **Account deletion size cap**: `MAX_ACCOUNT_DELETION_STORAGE_USAGE = 10_000` bytes (`account.rs:160`); larger accounts cannot be deleted (`actions.rs:333`).
- **Nonce monotonicity prevents replay**: `verify_nonce` rejects stale/equal (Monotonic) or non-sequential (Strict) nonces (`verifier.rs:211`).
- **Storage-usage overflow** while adding a key is fatal: `StorageInconsistentState` (`access_keys.rs:220`,`:248`).
- **Permission mismatch on a FunctionCall key** rejects the whole tx at verification (`RequiresFullAccess`/`DepositWithFunctionCall`/`ReceiverMismatch`/`MethodNameMismatch`/`NotEnoughAllowance`).

## Code anchors

| Location | Symbol | What happens here |
|---|---|---|
| `core/primitives-core/src/account.rs:39` | `Account` | V1/V2 enum; version-erasing accessors. |
| `core/primitives-core/src/account.rs:166` | `Account::new` | Picks min version per contract mode. |
| `core/primitives-core/src/account.rs:89` | `AccountContract` | None/Local/Global/GlobalByAccount + per-mode storage cost (`:126`). |
| `core/primitives-core/src/account.rs:410` | `BorshDeserialize for Account` | Sentinel-based V1/V2 borsh detection. |
| `core/primitives-core/src/account.rs:322` | `Deserialize for Account` | Rejects mixed local/global contracts. |
| `core/primitives-core/src/account.rs:467` | `AccessKey` | nonce + permission; `ACCESS_KEY_NONCE_RANGE_MULTIPLIER` (`:478`). |
| `core/primitives-core/src/account.rs:575` | `AccessKeyPermission` | Full/FunctionCall/GasKey variants; `MAX_NONCES_FOR_GAS_KEY` (`:589`). |
| `core/primitives-core/src/account.rs:625` | `FunctionCallPermission` | allowance/receiver/method restrictions. |
| `core/primitives-core/src/account.rs:546` | `GasKeyInfo` | balance + num_nonces; `MAX_BALANCE_TO_BURN` (`:554`). |
| `core/primitives/src/transaction.rs:63` | `TransactionNonce` | Nonce vs GasKeyNonce; `nonce_index()` routing. |
| `core/primitives/src/transaction.rs:110` | `NonceMode` | Monotonic vs Strict. |
| `runtime/runtime/src/access_keys.rs:149` | `action_add_key` | Add regular/gas key; storage accounting. |
| `runtime/runtime/src/access_keys.rs:52` | `action_delete_key` | Delete regular/gas key; balance burn. |
| `runtime/runtime/src/access_keys.rs:257` | `action_transfer_to_gas_key` | Funds gas-key balance. |
| `runtime/runtime/src/access_keys.rs:290` | `action_withdraw_from_gas_key` | Moves gas-key balance to account. |
| `runtime/runtime/src/access_keys.rs:46` | `initial_nonce_value` | `(block_height-1)*1e6` nonce seed. |
| `runtime/runtime/src/verifier.rs:47` | `check_storage_stake` | Storage-staking invariant + zero-balance exemption. |
| `runtime/runtime/src/verifier.rs:166` | `verify_function_call_permission` | FunctionCall key constraints. |
| `runtime/runtime/src/verifier.rs:211` | `verify_nonce` | Monotonic/Strict nonce + upper bound. |
| `runtime/runtime/src/verifier.rs:269` | `verify_and_charge_tx_ephemeral` | Regular-tx authorization + charge. |
| `runtime/runtime/src/verifier.rs:370` | `verify_and_charge_gas_key_tx_ephemeral` | Gas-key authorization; gas/deposit cost split; DepositFailed. |
| `runtime/runtime/src/actions.rs:152` | `action_create_account` | Named/top-level/sub-account creation rules. |
| `runtime/runtime/src/actions.rs:198` | `action_implicit_account_creation_transfer` | Near/Eth/Deterministic implicit creation. |
| `near-account-id validation.rs:43` | `validate` | Char/length/separator rules. |
| `near-account-id account_id_ref.rs:203` | `get_account_type` | Eth/Near-implicit/Deterministic/Named typing. |
| `core/primitives-core/src/version.rs:453` | `ProtocolFeature::protocol_version` | Activation versions for gated features. |

## Open questions

- The exact gas/cost-pricing changes introduced by `AccountCostIncrease` (v85) are only referenced here; the pricing arithmetic lives in [runtime-execution](runtime-execution.md) (`runtime/runtime/src/lib.rs:1187`+) and was not re-derived in this spec.
- The full `DeterministicStateInit` apply path (how `data`/`code` are written and charged for `0s…` accounts) is owned by [runtime-execution](runtime-execution.md); only the account-record creation is documented here.
