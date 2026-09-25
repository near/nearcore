# Transactions

A transaction in Near is a list of [actions](Actions.md) and additional information. There are two transaction versions:

### TransactionV0 (original)

```rust
pub struct TransactionV0 {
    /// An account on which behalf transaction is signed
    pub signer_id: AccountId,
    /// An access key which was used to sign a transaction
    pub public_key: PublicKey,
    /// Nonce is used to determine order of transaction in the pool.
    /// It increments for a combination of `signer_id` and `public_key`
    pub nonce: Nonce,
    /// Receiver account for this transaction
    pub receiver_id: AccountId,
    /// The hash of the block in the blockchain on top of which the given transaction is valid
    pub block_hash: CryptoHash,
    /// A list of actions to be applied
    pub actions: Vec<Action>,
}
```

### TransactionV1 (gas key support)

`TransactionV1` replaces the simple `Nonce` field with a `TransactionNonce` enum that supports both regular access keys and [gas keys](/DataStructures/AccessKey.md#gas-keys):

```rust
pub struct TransactionV1 {
    pub signer_id: AccountId,
    pub public_key: PublicKey,
    pub nonce: TransactionNonce,
    pub receiver_id: AccountId,
    pub block_hash: CryptoHash,
    pub actions: Vec<Action>,
}

pub enum TransactionNonce {
    /// Regular access key nonce.
    Nonce { nonce: Nonce },
    /// Gas key nonce with an index selecting which nonce slot to use.
    GasKeyNonce { nonce: Nonce, nonce_index: NonceIndex },
}
```

When `TransactionNonce::GasKeyNonce` is used, the transaction is treated as a gas key transaction: gas costs are deducted from the gas key balance, deposit costs from the account balance. The `nonce_index` selects which of the gas key's independent nonce slots to use (0..num_nonces).

## Signed Transaction

`SignedTransaction` is what the node receives from a wallet through JSON-RPC endpoint and then routed to the shard where `receiver_id` account lives. Signature proves an ownership of the corresponding `public_key` (which is an AccessKey for a particular account) as well as authenticity of the transaction itself.

```rust
pub struct SignedTransaction {
    pub transaction: Transaction,
    /// A signature of a hash of the Borsh-serialized Transaction
    pub signature: Signature,
```

Take a look some [scenarios](Scenarios/Scenarios.md) how transaction can be applied.

## Batched Transaction

A `Transaction` can contain a list of actions. When there are more than one action in a transaction, we refer to such
transaction as batched transaction. When such a transaction is applied, it is equivalent to applying each of the actions
separately, except:

* After processing a `CreateAccount` action, the rest of the action is applied on behalf of the account that is just created.
This allows one to, in one transaction, create an account, deploy a contract to the account, and call some initialization
function on the contract.
* `DeleteAccount` action, if present, must be the last action in the transaction.

The number of actions in one transaction is limited by `VMLimitConfig::max_actions_per_receipt`, the current value of which
is 100.

## Transaction Validation and Errors

When a transaction is received, various checks will be performed to ensure its validity. This section lists the checks
and potentially errors returned when they fail.

### Basic Validation

Basic validation of a transaction can be done without the state.

#### Valid `signer_id` format

Whether `signer_id` is valid. If not, a

```rust
/// TX signer_id is not in a valid format or not satisfy requirements see `near_core::primitives::utils::is_valid_account_id`
InvalidSignerId { signer_id: AccountId },
```

error is returned.

#### Valid `receiver_id` format

Whether `receiver_id` is valid. If not, a

```rust
/// TX receiver_id is not in a valid format or not satisfy requirements see `near_core::primitives::utils::is_valid_account_id`
InvalidReceiverId { receiver_id: AccountId },
```

error is returned.

#### Valid `signature`

Whether transaction is signed by the access key that corresponds to `public_key`. If not, a

```rust
/// TX signature is not valid
InvalidSignature
```

error is returned.

#### Number of actions does not exceed `max_actions_per_receipt`

Whether the number of actions included in the transaction is not greater than `max_actions_per_receipt`. If not, a

```rust
 /// The number of actions exceeded the given limit.
TotalNumberOfActionsExceeded { total_number_of_actions: u64, limit: u64 }
```

error is returned.

#### `DeleteAccount` is the last action

Among the actions in the transaction, whether `DeleteAccount`, if present, is the last action. If not, a

```rust
/// The delete action must be a final action in transaction
DeleteActionMustBeFinal
```

error is returned.

#### Prepaid gas does not exceed `max_total_prepaid_gas`

Whether total prepaid gas does not exceed `max_total_prepaid_gas`. If not, a

```rust
/// The total prepaid gas (for all given actions) exceeded the limit.
TotalPrepaidGasExceeded { total_prepaid_gas: Gas, limit: Gas }
```

error is returned.

#### Valid actions

Whether each included action is valid. Details of such check can be found in [Actions](Actions.md).

### Validation With State

After the basic validation is done, we check the transaction against current state to perform further validation.

#### Existing `signer_id`

Whether `signer_id` exists. If not, a

```rust
/// TX signer_id is not found in a storage
SignerDoesNotExist { signer_id: AccountId },
```

error is returned.

#### Valid transaction nonce

Whether the transaction nonce is greater than the existing nonce on the access key. If not, a

```rust
/// Transaction nonce must be strictly greater than `account[access_key].nonce`.
InvalidNonce { tx_nonce: Nonce, ak_nonce: Nonce },
```

error is returned.

#### Enough balance to cover transaction cost

If `signer_id` account has enough balance to cover the cost of the transaction. If not, a

```rust
 /// Account does not have enough balance to cover TX cost
NotEnoughBalance {
    signer_id: AccountId,
    balance: Balance,
    cost: Balance,
}
```

error is returned.

#### Access Key is allowed to cover transaction cost

If the transaction is signed by a function call access key and the function call access key does not have enough
allowance to cover the cost of the transaction, a

```rust
/// Access Key does not have enough allowance to cover transaction cost
NotEnoughAllowance {
    account_id: AccountId,
    public_key: PublicKey,
    allowance: Balance,
    cost: Balance,
}
```

error is returned.

#### Sufficient account balance to cover storage

If `signer_id` account does not have enough balance to cover its storage after paying for the cost of the transaction, a

```rust
/// Signer account doesn't have enough balance after transaction.
LackBalanceForState {
    /// An account which doesn't have enough balance to cover storage.
    signer_id: AccountId,
    /// Required balance to cover the state.
    amount: Balance,
}
```

error is returned.

#### Function call access key validations

If a transaction is signed by a function call access key, the following errors are possible:

* `InvalidAccessKeyError::RequiresFullAccess` if the transaction contains more than one action or if the only action it
contains is not a `FunctionCall` action.
* `InvalidAccessKeyError::DepositWithFunctionCall` if the function call action has nonzero `deposit`.
* `InvalidAccessKeyError::ReceiverMismatch { tx_receiver: AccountId, ak_receiver: AccountId }` if transaction's `receiver_id` does not match the `receiver_id` of the access key.
* `InvalidAccessKeyError::MethodNameMismatch { method_name: String }` if the name of the method that the transaction tries to call is not allowed by the access key.

### Gas Key Transaction Validation

When a transaction uses `TransactionNonce::GasKeyNonce`, additional validation is performed:

#### Valid gas key

The access key associated with `public_key` must be a gas key (have `GasKeyFullAccess` or `GasKeyFunctionCall` permission). If not, an `InvalidAccessKeyError` is returned.

#### Valid nonce index

The `nonce_index` must be less than the gas key's `num_nonces`. If not:

```rust
InvalidNonceIndex { tx_nonce_index: Option<NonceIndex>, num_nonces: NonceIndex }
```

#### Sufficient gas key balance

The gas key balance must be at least equal to the gas cost of the transaction. If not:

```rust
NotEnoughGasKeyBalance { signer_id: AccountId, balance: Balance, cost: Balance }
```

#### Cost splitting

For gas key transactions, the transaction cost is split into two independent checks:
- **Gas cost** (receipt fees + prepaid gas) is checked against the gas key balance
- **Deposit cost** (sum of deposits in all actions) is checked against the account balance

#### DepositFailed verdict

If the gas key has sufficient balance for gas costs but the account lacks balance for deposits, the transaction enters the `DepositFailed` state: gas is charged from the gas key (and the nonce is incremented), but the transaction actions are not executed. This prevents malicious contract behavior intending to add transactions to chunks without paying gas.

#### Function call permission constraints

If the gas key has `GasKeyFunctionCall` permission, the same function call access key validations apply (receiver, method name restrictions).
