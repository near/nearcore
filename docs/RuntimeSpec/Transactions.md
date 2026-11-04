# Transactions

A transaction in Near is a list of [actions](Actions.md) and additional information. There are two versions of a transaction:

```rust
pub enum Transaction {
    V0(TransactionV0),
    V1(TransactionV1),
}

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

pub struct TransactionV1 {
    pub signer_id: AccountId,
    pub public_key: PublicKey,
    /// Nonce is used to determine order of transaction in the pool.
    /// It increments for a combination of `signer_id` and `public_key`,
    /// and for gas key it also includes a `nonce_index`.
    pub nonce: TransactionNonce,
    pub receiver_id: AccountId,
    pub block_hash: CryptoHash,
    pub actions: Vec<Action>,
    /// Controls nonce validation mode (monotonic or strict sequential).
    pub nonce_mode: NonceMode,
}

pub enum TransactionNonce {
    /// Simple nonce without index, used by ordinary access keys
    Nonce { nonce: Nonce },
    /// Nonce with index, used by gas keys
    GasKeyNonce { nonce: Nonce, nonce_index: NonceIndex },
}

pub enum NonceMode {
    /// Any nonce strictly greater than the current access key nonce (default behavior).
    Monotonic,
    /// Nonce must be exactly `ak_nonce + 1` (sequential ordering).
    Strict,
}
```

The fields of `TransactionV1` have the same meaning as in `TransactionV0`. Its `nonce` is a `TransactionNonce`, and its last field `nonce_mode` sets how the nonce is checked. See [Valid transaction nonce](#valid-transaction-nonce). `NonceIndex` is a `u16`.

A `TransactionV0` is Borsh-serialized as the struct alone, with no prefix. A `TransactionV1` is Borsh-serialized as the byte 1 followed by the `TransactionV1` struct. To tell the two apart, a decoder reads the first two bytes. A `TransactionV0` starts with the 4-byte little-endian length of `signer_id`, which is at most 64, so its second byte is always 0. A `TransactionV1` starts with the byte 1 followed by the length of `signer_id`, so its second byte is not 0.

`TransactionV1` is accepted from protocol version 85 (nearcore 2.13.0). Before that, it is rejected with `InvalidTransactionVersion`. A transaction signed with a [gas key](../DataStructures/AccessKey.md#gas-keys) must be a `TransactionV1` with `TransactionNonce::GasKeyNonce`. See [Gas key transactions](#gas-key-transactions). `NonceMode::Strict` can only be set in a `TransactionV1`. A `TransactionV0` always uses `NonceMode::Monotonic`.

## Signed Transaction

`SignedTransaction` is what the node receives from a wallet through JSON-RPC endpoint and then routed to the shard where `receiver_id` account lives. Signature proves an ownership of the corresponding `public_key` (which is an AccessKey for a particular account) as well as authenticity of the transaction itself.

```rust
pub struct SignedTransaction {
    pub transaction: Transaction,
    /// A signature of a hash of the Borsh-serialized Transaction
    pub signature: Signature,
}
```

The signed hash covers the whole Borsh encoding of the `Transaction`, including the leading byte 1 of a `TransactionV1`.

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
A transaction signed with a gas key has some different checks, see [Gas key transactions](#gas-key-transactions).

#### Existing `signer_id`

Whether `signer_id` exists. If not, a

```rust
/// TX signer_id is not found in a storage
SignerDoesNotExist { signer_id: AccountId },
```

error is returned.

#### Valid transaction nonce

The transaction nonce is checked against the current nonce of the access key.
For a gas key, the current nonce is the nonce stored for the `nonce_index` of the transaction.
With `NonceMode::Monotonic`, which is the default, the transaction nonce must be greater than the current nonce.
With `NonceMode::Strict`, the transaction nonce must be exactly the current nonce plus 1.
`NonceMode::Strict` is available from protocol version 85 (nearcore 2.13.0). If the check fails, a

```rust
/// Transaction nonce must be strictly greater than `account[access_key].nonce`.
InvalidNonce { tx_nonce: Nonce, ak_nonce: Nonce },
```

error is returned.

The transaction nonce must also be less than the height of the block that includes the transaction, multiplied by 1,000,000. If not, a

```rust
/// Transaction nonce is larger than the upper bound given by the block height
NonceTooLarge { tx_nonce: Nonce, upper_bound: Nonce },
```

error is returned. A node does not know this block when a transaction is submitted, so it checks this bound only when it produces or applies a chunk.

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

### Gas key transactions

From protocol version 85 (nearcore 2.13.0), a transaction can be signed with a [gas key](../DataStructures/AccessKey.md#gas-keys).
Such a transaction must be a `TransactionV1` with `TransactionNonce::GasKeyNonce { nonce, nonce_index }`.
Its gas is paid from the gas key balance, and its deposits are paid from the account balance.
The checks below replace the balance, allowance and storage checks of [Validation With State](#validation-with-state).
The other checks still apply. This includes the nonce check and, for a `GasKeyFunctionCall` key, the function call access key validations.

#### Valid nonce index

The `nonce_index` must be less than the `num_nonces` of the gas key. If not, a

```rust
/// Transaction is specifying an invalid nonce index. Gas key transactions
/// must have a nonce_index in valid range, regular transactions must not.
InvalidNonceIndex { tx_nonce_index: Option<NonceIndex>, num_nonces: NonceIndex },
```

error is returned. If the access key is not a gas key, the error has `num_nonces: 0`.
The same error is returned with `tx_nonce_index: None` when a gas key signs a transaction without a `nonce_index`.
That is a `TransactionV0` or a `TransactionV1` with `TransactionNonce::Nonce`.

The transaction nonce is then checked against the nonce stored for `nonce_index`, as described in [Valid transaction nonce](#valid-transaction-nonce).

#### Enough gas key balance

The gas key balance must cover the gas cost of the transaction.
The gas cost is the gas burnt to convert the transaction into a receipt, at the current gas price, plus the remaining gas of the receipt, at the receipt gas price.
The receipt gas price is the larger of the current gas price and `min_gas_purchase_price`.
See [Fees](Fees/Fees.md#gas-key-transactions). If the balance is too low, a

```rust
/// Gas key does not have enough balance to cover gas costs.
NotEnoughGasKeyBalance { signer_id: AccountId, balance: Balance, cost: Balance },
```

error is returned.

#### Enough account balance for deposits

The account balance must cover the sum of the deposits of all actions.
After paying them, the account must still have enough balance for its storage. If not, a

```rust
/// Gas key transaction failed because the account could not cover the deposit cost.
/// Gas is still charged from the gas key in this case.
NotEnoughBalanceForDeposit {
    signer_id: AccountId,
    balance: Balance,
    cost: Balance,
    reason: DepositCostFailureReason,
}

pub enum DepositCostFailureReason {
    NotEnoughBalance,
    LackBalanceForState,
}
```

error is returned.
With `NotEnoughBalance`, the account balance does not cover the deposits. Then `balance` is the account balance and `cost` is the sum of the deposits.
With `LackBalanceForState`, the balance left after the deposits does not cover the storage of the account. In this case, `balance` is that remaining balance and `cost` is the missing amount.

Nodes reject such a transaction when it is submitted, and chunk producers do not include it.
When a chunk contains such a transaction, the transaction fails when it is converted to a receipt, and no receipt is created.
The gas key is still charged for the gas burnt to convert the transaction, at the current gas price, and the nonce for `nonce_index` is updated.
The rest of the gas is not charged, so nothing is refunded. The account balance does not change.
