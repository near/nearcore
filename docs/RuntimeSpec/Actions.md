# Actions

There are a several action types in Near:

```rust
pub enum Action {
    CreateAccount(CreateAccountAction),
    DeployContract(DeployContractAction),
    FunctionCall(FunctionCallAction),
    Transfer(TransferAction),
    Stake(StakeAction),
    AddKey(AddKeyAction),
    DeleteKey(DeleteKeyAction),
    DeleteAccount(DeleteAccountAction),
    Delegate(SignedDelegateAction),
}
```

Each transaction consists a list of actions to be performed on the `receiver_id` side. Since transactions are first
converted to receipts when they are processed, we will mostly concern ourselves with actions in the context of receipt
processing.
 
For the following actions, `predecessor_id` and `receiver_id` are required to be equal:

- `DeployContract`
- `Stake`
- `AddKey`
- `DeleteKey`
- `DeleteAccount`

NOTE: if the first action in the action list is `CreateAccount`, `predecessor_id` becomes `receiver_id`
for the rest of the actions until `DeleteAccount`. This gives permission by another account to act on the newly created account.

## CreateAccountAction

```rust
pub struct CreateAccountAction {}
```

If `receiver_id` has length == 64, this account id is considered to be `hex(public_key)`, meaning creation of account only succeeds if followed up with `AddKey(public_key)` action.

**Outcome**:

- creates an account with `id` = `receiver_id`
- sets Account `storage_usage` to `account_cost` (genesis config)

### Errors

**Execution Error**:

- If the action tries to create a top level account whose length is no greater than 32 characters, and `predecessor_id` is not
`registrar_account_id`, which is defined by the protocol, the following error will be returned

```rust
/// A top-level account ID can only be created by registrar.
CreateAccountOnlyByRegistrar {
    account_id: AccountId,
    registrar_account_id: AccountId,
    predecessor_id: AccountId,
}
```

- If the action tries to create an account that is neither a top-level account or a subaccount of `predecessor_id`,
the following error will be returned

```rust
/// A newly created account must be under a namespace of the creator account
CreateAccountNotAllowed { account_id: AccountId, predecessor_id: AccountId },
```

## DeployContractAction

```rust
pub struct DeployContractAction {
    pub code: Vec<u8>
}
```

**Outcome**:

- sets the contract code for account

### Errors

**Validation Error**:

- if the length of `code` exceeds `max_contract_size`, which is a genesis parameter, the following error will be returned:

```rust
/// The size of the contract code exceeded the limit in a DeployContract action.
ContractSizeExceeded { size: u64, limit: u64 },
```

**Execution Error**:

- If state or storage is corrupted, it may return `StorageError`.

## FunctionCallAction

```rust
pub struct FunctionCallAction {
    /// Name of exported Wasm function
    pub method_name: String,
    /// Serialized arguments
    pub args: Vec<u8>,
    /// Prepaid gas (gas_limit) for a function call
    pub gas: Gas,
    /// Amount of tokens to transfer to a receiver_id
    pub deposit: Balance,
}
```

Calls a method of a particular contract. See [details](./FunctionCall.md).

## TransferAction

```rust
pub struct TransferAction {
    /// Amount of tokens to transfer to a receiver_id
    pub deposit: Balance,
}
```

**Outcome**:

- transfers amount specified in `deposit` from `predecessor_id` to a `receiver_id` account

### Errors

**Execution Error**:

- If the deposit amount plus the existing amount on the receiver account exceeds `u128::MAX`,
a `StorageInconsistentState("Account balance integer overflow")` error will be returned.

## StakeAction

```rust
pub struct StakeAction {
    // Amount of tokens to stake
    pub stake: Balance,
    // This public key is a public key of the validator node
    pub public_key: PublicKey,
}
```

**Outcome**:

- A validator proposal that contains the staking public key and the staking amount is generated and will be included
in the next block.

### Errors

**Validation Error**:

- If the `public_key` is not an ristretto compatible ed25519 key, the following error will be returned:

```rust
/// An attempt to stake with a public key that is not convertible to ristretto.
UnsuitableStakingKey { public_key: PublicKey },
```

**Execution Error**:

- If an account has not staked but it tries to unstake, the following error will be returned:

```rust
/// Account is not yet staked, but tries to unstake
TriesToUnstake { account_id: AccountId },
```

- If an account tries to stake more than the amount of tokens it has, the following error will be returned:

```rust
/// The account doesn't have enough balance to increase the stake.
TriesToStake {
    account_id: AccountId,
    stake: Balance,
    locked: Balance,
    balance: Balance,
}
```

- If the staked amount is below the minimum stake threshold, the following error will be returned:

```rust
InsufficientStake {
    account_id: AccountId,
    stake: Balance,
    minimum_stake: Balance,
}
```

The minimum stake is determined by `last_epoch_seat_price / minimum_stake_divisor` where `last_epoch_seat_price` is the
seat price determined at the end of last epoch and `minimum_stake_divisor` is a genesis config parameter and its current
value is 10.

## AddKeyAction

```rust
pub struct AddKeyAction {
    pub public_key: PublicKey,
    pub access_key: AccessKey,
}
```

**Outcome**:

- Adds a new [AccessKey](/DataStructures/AccessKey.md) to the receiver's account and associates it with a `public_key` provided.

### Errors

**Validation Error**:

If the access key is of type `FunctionCallPermission`, the following errors can happen

- If `receiver_id` in `access_key` is not a valid account id, the following error will be returned 

```rust
/// Invalid account ID.
InvalidAccountId { account_id: AccountId },
```

- If the length of some method name exceed `max_length_method_name`, which is a genesis parameter (current value is 256),
the following error will be returned 

```rust
/// The length of some method name exceeded the limit in a Add Key action.
AddKeyMethodNameLengthExceeded { length: u64, limit: u64 },
```

- If the sum of length of method names (with 1 extra character for every method name) exceeds `max_number_bytes_method_names`, which is a genesis parameter (current value is 2000),
the following error will be returned

```rust
/// The total number of bytes of the method names exceeded the limit in a Add Key action.
AddKeyMethodNamesNumberOfBytesExceeded { total_number_of_bytes: u64, limit: u64 }
```

**Execution Error**:

- If an account tries to add an access key with a given public key, but an existing access key with this public key already exists, the following error will be returned

```rust
/// The public key is already used for an existing access key
AddKeyAlreadyExists { account_id: AccountId, public_key: PublicKey }
```

- If state or storage is corrupted, a `StorageError` will be returned.

## DeleteKeyAction

```rust
pub struct DeleteKeyAction {
    pub public_key: PublicKey,
}
```

**Outcome**:

- Deletes the [AccessKey](/DataStructures/AccessKey.md) associated with `public_key`.

### Errors

**Execution Error**:

- When an account tries to delete an access key that doesn't exist, the following error is returned

```rust
/// Account tries to remove an access key that doesn't exist
DeleteKeyDoesNotExist { account_id: AccountId, public_key: PublicKey }
```

- `StorageError` is returned if state or storage is corrupted.

## DeleteAccountAction

```rust
pub struct DeleteAccountAction {
    /// The remaining account balance will be transferred to the AccountId below
    pub beneficiary_id: AccountId,
}
```

**Outcomes**:

- The account, as well as all the data stored under the account, is deleted and the tokens are transferred to `beneficiary_id`.

### Errors

**Validation Error**:

- If `beneficiary_id` is not a valid account id, the following error will be returned

```rust
/// Invalid account ID.
InvalidAccountId { account_id: AccountId },
```

- If this action is not the last action in the action list of a receipt, the following error will be returned

```rust
/// The delete action must be a final action in transaction
DeleteActionMustBeFinal
```

- If the account still has locked balance due to staking, the following error will be returned

```rust
/// Account is staking and can not be deleted
DeleteAccountStaking { account_id: AccountId }
```

**Execution Error**:

- If state or storage is corrupted, a `StorageError` is returned.

## Delegate Actions

Introduced with [NEP-366](https://github.com/near/NEPs/blob/master/neps/nep-0366.md) to enable meta transactions.

In summary, a delegate action is an indirect submission of a transaction.
It allows a relayer to do the payment (gas and token costs) for a transaction authored by a user.

```rust
/// The struct contained in transactions and receipts, inside `Action::Delegate(_)``.
struct SignedDelegateAction {
    /// The actual action, see below.
    pub delegate_action: DelegateAction,
    /// NEP-483 proposal compliant signature
    pub signature: Signature,
}
```

Note that the signature follows a scheme which is proposed to be standardized in [NEP-483](https://github.com/near/NEPs/pull/483).


```rust
/// The struct a user creates and signs to create a meta transaction.
struct DelegateAction {
    /// Signer of the delegated actions
    pub sender_id: AccountId,
    /// Receiver of the delegated actions.
    pub receiver_id: AccountId,
    /// List of actions to be executed.
    ///
    /// With the meta transactions MVP defined in NEP-366, nested
    /// DelegateActions are not allowed. A separate type is used to enforce it.
    pub actions: Vec<NonDelegateAction>,
    /// Nonce to ensure that the same delegate action is not sent twice by a
    /// relayer and should match for given account's `public_key`.
    /// After this action is processed it will increment.
    pub nonce: Nonce,
    /// The maximal height of the block in the blockchain below which the given DelegateAction is valid.
    pub max_block_height: BlockHeight,
    /// Public key used to sign this delegated action.
    pub public_key: PublicKey,
}
```

### Outcomes

- All actions inside `delegate_action.actions` are submitted with the `delegate_action.sender_id` as the predecessor, `delegate_action.receiver_id` as the receiver, and the relayer (predecessor of `DelegateAction`) as the signer.
- All gas and balance costs for submitting `delegate_action.actions` are subtracted from the relayer.

### Errors

**Validation Error**:

- If the list of Transaction actions contains several `DelegateAction`

```rust
/// There should be the only one DelegateAction
DelegateActionMustBeOnlyOne
```

**Execution Error**:

- If the Sender's account doesn't exist

```rust
/// Happens when TX receiver_id doesn't exist
AccountDoesNotExist
```

- If the `signature` does not match the data and the `public_key` of the given key, then the following error will be returned

```rust
/// Signature does not match the provided actions and given signer public key.
DelegateActionInvalidSignature
```

- If the `sender_id` doesn't match the `tx.receiver_id`

```rust
/// Receiver of the transaction doesn't match Sender of the delegate action
DelegateActionSenderDoesNotMatchTxReceiver
```

- If the current block is equal or greater than `max_block_height`

```rust
/// Delegate action has expired
DelegateActionExpired
```

- If the `public_key` does not exist for Sender account

```rust
/// The given public key doesn't exist for Sender account
DelegateActionAccessKeyError
```

- If the `nonce` does match the `public_key` for the `sender_id`

```rust
/// Nonce must be greater sender[public_key].nonce
DelegateActionInvalidNonce
```

- If `nonce` is too large

```rust
/// DelegateAction nonce is larger than the upper bound given by the block height (block_height * 1e6)
DelegateActionNonceTooLarge
```
