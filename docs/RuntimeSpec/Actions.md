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
    DeployGlobalContract(DeployGlobalContractAction),
    UseGlobalContract(UseGlobalContractAction),
    DeterministicStateInit(DeterministicStateInitAction),
    TransferToGasKey(TransferToGasKeyAction),
    WithdrawFromGasKey(WithdrawFromGasKeyAction),
    DelegateV2(VersionedSignedDelegateAction),
    UniversalStateInit(UniversalStateInitAction),
}
```

`TransferToGasKey`, `WithdrawFromGasKey` and `DelegateV2` are accepted from protocol version 85 (nearcore 2.13.0). From protocol version 87 (nearcore 2.14.0), new `DelegateV2` actions are rejected with `RemovedProtocolFeature`. `UniversalStateInit` is accepted from protocol version 88 (nearcore 2.14.0). Before the protocol version that enables an action, the action fails validation with `UnsupportedProtocolFeature`.

Each transaction consists a list of actions to be performed on the `receiver_id` side. Since transactions are first
converted to receipts when they are processed, we will mostly concern ourselves with actions in the context of receipt
processing.
 
For the following actions, `predecessor_id` and `receiver_id` are required to be equal:

- `DeployContract`
- `Stake`
- `AddKey`
- `DeleteKey`
- `DeleteAccount`
- `DeployGlobalContract`
- `UseGlobalContract`
- `WithdrawFromGasKey`

If they are not equal, the action fails with `ActorNoPermission`.

NOTE: if the first action in the action list is `CreateAccount`, `predecessor_id` becomes `receiver_id`
for the rest of the actions until `DeleteAccount`. This gives permission by another account to act on the newly created account.

## CreateAccountAction

```rust
pub struct CreateAccountAction {}
```

If `receiver_id` is an implicit account ID, the action fails with `OnlyImplicitAccountCreationAllowed`. Implicit
account IDs are NEAR-implicit IDs (64 lowercase hex characters), `0x` ETH-implicit IDs, `0s` deterministic IDs and,
from protocol version 88 (nearcore 2.14.0), `0u` universal IDs. These accounts are created in other ways:

- A NEAR-implicit, ETH-implicit or deterministic account is created by a `Transfer` to its ID, when the `Transfer` is
  the only action of a receipt that is not a refund.
- A deterministic account can also be created by `DeterministicStateInit`.
- From protocol version 88, a universal account is created by `UniversalStateInit`, or by a `Transfer` that is not a
  refund. That `Transfer` may be batched with other actions.

**Outcome**:

- creates an account with `id` = `receiver_id`
- sets Account `storage_usage` to `account_cost` (genesis config)

### Errors

**Execution Error**:

- If `predecessor_id` is not `registrar_account_id` (currently `registrar`), and the action tries to create a top level
account whose ID is shorter than `min_allowed_top_level_account_length` (currently 65), the following error will be
returned. Account IDs are at most 64 characters long, so only the registrar can create named top level accounts with
`CreateAccount`.

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

- If `receiver_id` is an implicit account ID, the following error will be returned

```rust
OnlyImplicitAccountCreationAllowed { account_id: AccountId },
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
- If the permission is `GasKeyFullAccess` or `GasKeyFunctionCall`, the new key is a [gas key](/DataStructures/AccessKey.md#gas-keys) with a balance of 0.
  The `nonce` field of the key is set to 0. The runtime also creates `num_nonces` nonce records for the key, each set to `(block_height - 1) * 1_000_000`.
  These records add to the storage usage of the account, see [Gas Key Nonces](/DataStructures/AccessKey.md#gas-key-nonces).

### Errors

**Validation Error**:

If the access key permission is `FunctionCall` or `GasKeyFunctionCall`, the following errors can happen

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

If the access key permission is `GasKeyFullAccess` or `GasKeyFunctionCall`, the following errors can also happen

- If the protocol version is lower than 85, the following error is returned

```rust
UnsupportedProtocolFeature { protocol_feature: String, version: ProtocolVersion },
```

- If the permission is `GasKeyFunctionCall` and its `allowance` is not `None`, the following error is returned

```rust
/// Gas keys with FunctionCall permission cannot have an allowance set.
GasKeyFunctionCallAllowanceNotAllowed,
```

- If `num_nonces` is 0 or greater than 1024 (`MAX_NONCES_FOR_GAS_KEY`), the following error is returned

```rust
GasKeyInvalidNumNonces { requested_nonces: NonceIndex, limit: NonceIndex },
```

- If `balance` in `GasKeyInfo` is not 0, the following error is returned

```rust
AddGasKeyWithNonZeroBalance { balance: Balance },
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
- If the key is a [gas key](/DataStructures/AccessKey.md#gas-keys), its balance is burnt and all its nonce records are removed.

### Errors

**Execution Error**:

- When an account tries to delete an access key that doesn't exist, the following error is returned

```rust
/// Account tries to remove an access key that doesn't exist
DeleteKeyDoesNotExist { account_id: AccountId, public_key: PublicKey }
```

- If the key is a gas key with a balance of more than 1 NEAR, the following error is returned, with `public_key` set to the key.
  Use [WithdrawFromGasKey](#withdrawfromgaskeyaction) to lower the balance first.

```rust
/// Gas key balance is too high to burn during deletion
GasKeyBalanceTooHigh { account_id: AccountId, public_key: Option<PublicKey>, balance: Balance }
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
- The balances of all [gas keys](/DataStructures/AccessKey.md#gas-keys) of the account are burnt.

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

- If the storage usage of the account is more than 10,000 bytes, the following error is returned and nothing is deleted.
  The storage of the contract code or of the global contract reference does not count.
  All other storage counts, including access keys, gas key nonces and contract data.
  To delete the account, first remove contract data or delete keys until the storage usage is 10,000 bytes or less.

```rust
/// Delete account whose state is large is temporarily banned.
DeleteAccountWithLargeState { account_id: AccountId },
```

- If the sum of the balances of all gas keys of the account is more than 1 NEAR, the following error is returned, with `public_key: None`.
  Use [WithdrawFromGasKey](#withdrawfromgaskeyaction) to lower the balances first.

```rust
/// Gas key balance is too high to burn during deletion
GasKeyBalanceTooHigh { account_id: AccountId, public_key: Option<PublicKey>, balance: Balance }
```

- If state or storage is corrupted, a `StorageError` is returned.

## TransferToGasKeyAction

```rust
pub struct TransferToGasKeyAction {
    /// The public key of the gas key to fund
    pub public_key: PublicKey,
    /// Amount of NEAR to transfer to the gas key
    pub deposit: Balance,
}
```

Adds `deposit` to the balance of the [gas key](/DataStructures/AccessKey.md#gas-keys) with the given `public_key` on the receiver account.
Any account can send this action, so an account can fund a gas key of another account.
The `deposit` is paid by the sender in the same way as the `deposit` of a `Transfer`.
A contract can create this action with the `promise_batch_action_transfer_to_gas_key` host function.

**Outcome**:

- The balance of the gas key is increased by `deposit`.

### Errors

**Validation Error**:

- If the protocol version is lower than 85, `UnsupportedProtocolFeature` is returned.

**Execution Error**:

- If the receiver account has no access key with `public_key`, or the key is not a gas key, the following error is returned

```rust
/// Gas key does not exist for the specified public key
GasKeyDoesNotExist { account_id: AccountId, public_key: PublicKey }
```

## WithdrawFromGasKeyAction

```rust
pub struct WithdrawFromGasKeyAction {
    /// The public key of the gas key to withdraw from
    pub public_key: PublicKey,
    /// Amount of NEAR to transfer from the gas key
    pub amount: Balance,
}
```

Moves `amount` from the balance of the [gas key](/DataStructures/AccessKey.md#gas-keys) with the given `public_key` to the balance of the receiver account.
The `predecessor_id` and `receiver_id` must be equal.
No host function creates this action, so it can only come from a transaction or from the actions of a delegate action.
From protocol version 87 (nearcore 2.14.0), it cannot be inside a delegate action.

**Outcome**:

- The balance of the gas key is decreased by `amount`.
- The balance of the account is increased by `amount`.

### Errors

**Validation Error**:

- If the protocol version is lower than 85, `UnsupportedProtocolFeature` is returned.
- From protocol version 87 (nearcore 2.14.0), if a delegate action contains this action, the following error is returned. A receipt created before protocol version 87 still executes.

```rust
/// A `WithdrawFromGasKey` action must not be nested inside a delegate action.
WithdrawFromGasKeyNotAllowedInDelegate
```

**Execution Error**:

- If the account has no access key with `public_key`, or the key is not a gas key, the following error is returned

```rust
/// Gas key does not exist for the specified public key
GasKeyDoesNotExist { account_id: AccountId, public_key: PublicKey }
```

- If the balance of the gas key is less than `amount`, the following error is returned

```rust
/// Gas key does not have sufficient balance for the requested withdrawal
InsufficientGasKeyBalance { account_id: AccountId, public_key: PublicKey, balance: Balance, required: Balance }
```

## DelegateV2

`DelegateV2` is a version of the [delegate action](#delegate-actions) that a [gas key](/DataStructures/AccessKey.md#gas-keys) can sign.
It is accepted at protocol versions 85 and 86.

```rust
pub struct VersionedSignedDelegateAction {
    pub delegate_action: VersionedDelegateActionPayload,
    pub signature: Signature,
}

pub enum VersionedDelegateActionPayload {
    V2(DelegateActionV2),
}

pub struct DelegateActionV2 {
    /// Signer of the delegated actions
    pub sender_id: AccountId,
    /// Receiver of the delegated actions.
    pub receiver_id: AccountId,
    /// List of actions to be executed.
    pub actions: Vec<NonDelegateAction>,
    /// Nonce of the signing key, advanced when this action is processed. For
    /// a gas key it also selects which of the parallel nonces to advance.
    pub nonce: TransactionNonce,
    /// The maximal height of the block in the blockchain below which the given DelegateActionV2 is valid.
    pub max_block_height: BlockHeight,
    /// Public key used to sign this delegated action.
    pub public_key: PublicKey,
}
```

`DelegateActionV2` has the same fields as `DelegateAction`, except that `nonce` is a [`TransactionNonce`](/RuntimeSpec/Transactions.md).
The signature is over the SHA-256 hash of the Borsh encoding of the 4-byte prefix `(1 << 30) + 611` (NEP-611), followed by the `VersionedDelegateActionPayload`.
The payload starts with the variant byte `0` for `V2`.
A `Delegate` action uses the prefix `(1 << 30) + 366` (NEP-366) instead.
A `TransactionNonce::Nonce` must be signed by a regular access key, and it is checked against the nonce of that key.
A `TransactionNonce::GasKeyNonce` must be signed by a gas key, and it is checked against the nonce stored for its `nonce_index`.
The relayer pays for the gas, as with `Delegate`. The balance of the gas key is not used.

From protocol version 87 (nearcore 2.14.0), a new transaction or receipt with a `DelegateV2` action fails validation with the following error. A receipt created before protocol version 87 still executes.

```rust
RemovedProtocolFeature { protocol_feature: String, version: ProtocolVersion },
```

Besides the errors listed in [Delegate Actions](#delegate-actions), a `DelegateV2` action can fail with these execution errors:

- `DelegateActionAccessKeyError(DelegateActionRequiresGasKey)` if the nonce is a `GasKeyNonce` and the key is not a gas key.
- `DelegateActionAccessKeyError(DelegateActionRequiresNonGasKey)` if the nonce is a plain `Nonce` and the key is a gas key.
- `DelegateActionInvalidNonceIndex { nonce_index: NonceIndex, num_nonces: NonceIndex }` if `nonce_index` is not less than the `num_nonces` of the gas key.

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

- If the action is a `Delegate` and its `public_key` belongs to a [gas key](/DataStructures/AccessKey.md#gas-keys), the following error is returned. A `Delegate` action must be signed with a regular access key.

```rust
/// A plain access key nonce does not select one of the gas key's nonces.
DelegateActionAccessKeyError(DelegateActionRequiresNonGasKey)
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

## DeployGlobalContractAction

```rust
pub struct DeployGlobalContractAction {
    /// WebAssembly binary
    pub code: Vec<u8>,
    /// How this global contract should be referenced
    pub deploy_mode: GlobalContractDeployMode,
}

pub enum GlobalContractDeployMode {
    /// Contract is deployed under its code hash.
    /// Users will be able reference it by that hash.
    /// This effectively makes the contract immutable.
    CodeHash,
    /// Contract is deployed under the owner account id.
    /// Users will be able reference it by that account id.
    /// This allows the owner to update the contract for all its users.
    AccountId,
}
```

**Outcome**:

- First, the provided code is made available as global contract on the current shard.
- The same code propagates globally, shard by shard.
- Eventually, all accounts on all shards can reference the submitted code by the corresponding global contract identifier.

### Errors

**Validation Error**:

- `ContractSizeExceeded` if the provided WebAssembly code is larger than `max_contract_size` (4MiB).

**Execution Error**:

- `LackBalanceForState` if the account does not hold enough NEAR to cover the added storage.

## UseGlobalContractAction

```rust
pub struct UseGlobalContractAction {
    /// References a deployed global contract to use in the receiver account.
    pub contract_identifier: GlobalContractIdentifier,
}
```
**Outcome**:

### Errors

**Validation Error**:

- `InvalidAccountId` if the provided account id does not follow the [AccountId specification](../DataStructures/Account.md).

**Execution Error**:

- `GlobalContractDoesNotExist` if the referenced global contract does not exist on the shard of the receiver. (It may
  take a while for it to propagate to all shards.)

## DeterministicStateInitAction

```rust
pub struct DeterministicStateInitAction {
    /// The data required to initialize the account.
    pub state_init: DeterministicAccountStateInit,
    /// A NEAR balance to cover storage requirements. Extra balance is refunded.
    pub deposit: Balance,
}
```

**Outcome**:

- if the account was already created before:
    - do nothing
- if the account was not created before:
    - creates an account with deterministic account id
    - sets the contract code to the specified global contract
    - stores the initial data into the contract storage

### Errors

**Validation Error**:

- `InvalidDeterministicStateInitReceiver` if the receiver id is not derived from the provided `DeterministicAccountStateInit`
- `DeterministicStateInitKeyLengthExceeded` if any data key is longer than `max_length_storage_key` (4MiB)
- `DeterministicStateInitValueLengthExceeded` if any data value is longer than `max_length_storage_value` (4MiB)

**Execution Error**:

- `GlobalContractDoesNotExist` if the referenced global contract does not exist on the shard of the receiver. (It may
  take a while for it to propagate to all shards.)