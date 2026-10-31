# Access Keys

Access key provides an access for a particular account. Each access key belongs to some account and
is identified by a unique (within the account) public key. Access keys are stored as `account_id,public_key` in a trie state. Account can have from [zero](#account-without-access-keys) to multiple access keys.

```rust
pub struct AccessKey {
    /// The nonce for this access key.
    /// When a key is created, its nonce is initialized to `(block_height - 1) * 1_000_000`.
    /// Valid transaction nonces for a given block must be strictly less than `block_height * 1_000_000`.
    /// See `nearcore/runtime/runtime/src/access_keys.rs::initial_nonce_value()` and the constant
    /// `core/primitives-core/src/account.rs::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER`.
    pub nonce: Nonce,
    /// Defines permissions for this access key.
    pub permission: AccessKeyPermission,
}
```

There are 4 types of `AccessKeyPermission`: `FullAccess`, `FunctionCall`, `GasKeyFullAccess` and `GasKeyFunctionCall`. `FullAccess` grants permissions to issue any action on the account. This includes [DeployContract](../RuntimeSpec/Actions.md#deploycontractaction), [Transfer](../RuntimeSpec/Actions.md#transferaction) tokens, call functions [FunctionCall](../RuntimeSpec/Actions.md#functioncallaction), [Stake](../RuntimeSpec/Actions.md#stakeaction) and even permission to delete the account [DeleteAccountAction](../RuntimeSpec/Actions.md#deleteaccountaction). `FunctionCall` on the other hand, **only** grants permission to call any or a specific set of methods on one given contract. It has an allowance of `$NEAR` that can be spent on **GAS and transaction fees only**. Function call access keys **cannot** be used to transfer `$NEAR`.

`GasKeyFullAccess` and `GasKeyFunctionCall` are [gas keys](#gas-keys). They exist from protocol version 85 (nearcore 2.13.0). They grant the same permissions as `FullAccess` and `FunctionCall`. They also hold a `GasKeyInfo` with a prepaid balance that pays for gas and a number of independent nonces.

```rust
pub enum AccessKeyPermission {
    FunctionCall(FunctionCallPermission),
    FullAccess,
    GasKeyFunctionCall(GasKeyInfo, FunctionCallPermission),
    GasKeyFullAccess(GasKeyInfo),
}
```

## AccessKeyPermission::FunctionCall

Grants limited permission to make [FunctionCall](../RuntimeSpec/Actions.md#functioncallaction) to a specified `receiver_id` and methods of a particular contract with a limit of allowed balance to spend.

```rust
pub struct FunctionCallPermission {
    /// Allowance is a balance limit to use by this access key to pay for function call gas and
    /// transaction fees. When this access key is used, both account balance and the allowance is
    /// decreased by the same value.
    /// `None` means unlimited allowance.
    /// NOTE: To change or increase the allowance, the old access key needs to be deleted and a new
    /// access key should be created.
    pub allowance: Option<Balance>,

    /// The access key only allows transactions with the given receiver's account id.
    pub receiver_id: AccountId,

    /// A list of method names that can be used. The access key only allows transactions with the
    /// function call of one of the given method names.
    /// Empty list means any method name can be used.
    pub method_names: Vec<String>,
}
```

## Gas Keys

A gas key is an access key with the permission `GasKeyFullAccess` or `GasKeyFunctionCall`. It holds a prepaid balance. When a transaction is signed with a gas key, the gas is paid from this balance instead of the account balance. Deposits, such as the `deposit` of a `Transfer` action, are still paid from the account balance. See [Transactions](../RuntimeSpec/Transactions.md#gas-key-transactions) for the checks and [Fees](../RuntimeSpec/Fees/Fees.md#gas-key-transactions) for the costs.

A gas key is created by an [AddKey](../RuntimeSpec/Actions.md#addkeyaction) action with a gas key permission. Its balance starts at 0. [TransferToGasKey](../RuntimeSpec/Actions.md#transfertogaskeyaction) adds to the balance, and any account can send it. [WithdrawFromGasKey](../RuntimeSpec/Actions.md#withdrawfromgaskeyaction) moves balance back to the account. Gas refunds of transactions signed with the gas key also go to its balance. See [Refunds](../RuntimeSpec/Refunds.md#gas-key-balance-refunds).

```rust
pub struct GasKeyInfo {
    pub balance: Balance,
    pub num_nonces: NonceIndex,
}
```

- `balance` is the prepaid balance of the gas key. It must be 0 in the `AddKey` action that creates the key.
- `num_nonces` is the number of independent nonces of the gas key. It must be from 1 to 1024 (`MAX_NONCES_FOR_GAS_KEY`). `NonceIndex` is a `u16`.

### AccessKeyPermission::GasKeyFullAccess

Grants the same permissions as `FullAccess`. The gas of transactions signed with this key is paid from the gas key balance.

### AccessKeyPermission::GasKeyFunctionCall

Grants the same permissions as `FunctionCall`. The gas of transactions signed with this key is paid from the gas key balance. The `allowance` in its `FunctionCallPermission` must be `None`.

### Gas Key Nonces

A regular access key has one nonce, in the `nonce` field of `AccessKey`. A gas key has `num_nonces` independent nonces instead, with the indices 0 to `num_nonces - 1`. The `nonce` field of a gas key is always 0 and is not used. Clients that share a gas key can use different nonce indices to send transactions in parallel without nonce conflicts. A transaction signed with a gas key must be a `TransactionV1` with a `TransactionNonce::GasKeyNonce`, which gives both the nonce and the `nonce_index`. See [Transactions](../RuntimeSpec/Transactions.md).

Each nonce is a separate record in the state. Its trie key is the trie key of the access key followed by the `nonce_index` as 2 little-endian bytes. Its value is the nonce as a Borsh-serialized `u64`. When the gas key is created, every nonce is set to `(block_height - 1) * 1_000_000`, where `block_height` is the height of the block in which the `AddKey` action is executed.

Each nonce adds to the storage usage of the account. An ED25519 or ML-DSA-65 key uses 83 bytes per nonce. This is 33 bytes for the key as stored in the trie, 2 bytes for the index, 8 bytes for the value and 40 bytes of overhead per record (`storage_num_extra_bytes_record`). For a SECP256K1 key, it is 115 bytes per nonce. The account needs storage staking for these bytes, as for any other state. With the current storage price of 10^19 yoctoNEAR per byte, 1024 nonces of an ED25519 key need about 0.85 NEAR.

### Gas Key Deletion

[DeleteKey](../RuntimeSpec/Actions.md#deletekeyaction) on a gas key burns the remaining balance of the key and removes all its nonces. If the balance is more than 1 NEAR, the action fails with `GasKeyBalanceTooHigh`. To delete such a key, first move the balance to the account with [WithdrawFromGasKey](../RuntimeSpec/Actions.md#withdrawfromgaskeyaction). [DeleteAccount](../RuntimeSpec/Actions.md#deleteaccountaction) burns the balances of all gas keys of the account. It fails with `GasKeyBalanceTooHigh` if the sum of these balances is more than 1 NEAR.

### Gas Key Namespace

Gas keys and regular access keys share the public keys of an account. A public key belongs to at most one access key at a time, regular or gas key. An `AddKey` action with a public key that is already used fails with `AddKeyAlreadyExists`.

### Gas Keys and Meta Transactions

A [`Delegate`](../RuntimeSpec/Actions.md#delegate-actions) action signed by a gas key fails with `DelegateActionRequiresNonGasKey`. A [`DelegateV2`](../RuntimeSpec/Actions.md#delegatev2) action can be signed by a gas key. From protocol version 87 (nearcore 2.14.0), new `DelegateV2` actions are rejected. So a gas key cannot sign the delegate action of a new meta transaction. A relayer can still sign the outer transaction with a gas key.

## Account without access keys

If account has no access keys attached it means that it has no owner who can run transactions from its behalf. However, if such accounts has code it can be invoked by other accounts and contracts.
