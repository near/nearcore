# Access Keys

Access key provides an access for a particular account. Each access key belongs to some account and
is identified by a unique (within the account) public key. Access keys are stored as `account_id,public_key` in a trie state. Account can have from [zero](#account-without-access-keys) to multiple access keys.

```rust
pub struct AccessKey {
    /// The nonce for this access key.
    /// NOTE: In some cases the access key needs to be recreated. If the new access key reuses the
    /// same public key, the nonce of the new access key should be equal to the nonce of the old
    /// access key. It's required to avoid replaying old transactions again.
    pub nonce: Nonce,
    /// Defines permissions for this access key.
    pub permission: AccessKeyPermission,
}
```

There are 2 types of access keys in NEAR: **regular access keys** and **gas keys**. Regular access keys have `FullAccess` or `FunctionCall` permission. Gas keys have `GasKeyFullAccess` or `GasKeyFunctionCall` permission and carry a prepaid balance used to pay for gas costs.

`FullAccess` grants permissions to issue any action on the account. This includes [DeployContract](../RuntimeSpec/Actions.md#deploycontractaction), [Transfer](../RuntimeSpec/Actions.md#transferaction) tokens, call functions [FunctionCall](../RuntimeSpec/Actions.md#functioncallaction), [Stake](../RuntimeSpec/Actions.md#stakeaction) and even permission to delete the account [DeleteAccountAction](../RuntimeSpec/Actions.md#deleteaccountaction). `FunctionCall` on the other hand, **only** grants permission to call any or a specific set of methods on one given contract. It has an allowance of `$NEAR` that can be spent on **GAS and transaction fees only**. Function call access keys **cannot** be used to transfer `$NEAR`.

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

Gas keys are a special type of access key that carry a prepaid NEAR balance used exclusively to pay for gas costs. When a transaction is signed with a gas key, the gas fees are deducted from the gas key's balance rather than the signer's account balance. Deposits (e.g., for `Transfer` or `FunctionCall` with attached deposit) are still paid from the account balance.

Gas keys are created via [AddKey](../RuntimeSpec/Actions.md#addkeyaction) with a `GasKeyFullAccess` or `GasKeyFunctionCall` permission. They are funded via [TransferToGasKey](../RuntimeSpec/Actions.md#transfertogaskeyaction) and drained via [WithdrawFromGasKey](../RuntimeSpec/Actions.md#withdrawfromgaskeyaction).

### GasKeyInfo

```rust
pub struct GasKeyInfo {
    /// Prepaid balance available for paying gas costs.
    pub balance: Balance,
    /// Number of independent nonce slots for this gas key.
    pub num_nonces: NonceIndex,
}
```

- `balance` starts at zero when the key is created and is increased via `TransferToGasKey`.
- `num_nonces` determines how many independent nonce indices (0..num_nonces) the gas key supports, enabling parallel transaction submission. Maximum value is 1024.

### AccessKeyPermission::GasKeyFullAccess

Equivalent to `FullAccess` but with a gas key balance. Grants permission to issue any action on the account, with gas costs paid from the gas key balance.

### AccessKeyPermission::GasKeyFunctionCall

Equivalent to `FunctionCall` but with a gas key balance. Grants limited permission to make function calls, with gas costs paid from the gas key balance. The `FunctionCallPermission` embedded in this variant must have `allowance` set to `None`.

### Gas Key Nonces

Unlike regular access keys which have a single nonce, gas keys have multiple independent nonce slots (up to 1024). Each nonce slot is stored separately in the trie as a `GasKeyNonce` entry. This allows multiple clients sharing the same gas key to submit transactions in parallel without nonce conflicts by using different `nonce_index` values.

Transactions using a gas key must be `TransactionV1` and include a `GasKeyNonce` which specifies both the `nonce` value and the `nonce_index` to use. See [Transactions](../RuntimeSpec/Transactions.md) for details.

### Gas Key Deletion

When a gas key is deleted (via [DeleteKey](../RuntimeSpec/Actions.md#deletekeyaction)), its remaining balance is burned. To protect users from accidental loss, deletion fails if the gas key balance exceeds 1 NEAR. Users must first withdraw funds via [WithdrawFromGasKey](../RuntimeSpec/Actions.md#withdrawfromgaskeyaction) to reduce the balance below 1 NEAR before deleting.

### Gas Key Namespace

Gas keys and regular access keys share the same public key namespace on an account. A public key can only be associated with one access key (regular or gas) at a time. Adding a key with a public key that already exists will fail with `AddKeyAlreadyExists`.

## Account without access keys

If account has no access keys attached it means that it has no owner who can run transactions from its behalf. However, if such accounts has code it can be invoked by other accounts and contracts.
