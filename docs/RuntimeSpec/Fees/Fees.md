# Runtime Fees

Runtime fees are measured in Gas. Gas price will be discussed separately.

When a transaction is converted into a receipt, the signer account is charged for the full cost of the transaction.
This cost consists of extra attached gas, attached deposits and the transaction fee.

The total transaction fee is the sum of the following:

- A fee for creation of the receipt
- A fee for every action

Every [Fee](/GenesisConfig/RuntimeFeeConfig/Fee.md) consists of 3 values measured in gas:

- `send_sir` and `send_not_sir` - the gas burned when the action is being created to be sent to a receiver.
  - `send_sir` is used when `current_account_id == receiver_id` (`current_account_id` is a `signer_id` for a signed transaction).
  - `send_not_sir` is used when `current_account_id != receiver_id`
- `execution` - the gas burned when the action is being executed on the receiver's account.

## Receipt creation cost

Fees are charged for 2 types of receipts:

- Action receipts [ActionReceipt](/RuntimeSpec/Receipts.md#actionreceipt)
- Data receipts [DataReceipt](/RuntimeSpec/Receipts.md#datareceipt)

A transaction is converted into an [ActionReceipt](/RuntimeSpec/Receipts.md#actionreceipt).
Data receipts are used for data dependencies and will be discussed separately.

The `Fee` for an action receipt creation is described in the config [`action_receipt_creation_config`](/GenesisConfig/RuntimeFeeConfig.md#action_receipt_creation_config).

Example: when a signed transaction is being converted into a receipt, the gas for `action_receipt_creation_config.send` is being burned immediately,
while the gas for `action_receipt_creation_config.execution` is only charged, but not burned. It'll be burned when
the newly created receipt is executed on the receiver's account.

## Fees for actions

Every [`Action`](/RuntimeSpec/Actions) has a corresponding Fee(s).
The fees of most actions are described in the config [`action_creation_config`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md).
The fees of the global contract, state init and gas key actions are not part of that config.
This page names those fees by their runtime parameter names, such as `action_gas_key_transfer`.
Similar to a receipt creation costs, the `send` gas is burned when an action is added to a receipt to be sent, and the `execution` gas is only charged, but not burned.

Fees are either a base fee or a fee per unit of some data within the action, such as a byte or a storage entry.

Here is the list of actions and their corresponding fees:

- [CreateAccount](/RuntimeSpec/Actions.md#createaccountaction) uses
  - the base fee [`create_account_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#create_account_cost)
- [DeployContract](/RuntimeSpec/Actions.md#deploycontractaction) uses the sum of the following fees:
  - the base fee [`deploy_contract_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#deploy_contract_cost)
  - the fee per byte of the contract code to be deployed with the fee [`deploy_contract_cost_per_byte`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#deploy_contract_cost_per_byte)
    To compute the number of bytes for a deploy contract action `deploy_contract_action` use `deploy_contract_action.code.len()`
- [FunctionCall](/RuntimeSpec/Actions.md#functioncallaction) uses the sum of the following fees:
  - the base fee [`function_call_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#function_call_cost)
  - the fee per byte of method name string and per byte of arguments with the fee [`function_call_cost_per_byte`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#function_call_cost_per_byte).
    To compute the number of bytes for a function call action `function_call_action` use `function_call_action.method_name.as_bytes().len() + function_call_action.args.len()`
- [Transfer](/RuntimeSpec/Actions.md#transferaction) uses one of the following fees:
  - if the `receiver_id` is a [NEAR-implicit account ID](/DataStructures/Account.md#near-implicit-account-id), then a sum of base fees is used:
    - the create account base fee [`create_account_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#create_account_cost)
    - the transfer base fee [`transfer_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#transfer_cost)
    - the add full access key base fee [`add_key_cost.full_access_cost`](/GenesisConfig/RuntimeFeeConfig/AccessKeyCreationConfig.md#full_access_cost)
  - if the `receiver_id` is an [ETH-implicit account ID](/DataStructures/Account.md#eth-implicit-account-id), a [deterministic account ID](/DataStructures/Account.md#deterministic-account-id), or from protocol version 88 (nearcore 2.14.0) a universal (`0u`) account ID, then a sum of base fees is used:
    - the create account base fee [`create_account_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#create_account_cost)
    - the transfer base fee [`transfer_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#transfer_cost)
  - for any other `receiver_id`, only the base fee is used:
    - the transfer base fee [`transfer_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#transfer_cost)
- [Stake](/RuntimeSpec/Actions.md#stakeaction) uses
  - the base fee [`stake_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#stake_cost)
- [AddKey](/RuntimeSpec/Actions.md#addkeyaction) uses one of the following fees:
  - if the access key is [`AccessKeyPermission::FullAccess`](/DataStructures/AccessKey) the base fee is used
    - the add full access key base fee [`add_key_cost.full_access_cost`](/GenesisConfig/RuntimeFeeConfig/AccessKeyCreationConfig.md#full_access_cost)
  - if the access key is [`AccessKeyPermission::FunctionCall`](/DataStructures/AccessKey.md#accesskeypermissionfunctioncall) the sum of the fees is used
    - the add function call permission access key base fee [`add_key_cost.function_call_cost`](/GenesisConfig/RuntimeFeeConfig/AccessKeyCreationConfig.md#function_call_cost)
    - the fee per byte of method names with extra byte for every method with the fee [`add_key_cost.function_call_cost_per_byte`](/GenesisConfig/RuntimeFeeConfig/AccessKeyCreationConfig.md#function_call_cost_per_byte)
        To compute the number of bytes for `function_call_permission` use `function_call_permission.method_names.iter().map(|name| name.as_bytes().len() as u64 + 1).sum::<u64>()`
  - if the access key is a gas key, with `AccessKeyPermission::GasKeyFullAccess` or `AccessKeyPermission::GasKeyFunctionCall`, the fees of the matching `FullAccess` or `FunctionCall` case above are used, plus the following fees:
    - the `send` part of the fee per byte `action_gas_key_byte`, for the bytes of the gas key information.
      To compute the number of bytes use `GasKeyInfo::borsh_len()`
    - the `execution` part of the base fee `action_gas_key_nonce_write_base`, once for each nonce of the gas key
    - the `execution` part of the fee per byte `action_gas_key_byte`, for the trie key and value of each nonce.
      To compute the number of bytes use `gas_key_info.num_nonces * (receiver_id.len() + add_key_action.public_key.trie_id_len() + 2 + size_of::<NonceIndex>() + size_of::<Nonce>())`.
      The `2` counts the column byte and the separator byte of the trie key.
- [DeleteKey](/RuntimeSpec/Actions.md#deletekeyaction) uses
  - the base fee [`delete_key_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#delete_key_cost)
- [DeleteAccount](/RuntimeSpec/Actions.md#deleteaccountaction) uses the base fee [`delete_account_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#delete_account_cost).
  The remaining balance goes to `beneficiary_id` in a balance refund receipt, which has no fee.
- [Delegate](/RuntimeSpec/Actions.md#delegate-actions) uses the sum of the following fees. `DelegateV2` uses the same fees:
  - the base fee `delegate_cost`
  - the `send` fees of the inner actions, which are burned when the transaction is converted into a receipt
  - the `send` and `execution` fees of the inner actions and the `execution` fee of an action receipt.
    These fees are prepaid for the receipt that the delegate action creates. The prepaid `send` fees are burned when that receipt is created.
- [DeployGlobalContract](/RuntimeSpec/Actions.md#deployglobalcontractaction) uses the sum of the following fees:
  - the base fee `action_deploy_global_contract`
  - the fee per byte of the contract code `action_deploy_global_contract_per_byte`.
    To compute the number of bytes use `deploy_global_contract_action.code.len()`

  The action also burns tokens for the code storage, see [DeployGlobalContractAction](/RuntimeSpec/Actions.md#deployglobalcontractaction).
- [UseGlobalContract](/RuntimeSpec/Actions.md#useglobalcontractaction) uses the sum of the following fees:
  - the base fee `action_use_global_contract`
  - the fee per byte of the contract identifier `action_use_global_contract_per_identifier_byte`.
    To compute the number of bytes use `use_global_contract_action.contract_identifier.len()`.
    It is 32 for a code hash and the length of the account ID for an account ID.
- [DeterministicStateInit](/RuntimeSpec/Actions.md#deterministicstateinitaction) uses the sum of the following fees:
  - the base fee `action_deterministic_state_init`
  - the fee per byte of the storage keys and values `action_deterministic_state_init_per_byte`.
    To compute the number of bytes use `state_init_action.state_init.len_bytes()`, which is the total length of all keys and values in `state_init_action.state_init.data()`
  - the fee per storage entry `action_deterministic_state_init_per_entry`.
    To compute the number of entries use `state_init_action.state_init.data().len()`
- `TransferToGasKey` and `WithdrawFromGasKey` use the sum of the following fees:
  - the base fee `action_gas_key_transfer`
  - the `send` part of the fee per byte `action_gas_key_byte`, for the bytes of the public key.
    To compute the number of bytes use `action.public_key.trie_id_len()`.
    This is the length of the key as stored in the trie: 33 bytes for ED25519 and ML-DSA-65 keys, and 65 bytes for SECP256K1 keys.
  - the `execution` part of the fee per byte `action_gas_key_byte`, for the trie key and value of the gas key.
    To compute the number of bytes use `receiver_id.len() + action.public_key.trie_id_len() + 2 + AccessKey::min_gas_key_borsh_len()`.
    `AccessKey::min_gas_key_borsh_len()` is the Borsh length of the smallest gas key record.
- From protocol version 88 (nearcore 2.14.0), `UniversalStateInit` uses the sum of the following fees:
  - the base fee `action_universal_state_init`
  - the fee per byte of the serialized state init `action_universal_state_init_per_byte`.
    To compute the number of bytes use `universal_state_init_action.state_init.0.len()`
  - the fee per storage entry `action_universal_state_init_per_entry`, for each storage entry in the decoded state init
  - the add full access key base fee [`add_key_cost.full_access_cost`](/GenesisConfig/RuntimeFeeConfig/AccessKeyCreationConfig.md#full_access_cost), for each access key in the decoded state init

## Account creation charge

From protocol version 85 (nearcore 2.13.0), the runtime charges extra when a receipt creates an account.
The aim is that creating an account costs `account_creation_charge`, which is 0.007 NEAR.
The charge applies when a receipt succeeds and its receiver account exists after the receipt but not before it.
This applies to `CreateAccount` and `DeterministicStateInit`. It also applies to a `Transfer` that is the only action of its receipt and goes to a new NEAR-implicit, ETH-implicit or deterministic account.
From protocol version 88 (nearcore 2.14.0), it also happens with a `Transfer` to a new universal (`0u`) account and with `UniversalStateInit`.

The receipt burns gas at `burn_price`, which is the lower of the receipt gas price and the gas price of the current block.
The refund of the receipt includes the price difference for all the gas that the receipt burned, see [Refunds](../Refunds.md).
The runtime takes the charge from this price difference refund and burns it:

```python
price_difference_refund = gas_burnt * (receipt_gas_price - burn_price)
create_account_execution_cost = config.action_creation_config.create_account_cost.execution * burn_price
charge = min(max(0, account_creation_charge - create_account_execution_cost), price_difference_refund)
```

At a burn price of 10^8 yoctoNEAR per gas, the `execution` fee of `create_account_cost` is 0.00072 NEAR, and the charge is 0.00628 NEAR.
Together they cost 0.007 NEAR.
If the burn price is so high that the `execution` fee alone reaches 0.007 NEAR, there is no extra charge.

## Gas tracking

In `Runtime`, gas is tracked in the following fields of `ActionResult` struct:

- `gas_burnt` - irreversible amount of gas spent on computations.
- `gas_used` - includes burnt gas and gas attached to the new `ActionReceipt`s created during the method execution.
- `gas_burnt_for_function_call` - stores gas burnt during function call execution. Up to protocol version 86, the contract account gets 30% of it as a reward for a possibility to invoke the function. From protocol version 87 (nearcore 2.14.0), the reward parameter `burnt_gas_reward` is 0, so the contract account gets no reward.

Initially runtime charges `gas_used` from the account. Some gas may be refunded later, see [Refunds](../Refunds.md).

At first, we charge fees related to conversion from `SignedTransaction` to `ActionReceipt` and future execution of this receipt:

- costs of all `SignedTransaction`s passed to `Runtime::apply` are computed in `tx_cost` function during validation;
- `total_cost` is deducted from signer, which is a sum of:
  - `gas_burnt * gas_price` where `gas_burnt` is action receipt send fee + `total_send_fees(transaction.actions)` + the signature verification cost, and `gas_price` is the gas price of the current block;
  - `gas_remaining * receipt_gas_price` where `gas_remaining` is `total_prepaid_gas(transaction.actions)` + `total_prepaid_send_fees(transaction.actions)` + action receipt exec fee + `total_prepaid_exec_fees(transaction.actions)` to pay the attached gas and all remaining fees caused by transaction;
  - `total_deposit(transaction.actions)`;
- each transaction is converted to receipt and passed to `Runtime::process_receipt`.

`total_prepaid_gas` is the gas attached to function calls, including the function calls inside delegate actions.
`total_prepaid_send_fees` is the sum of the `send` fees of the actions inside delegate actions.
The signature verification cost is charged for the key that signed the transaction and for the key of each delegate action.
It is `ml_dsa_65_verification_cost` (100 Ggas) for an ML-DSA-65 key and 0 for ED25519 and SECP256K1 keys.
ML-DSA-65 keys are supported from protocol version 85 (nearcore 2.13.0).

`receipt_gas_price` is `max(gas_price, min_gas_purchase_price)`, and it becomes the [`gas_price`](../Receipts.md#gas_price) of the new receipt.
From protocol version 85 (nearcore 2.13.0), `min_gas_purchase_price` is 10^9 yoctoNEAR per gas. Before that, it was 0.
The receipt later burns gas at the lower of `receipt_gas_price` and the gas price of the block where it is executed.
Unused gas and the price difference for burned gas are refunded, see [Refunds](../Refunds.md).

Then each `ActionReceipt` is passed to `Runtime::apply_action_receipt` where gas is tracked as follows:

- `ActionResult` is created with `ActionReceipt` execution fee;
- all actions inside `ActionReceipt` are passed to `Runtime::apply_action`;
- `ActionResult` with charged base execution fees is created there;
- if action execution creates new `ActionReceipt`s, their fees are added to the `ActionResult`. For example, when a function call creates a promise, it pays:
  - `gas_burnt`: the **send** fees for the new `ActionReceipt` and its actions
  - `gas_used`: `gas_burnt` + the **exec** fees for the new `ActionReceipt` and its actions + the gas attached to the function calls in the new `ActionReceipt`
- all computed `ActionResult`s are merged into one, where all gas values are summed up;
- unused gas is refunded in `refund_unspent_gas_and_deposits`, after subtracting the gas refund fee, see [Refunds](../Refunds.md).

Inside `VMLogic`, the fees are tracked in the `GasCounter` struct.
The VM itself is called in the `action_function_call` inside `Runtime`. When all actions are processed, the result is returned as a `VMOutcome`, which is later merged with `ActionResult`.
 
# Example

<!-- cspell:ignore VGYT Fwya Wckywk Ejuz -->
Let's say we have the following transaction:

```rust
Transaction {
    signer_id: "alice.near",
    public_key: "2onVGYTFwyaGetWckywk92ngBiZeNpBeEjuzSznEdhRE",
    nonce: 23,
    receiver_id: "lockup.alice.near",
    block_hash: "3CwEMonK6MmKgjKePiFYgydbAvxhhqCPHKuDMnUcGGTK",
    actions: [
        Action::CreateAccount(CreateAccountAction {}),
        Action::Transfer(TransferAction {
            deposit: 100000000000000000000000000,
        }),
        Action::DeployContract(DeployContractAction {
            code: vec![/*<...128000 bytes...>*/],
        }),
        Action::FunctionCall(FunctionCallAction {
            method_name: "new",
            args: b"{\"owner_id\": \"alice.near\"}".to_vec(),
            gas: 25000000000000,
            deposit: 0,
        }),
    ],
}
```

It has `signer_id != receiver_id` so it will use `send_not_sir` for send fees.

It contains 4 actions with 2 actions that requires to compute number of bytes.
We assume `code` in `DeployContractAction` contains `128000` bytes. And `FunctionCallAction` has
`method_name` with length of `3` and `args` length of `26`, so total of `29`.

First let's compute the amount that will be burned immediately for sending a receipt.

```python
burnt_gas = \
    config.action_receipt_creation_config.send_not_sir + \
    config.action_creation_config.create_account_cost.send_not_sir + \
    config.action_creation_config.transfer_cost.send_not_sir + \
    config.action_creation_config.deploy_contract_cost.send_not_sir + \
    128000 * config.action_creation_config.deploy_contract_cost_per_byte.send_not_sir + \
    config.action_creation_config.function_call_cost.send_not_sir + \
    29 * config.action_creation_config.function_call_cost_per_byte.send_not_sir
```

Now, by using `burnt_gas`, we can calculate the total transaction fee

```python
total_transaction_fee = burnt_gas + \
    config.action_receipt_creation_config.execution + \
    config.action_creation_config.create_account_cost.execution + \
    config.action_creation_config.transfer_cost.execution + \
    config.action_creation_config.deploy_contract_cost.execution + \
    128000 * config.action_creation_config.deploy_contract_cost_per_byte.execution + \
    config.action_creation_config.function_call_cost.execution + \
    29 * config.action_creation_config.function_call_cost_per_byte.execution
```

This `total_transaction_fee` is the amount of gas required to create a new receipt from the transaction.

NOTE: There are extra amounts required to prepay for deposit in `TransferAction` and gas in `FunctionCallAction`, but this is not part of the total transaction fee.
