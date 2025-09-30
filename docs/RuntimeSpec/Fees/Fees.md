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

There are 2 types of receipts:

- Action receipts [ActionReceipt](/RuntimeSpec/Receipts.md#actionreceipt)
- Data receipts [DataReceipt](/RuntimeSpec/Receipts.md#datareceipt)

A transaction is converted into an [ActionReceipt](/RuntimeSpec/Receipts.md#actionreceipt).
Data receipts are used for data dependencies and will be discussed separately.

The `Fee` for an action receipt creation is described in the config [`action_receipt_creation_config`](/GenesisConfig/RuntimeFeeConfig.md#action_receipt_creation_config).

Example: when a signed transaction is being converted into a receipt, the gas for `action_receipt_creation_config.send` is being burned immediately,
while the gas for `action_receipt_creation_config.execution` is only charged, but not burned. It'll be burned when
the newly created receipt is executed on the receiver's account.

## Fees for actions

Every [`Action`](/RuntimeSpec/Actions) has a corresponding Fee(s) described in the config [`action_creation_config`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md).
Similar to a receipt creation costs, the `send` gas is burned when an action is added to a receipt to be sent, and the `execution` gas is only charged, but not burned.

Fees are either a base fee or a fee per byte of some data within the action.

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
  - if the `receiver_id` is an [Implicit Account ID](/DataStructures/Account#near-implicit-account-id), then a sum of base fees is used:
    - the create account base fee [`create_account_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#create_account_cost)
    - the transfer base fee [`transfer_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#transfer_cost)
    - the add full access key base fee [`add_key_cost.full_access_cost`](/GenesisConfig/RuntimeFeeConfig/AccessKeyCreationConfig.md#full_access_cost)
  - if the `receiver_id` is NOT an [Implicit Account ID](/DataStructures/Account.md#near-implicit-account-id), then only the base fee is used:
    - the transfer base fee [`transfer_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#transfer_cost)
- [Stake](/RuntimeSpec/Actions.md#stakeaction) uses
  - the base fee [`stake_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#stake_cost)
- [AddKey](/RuntimeSpec/Actions.md#addkeyaction) uses one of the following fees:
  - if the access key is [`AccessKeyPermission::FullAccess`](/DataStructures/AccessKey) the base fee is used
    - the add full access key base fee [`add_key_cost.full_access_cost`](/GenesisConfig/RuntimeFeeConfig/AccessKeyCreationConfig.md#full_access_cost)
  - if the access key is [`AccessKeyPermission::FunctionCall`](/DataStructures/AccessKey.md#accesskeypermissionfunctioncall) the sum of the fees is used
    - the add function call permission access key base fee [`add_key_cost.function_call_cost`](/GenesisConfig/RuntimeFeeConfig/AccessKeyCreationConfig.md#full_access_cost)
    - the fee per byte of method names with extra byte for every method with the fee [`add_key_cost.function_call_cost_per_byte`](/GenesisConfig/RuntimeFeeConfig/AccessKeyCreationConfig.md#function_call_cost_per_byte)
        To compute the number of bytes for `function_call_permission` use `function_call_permission.method_names.iter().map(|name| name.as_bytes().len() as u64 + 1).sum::<u64>()`
- [DeleteKey](/RuntimeSpec/Actions.md#deletekeyaction) uses
  - the base fee [`delete_key_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#delete_key_cost)
- [DeleteAccount](/RuntimeSpec/Actions.md#deleteaccountaction) uses
  - the base fee [`delete_account_cost`](/GenesisConfig/RuntimeFeeConfig/ActionCreationConfig.md#delete_account_cost)
  - action receipt creation fee for creating Transfer to send remaining funds to `beneficiary_id`
  - full transfer fee described in the corresponding item
    
## Gas tracking

In `Runtime`, gas is tracked in the following fields of `ActionResult` struct:

- `gas_burnt` - irreversible amount of gas spent on computations.
- `gas_used` - includes burnt gas and gas attached to the new `ActionReceipt`s created during the method execution.
- `gas_burnt_for_function_call` - stores gas burnt during function call execution. Later, contract account gets 30% of it as a reward for a possibility to invoke the function.

Initially runtime charges `gas_used` from the account. Some gas may be refunded later, see [Refunds](../Refunds.md).

At first, we charge fees related to conversion from `SignedTransaction` to `ActionReceipt` and future execution of this receipt:

- costs of all `SignedTransaction`s passed to `Runtime::apply` are computed in `tx_cost` function during validation;
- `total_cost` is deducted from signer, which is a sum of:
  - `gas_to_balance(gas_burnt)` where `gas_burnt` is action receipt send fee + `total_send_fees(transaction.actions)`);
  - `gas_to_balance(gas_remaining)` where `gas_remaining` is action receipt exec fee + `total_prepaid_exec_fees(transaction.actions)` to pay all remaining fees caused by transaction;
  - `total_deposit(transaction.actions)`;
- each transaction is converted to receipt and passed to `Runtime::process_receipt`.

Then each `ActionReceipt` is passed to `Runtime::apply_action_receipt` where gas is tracked as follows:

- `ActionResult` is created with `ActionReceipt` execution fee;
- all actions inside `ActionReceipt` are passed to `Runtime::apply_action`;
- `ActionResult` with charged base execution fees is created there;
- if action execution leads to new `ActionReceipt`s creation, corresponding `action_[action_name]` function adds new fees to the `ActionResult`. E.g. `action_delete_account` also charges the following fees:
  - `gas_burnt`: **send** fee for **new** `ActionReceipt` creation + complex **send** fee for `Transfer` to beneficiary account
  - `gas_used`: `gas_burnt` + **exec** fee for created `ActionReceipt` + complex **exec** fee for `Transfer`
- all computed `ActionResult`s are merged into one, where all gas values are summed up;
- unused gas is refunded in `generate_refund_receipts`, after subtracting the gas refund fee, see [Refunds](../Refunds.md).

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
