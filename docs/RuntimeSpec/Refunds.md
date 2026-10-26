# Refunds

When execution of a receipt fails or there is some unused amount of prepaid gas left after a function call, the Runtime generates refund receipts.

The are 2 types of refunds:

- Refunds for the failed receipt for attached deposits. Let's call them **deposit refunds**.
- Refunds for the unused gas and fees. Let's call them **gas refunds**.

Refund receipts are identified by having `predecessor_id == "system"`. They are also special because they are free for the user. The runtime does not charge or burn any tokens to generate or execute them.
Their execution outcome still reports `gas_burnt`, which is the `execution` fee of an action receipt plus the `execution` fee of one `Transfer` action. This is about 0.22 Tgas when the receiver is a named account. The `Transfer` fee depends on the type of the receiver account, as described in [Runtime Fees](Fees/Fees.md#fees-for-actions). For a NEAR-implicit, ETH-implicit, deterministic or universal (from protocol version 88) receiver, the fee includes the account creation fees. This applies even when the account already exists. This gas counts toward the chunk gas limit like the gas of any other receipt.

If the execution of a refund fails, the refund amount is burnt.
The refund receipt is an `ActionReceipt` that consists of a single action `Transfer` with the `deposit` amount of the refund.

## Deposit Refunds

Deposit refunds are generated when an action receipt fails to execute. All attached deposit amounts are summed together and
sent as a refund to the refund receiver of the receipt. The refund receiver is the `predecessor_id` by default.
From protocol version 82 (nearcore 2.10.0), a contract can set a different refund receiver with the `promise_set_refund_to`
host function when it creates the receipt. A contract can read the refund receiver of the current receipt with the
`refund_to_account_id` host function.

Deposit refunds have the following fields in the `ActionReceipt`:

- `signer_id` is `system`
- `signer_public_key` is ED25519 key with data equal to 32 bytes of `0`.

Deposit refunds are free for the user and incur no refund fee.

## Gas Refunds

Gas refunds are generated when a receipt used the amount of gas lower than the attached amount of gas.
From protocol version 85 (nearcore 2.13.0), they are also generated when a receipt burns gas at a lower price than the price at which the gas was bought.

If the receipt execution succeeded, the gas amount is equal to `prepaid_gas + execution_gas - used_gas`.

If the receipt execution failed, the gas amount is equal to `prepaid_gas + execution_gas - burnt_gas`.

The difference between `burnt_gas` and `used_gas` is the `used_gas` also includes the fees and the prepaid gas of
newly generated receipts, e.g. from cross-contract calls in function calls actions.

From this unspent gas amount, the network charges a gas refund fee, starting with protocol version 78. The exact fee is calculated as `max(gas_refund_penalty * unspent_gas, min_gas_refund_penalty)`. As of version 78, `gas_refund_penalty` is 0% and `min_gas_refund_penalty` 0 Tgas, always resulting in a zero-cost fee. This is only a stepping stone to give projects time to adapt before the fee is taking effect. The plan is to increase this to 5% and 1 Tgas, as specified in [NEP-536](https://github.com/near/NEPs/blob/master/neps/nep-0536.md). There is no fixed timeline available for this.

The refund for the unspent gas is the unspent gas multiplied by the [`gas_price`](Receipts.md#gas_price) of the receipt, minus the gas refund fee converted to tokens. The `gas_price` of the receipt is the price at which the gas was bought. The refund is never less than 0. Before protocol version 85, the fee is converted at the same `gas_price`. So when the fee is equal to or larger than the unspent gas, nothing is refunded for the unspent gas.

From protocol version 85 (nearcore 2.13.0), the gas attached to a receipt is bought at the higher of the block gas price and `min_gas_purchase_price`, which is 10^9 yoctoNEAR per gas. The receipt burns gas at the lower of this purchase price and the gas price of the block where it is executed. This is the burn price. The gas refund then also includes the price difference for the burnt gas, which is `burnt_gas * (purchase_price - burn_price)`. The gas refund fee is converted to tokens at the burn price.

If the receipt succeeds and creates its receiver account, the runtime keeps an account creation charge from this price difference refund and burns it. The charge is `account_creation_charge` (0.007 NEAR) minus the `create_account` execution fee at the burn price. It is never less than 0 and never more than the price difference refund. See [Account creation charge](Fees/Fees.md#account-creation-charge).

Gas refunds have the following fields in the `ActionReceipt`:

- `signer_id` is the actual `signer_id` from the receipt that generates this refund.
- `signer_public_key` is the `signer_public_key` from the receipt that generates this refund.

## Access Key Allowance refunds

When an account used a restricted access key with `FunctionCallPermission`, it may have had a limited allowance.
The allowance was charged for the full amount of receipt fees including full prepaid gas.
To refund the allowance we distinguish between Deposit refunds and Gas refunds using `signer_id` in the action receipt.

If the `signer_id == receiver_id && predecessor_id == "system"` it means it's a gas refund and the runtime should try to refund the allowance.

Note, that it's not always possible to refund the allowance, because the access key can be deleted between the moment when the transaction was
issued and when the gas refund arrived. In this case we use the best effort to refund the allowance. It means:

- the access key on the `signer_id` account with the public key `signer_public_key` should exist
- the access key permission should be `FunctionCallPermission`
- the allowance should be set to `Some` limited value, instead of unlimited allowance (`None`)
- the runtime uses saturating add to increase the allowance, to avoid overflows
