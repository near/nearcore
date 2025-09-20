# Refunds

When execution of a receipt fails or there is some unused amount of prepaid gas left after a function call, the Runtime generates refund receipts.

The are 2 types of refunds:

- Refunds for the failed receipt for attached deposits. Let's call them **deposit refunds**.
- Refunds for the unused gas and fees. Let's call them **gas refunds**.

Refund receipts are identified by having `predecessor_id == "system"`. They are also special because they don't cost any gas to generate or execute. As a result, they also do not contribute to the block gas limit.

If the execution of a refund fails, the refund amount is burnt.
The refund receipt is an `ActionReceipt` that consists of a single action `Transfer` with the `deposit` amount of the refund.

## Deposit Refunds

Deposit refunds are generated when an action receipt fails to execute. All attached deposit amounts are summed together and
sent as a refund to a `predecessor_id` (because only the predecessor can attach deposits).

Deposit refunds have the following fields in the `ActionReceipt`:

- `signer_id` is `system`
- `signer_public_key` is ED25519 key with data equal to 32 bytes of `0`.

Deposit refunds are free for the user and incur no refund fee.

## Gas Refunds

Gas refunds are generated when a receipt used the amount of gas lower than the attached amount of gas.

If the receipt execution succeeded, the gas amount is equal to `prepaid_gas + execution_gas - used_gas`.

If the receipt execution failed, the gas amount is equal to `prepaid_gas + execution_gas - burnt_gas`.

The difference between `burnt_gas` and `used_gas` is the `used_gas` also includes the fees and the prepaid gas of
newly generated receipts, e.g. from cross-contract calls in function calls actions.

From this unspent gas amount, the network charges a gas refund fee, starting with protocol version 78. The exact fee is calculated as `max(gas_refund_penalty * unspent_gas, min_gas_refund_penalty)`. As of version 78, `gas_refund_penalty` is 0% and `min_gas_refund_penalty` 0 Tgas, always resulting in a zero-cost fee. This is only a stepping stone to give projects time to adapt before the fee is taking effect. The plan is to increase this to 5% and 1 Tgas, as specified in [NEP-536](https://github.com/near/NEPs/blob/master/neps/nep-0536.md). There is no fixed timeline available for this.

Should the gas refund fee be equal or larger than the unspent gas, no refund will be produced.

If there is gas to refund left, the gas amount is converted to tokens by multiplying by the gas price at which the original transaction was generated.

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
