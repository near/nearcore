# action_creation_config

Describes the cost of creating a specific action, `Action`. Includes all variants.

## create_account_cost

`type: Fee`

Base cost of creating an account.

## deploy_contract_cost

`type: Fee`

Base cost of deploying a contract.

## deploy_contract_cost_per_byte

`type: Fee`

Cost per byte of deploying a contract.

## function_call_cost

`type: Fee`

Base cost of calling a function.

## function_call_cost_per_byte

`type: Fee`

Cost per byte of method name and arguments of calling a function.

## transfer_cost

`type: Fee`

Base cost of making a transfer.

NOTE: If the account ID is an implicit account ID (64-length hex account ID), then the cost of the transfer fee
will be `transfer_cost + create_account_cost + add_key_cost.full_access_cost`.
This is needed to account for the implicit account creation costs.

## stake_cost

`type: Fee`

Base cost of staking.

## add_key_cost

_type: [AccessKeyCreationConfig](AccessKeyCreationConfig.md)_
Base cost of adding a key.

## delete_key_cost

`type: Fee`

Base cost of deleting a key.

## delete_account_cost

`type: Fee`

Base cost of deleting an account.
