# RuntimeFeesConfig

Economic parameters for runtime

## action_receipt_creation_config

`type: Fee`

Describes the cost of creating an action receipt, `ActionReceipt`, excluding the actual cost
of actions.

## data_receipt_creation_config

`type:` [DataReceiptCreationConfig](RuntimeFeeConfig/DataReceiptCreationConfig.md)

Describes the cost of creating a data receipt, `DataReceipt`.

## action_creation_config

`type:` [ActionCreationConfig](RuntimeFeeConfig/ActionCreationConfig.md)

Describes the cost of creating a certain action, `Action`. Includes all variants.

## storage_usage_config

`type:` [StorageUsageConfig](RuntimeFeeConfig/StorageUsageConfig.md)

Describes how the storage that an account uses is counted for storage staking.

## burnt_gas_reward

`type:` [Fraction](RuntimeFeeConfig/Fraction.md)

Fraction of the burnt gas to reward to the contract account for execution.
