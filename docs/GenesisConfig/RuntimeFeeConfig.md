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
It is `0` since protocol version 87.

## pessimistic_gas_price_inflation_ratio

`type:` [Fraction](RuntimeFeeConfig/Fraction.md)

Not used by the runtime. It is `1/1`.

## ml_dsa_65_verification_cost

`type: Gas`

The extra gas to verify an ML-DSA-65 signature, above the cost of an ED25519 or SECP256K1 signature. The signer pays it as burnt gas when the transaction is converted to a receipt, once for the transaction signature and once for each `Delegate` action signed with ML-DSA-65. The value is `100_000_000_000`.
