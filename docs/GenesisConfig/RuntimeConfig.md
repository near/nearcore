# RuntimeConfig

The structure that holds the parameters of the runtime, mostly economics.
It is not part of genesis.
Each nearcore release contains the runtime config for every protocol version (`core/parameters/res/runtime_configs/`), and the node uses the config of the current protocol version.
The `EXPERIMENTAL_protocol_config` RPC method returns it as `runtime_config`.
This page describes only some of the fields.
The others are `account_creation_config`, `congestion_control_config`, `witness_config`, `min_gas_purchase_price` and `account_creation_charge`.

## storage_amount_per_byte

`type: Balance`

The balance, in yoctoNEAR, that an account must hold for each byte of storage it uses.
The value is 10^19 yoctoNEAR (1 NEAR per 100 kB).
The balance stays on the account.
A transaction or receipt that would leave the account balance (amount plus locked) below the required amount fails with `LackBalanceForState`.
Accounts that use 770 bytes of storage or less are exempt (zero balance accounts, NEP-448).

## transaction_costs

`type: RuntimeFeesConfig` [RuntimeFeesConfig](RuntimeFeeConfig.md)

Costs of different actions that need to be performed when sending and processing transaction and receipts.

## wasm_config

`type: VMConfig` [VMConfig](VMConfig.md)

Config of wasm operations.
