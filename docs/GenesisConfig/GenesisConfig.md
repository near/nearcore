# GenesisConfig

The fields of `GenesisConfig` are the top-level keys of `genesis.json`.
A field whose section below gives a default value is optional.
All other fields are required, and a `genesis.json` file without one of them fails to parse.
The `records` key is not part of `GenesisConfig`.
It can be left out when the records come from a separate file set by `genesis_records_file` in the node `config.json`.
The node still needs the records from one of these places.
The node ignores keys that it does not know.
A misspelled optional field is ignored without a warning, and the node uses its default value.

For mainnet and testnet, the node does not use the epoch settings from genesis.
These settings are `num_block_producer_seats`, the kickout thresholds, `max_inflation_rate` and `fishermen_threshold`.
They also include the optional fields at the end of this page.
They do not include `shard_layout` and `use_production_config`.
For these settings, mainnet and testnet use the epoch config that the node has built in for each protocol version (`core/primitives/res/epoch_configs/`).
`epoch_length` is the exception: the node always reads it from genesis.
Other chains use the files in the `epoch_configs` folder of the node home when that folder exists.
Without that folder, a chain whose ID starts with `test-chain-` builds its epoch config from the genesis fields.
Any other chain uses fixed test values and takes only `num_block_producer_seats`, `shard_layout` and `epoch_length` from genesis.

Runtime parameters, such as fees, gas costs and VM limits, are not part of genesis.
Each nearcore release contains them for every protocol version (`core/parameters/res/runtime_configs/`), and the node uses the set for the current protocol version.
The node ignores a `runtime_config` key in `genesis.json`, such as the one in the mainnet genesis file.
The `EXPERIMENTAL_protocol_config` RPC method returns the active values.
See [RuntimeConfig](RuntimeConfig.md).
The share of burnt gas that a contract account receives as a reward is also a runtime parameter, `burnt_gas_reward` in [RuntimeFeeConfig](RuntimeFeeConfig.md).
From protocol version 87 (nearcore 2.14.0), it is 0.
Before that it was 3/10.

## protocol_version

`type: u32`

Protocol version that this genesis works with.
Mainnet and testnet start at protocol version 29.

## genesis_time

`type: DateTime`

Official time of blockchain start.

## genesis_height

`type: u64`

Height of the genesis block. Note that genesis height is not necessarily 0.
For example, mainnet genesis height is `9820210`.

## chain_id

`type: String`

ID of the blockchain. This must be unique for every blockchain.
If your testnet blockchains do not have unique chain IDs, you will have a bad time.

## num_block_producer_seats

`type: u64`

Number of block producer seats at genesis.
On mainnet and testnet the node uses the value from its built-in epoch config for the current protocol version instead.
Mainnet uses 100.

## dynamic_resharding

`type: bool`

The node does not use this flag.
It only copies the value into the `EXPERIMENTAL_protocol_config` RPC response.
Dynamic resharding is set by the `dynamic_resharding_config` field of the epoch config.

## epoch_length

`type: u64`

Epoch length counted in block heights.
The node always reads this value from genesis, also on mainnet and testnet.
It replaces the `epoch_length` of the epoch config.
Mainnet uses 43200.

## gas_limit

`type: Gas`

Gas limit of a chunk.
The chunks of the genesis block use this value, and every later chunk keeps the same limit.
Mainnet uses `1000000000000000` (10^15).

## min_gas_price

`type: Balance`

Minimum gas price in yoctoNEAR per gas. It is also the gas price of the genesis block.
For chains whose genesis protocol version is 29 (mainnet and testnet), blocks after genesis use a fixed minimum of `100000000` (10^8) instead of this value.
The mainnet genesis file holds `1000000000` (10^9).

## max_gas_price

`type: Balance`

Maximum gas price in yoctoNEAR per gas.
The node uses the smaller of this value and 20 times the minimum gas price.
The mainnet genesis file holds `10000000000000000000000` (10^22).
On mainnet the result is `2000000000` (2 x 10^9).

## block_producer_kickout_threshold

`type: u8`

Criterion for kicking out block producers (this is a number between 0 and 100).
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 80.

## chunk_producer_kickout_threshold

`type: u8`

Criterion for kicking out chunk producers (this is a number between 0 and 100).
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 80.

## gas_price_adjustment_rate

`type: Fraction`

Gas price adjustment rate

## validators

`type: [AccountInfo]`

List of initial validators.

## records

_type: Vec\<[StateRecord](StateRecord.md)\>_

Records in storage at genesis (get split into shards at genesis creation).

## transaction_validity_period

`type: u64`

Number of blocks for which a given transaction is valid.
The value must not be more than twice the `epoch_length` of any epoch config that the node loads, or the node does not start.
On mainnet and testnet that `epoch_length` is 43200.
Mainnet uses 86400.

## protocol_reward_rate

`type: Fraction`

Share of each epoch reward that goes to the protocol treasury account.
For chains whose genesis protocol version is 29 (mainnet and testnet), the node uses 1/10 instead of this value.
The mainnet genesis file holds `[0, 1]`.

## max_inflation_rate

`type: Fraction`

Maximum yearly inflation of the total supply.
Each epoch mints at most `max_inflation_rate * total_supply * epoch_duration / one_year`.
Here `epoch_duration` is the real time that the epoch took, and one year is 365 days.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
From protocol version 81 (nearcore 2.9.0), mainnet uses 1/40 (2.5% per year).
Before that it used 1/20 (5% per year).
The mainnet genesis file holds `[0, 1]`.

## total_supply

`type: Balance`

Total supply of tokens at genesis.

## num_blocks_per_year

`type: u64`

Expected number of blocks per year.
The node does not use this value.
The epoch reward depends on the real duration of the epoch instead (see `max_inflation_rate`).

## protocol_treasury_account

`type: AccountId`

Protocol treasury account

## fishermen_threshold

`type: Balance`

Fishermen stake threshold.
The field is required, but the node does not use its value.
The mainnet genesis file and the mainnet epoch configs set it to the largest `u128` value.

## protocol_upgrade_stake_threshold

`type: Fraction`

This field is optional. The default is `[8, 10]`.
More than this share of the block producer stake must vote for a new protocol version before the network switches to it.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses `[4, 5]`.

## chunk_validator_only_kickout_threshold

`type: u8`

This field is optional. The default is 80.
Criterion for kicking out validators that are only chunk validators (this is a number between 0 and 100).
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 70.

## target_validator_mandates_per_shard

`type: u64`

This field is optional. The default is 68.
Target number of chunk validator mandates for each shard.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 105.

## online_min_threshold

`type: Fraction`

This field is optional. The default is `[90, 100]`.
A validator whose online ratio is below this value gets no reward.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses `[90, 100]`.

## online_max_threshold

`type: Fraction`

This field is optional. The default is `[99, 100]`.
A validator whose online ratio is at or above this value gets the full reward.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses `[99, 100]`.

## minimum_stake_divisor

`type: u64`

This field is optional. The default is 10.
The minimum stake for a staking proposal is the seat price of the next epoch divided by this number.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 10.

## shard_layout

`type: ShardLayout`

This field is optional. If it is omitted, the chain starts with one shard.
Defines the shards at genesis and how accounts map to them.
The node uses it to split the genesis records into shards.
On mainnet and testnet, the later shard layouts come from the built-in epoch configs and from resharding.
The mainnet genesis file does not set this field, so mainnet starts with one shard.

## minimum_validators_per_shard

`type: u64`

This field is optional. The default is 1.
Minimum number of validators that each shard must have.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 1.

## max_kickout_stake_perc

`type: u8`

This field is optional. The default is 100.
Maximum percentage of the total validator stake that can be kicked out in one epoch.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 30.

## minimum_stake_ratio

`type: Fraction`

This field is optional. The default is `[160, 1000000]`.
The lowest ratio of stake to total stake that a validator can have to be selected as a block producer, chunk producer or chunk validator.
See [NEP-167](https://github.com/near/NEPs/pull/167) for details.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses `[1, 62500]`.

## shuffle_shard_assignment_for_chunk_producers

`type: bool`

This field is optional. The default is `false`.
If it is `true`, the node shuffles the sets of chunk producers across shards.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses `false`.

## use_production_config

`type: bool`

This field is optional. The default is `false`.
The node does not use this field.

## num_chunk_producer_seats

`type: u64`

This field is optional. The default is 100.
Number of chunk producer seats.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 100.

## num_chunk_validator_seats

`type: u64`

This field is optional. The default is 300.
Number of chunk validator seats.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 500.

## chunk_producer_assignment_changes_limit

`type: u64`

This field is optional. The default is 5.
Maximum number of shard changes in chunk producer assignments when the node moves chunk producers to balance their number across shards.
On mainnet and testnet the node uses the value from its built-in epoch config instead.
Mainnet uses 5.

## protocol economics

> For the specific economic specs, refer to [Economics Section](../Economics/Economics.md).
