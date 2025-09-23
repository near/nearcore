# GenesisConfig

## protocol_version

`type: u32`

Protocol version that this genesis works with.

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

## num_block_producers

`type: u32`

Number of block producer seats at genesis.

## block_producers_per_shard

`type: [ValidatorId]`

Defines number of shards and number of validators per each shard at genesis.

## avg_fisherman_per_shard

`type: [ValidatorId]`

Expected number of fisherman per shard.

## dynamic_resharding

`type: bool`

Enable dynamic re-sharding.

## epoch_length

`type: BlockIndex`

Epoch length counted in blocks.

## gas_limit

`type: Gas`

Initial gas limit for a block

## gas_price

`type: Balance`

Initial gas price

## block_producer_kickout_threshold

`type: u8`

Criterion for kicking out block producers (this is a number between 0 and 100)

## chunk_producer_kickout_threshold

`type: u8`

Criterion for kicking out chunk producers (this is a number between 0 and 100)

## gas_price_adjustment_rate

`type: Fraction`

Gas price adjustment rate

## runtime_config

_type: [RuntimeConfig](RuntimeConfig.md)_

Runtime configuration (mostly economics constants).

## validators

`type: [AccountInfo]`

List of initial validators.

## records

_type: Vec\<[StateRecord](StateRecord.md)\>_

Records in storage at genesis (get split into shards at genesis creation).

## transaction_validity_period

`type: u64`

Number of blocks for which a given transaction is valid

## developer_reward_percentage

`type: Fraction`

Developer reward percentage.

## protocol_reward_percentage

`type: Fraction`

Protocol treasury percentage.

## max_inflation_rate

`type: Fraction`

Maximum inflation on the total supply every epoch.

## total_supply

`type: Balance`

Total supply of tokens at genesis.

## num_blocks_per_year

`type: u64`

Expected number of blocks per year

## protocol_treasury_account

`type: AccountId`

Protocol treasury account

## protocol economics

> For the specific economic specs, refer to [Economics Section](../Economics/Economics.md).
