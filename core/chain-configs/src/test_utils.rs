use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_primitives::account::{AccessKey, Account};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{AccountId, AccountInfo, Balance, NumSeats, NumShards};
use near_primitives::utils::{from_timestamp, generate_random_string};
use near_primitives::version::PROTOCOL_VERSION;
use near_time::Clock;
use num_rational::Ratio;

use crate::{
    Genesis, GenesisConfig, FAST_EPOCH_LENGTH, GAS_PRICE_ADJUSTMENT_RATE, INITIAL_GAS_LIMIT,
    MAX_INFLATION_RATE, MIN_GAS_PRICE, NEAR_BASE, NUM_BLOCKS_PER_YEAR, PROTOCOL_REWARD_RATE,
    PROTOCOL_TREASURY_ACCOUNT, TRANSACTION_VALIDITY_PERIOD,
};

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000 * NEAR_BASE;

impl GenesisConfig {
    pub fn test(clock: Clock) -> Self {
        GenesisConfig {
            genesis_time: from_timestamp(clock.now_utc().unix_timestamp_nanos() as u64),
            genesis_height: 0,
            gas_limit: 10u64.pow(15),
            min_gas_price: 0,
            max_gas_price: 1_000_000_000,
            total_supply: 1_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: 100,
            epoch_length: 5,
            protocol_version: PROTOCOL_VERSION,
            ..Default::default()
        }
    }
}

impl Genesis {
    // Creates new genesis with a given set of accounts and shard layout.
    // The first num_validator_seats from accounts will be treated as 'validators'.
    pub fn test_with_seeds(
        clock: Clock,
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        _num_validator_seats_per_shard: Vec<NumSeats>,
        shard_layout: ShardLayout,
    ) -> Self {
        let mut validators = vec![];
        let mut records = vec![];
        for (i, account) in accounts.into_iter().enumerate() {
            let signer =
                InMemorySigner::from_seed(account.clone(), KeyType::ED25519, account.as_ref());
            let i = i as u64;
            if i < num_validator_seats {
                validators.push(AccountInfo {
                    account_id: account.clone(),
                    public_key: signer.public_key.clone(),
                    amount: TESTING_INIT_STAKE,
                });
            }
            add_account_with_key(
                &mut records,
                account,
                &signer.public_key.clone(),
                TESTING_INIT_BALANCE - if i < num_validator_seats { TESTING_INIT_STAKE } else { 0 },
                if i < num_validator_seats { TESTING_INIT_STAKE } else { 0 },
                CryptoHash::default(),
            );
        }
        add_protocol_account(&mut records);
        let epoch_config =
            Self::test_epoch_config(num_validator_seats, shard_layout, FAST_EPOCH_LENGTH);
        let config = GenesisConfig {
            protocol_version: PROTOCOL_VERSION,
            genesis_time: from_timestamp(clock.now_utc().unix_timestamp_nanos() as u64),
            chain_id: random_chain_id(),
            dynamic_resharding: false,
            validators,
            protocol_reward_rate: PROTOCOL_REWARD_RATE,
            total_supply: get_initial_supply(&records),
            max_inflation_rate: MAX_INFLATION_RATE,
            num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
            protocol_treasury_account: PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
            transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
            gas_limit: INITIAL_GAS_LIMIT,
            gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
            min_gas_price: MIN_GAS_PRICE,

            // epoch config parameters
            num_block_producer_seats: epoch_config.num_block_producer_seats,
            num_block_producer_seats_per_shard: epoch_config.num_block_producer_seats_per_shard,
            avg_hidden_validator_seats_per_shard: epoch_config.avg_hidden_validator_seats_per_shard,
            protocol_upgrade_stake_threshold: epoch_config.protocol_upgrade_stake_threshold,
            epoch_length: epoch_config.epoch_length,
            block_producer_kickout_threshold: epoch_config.block_producer_kickout_threshold,
            chunk_producer_kickout_threshold: epoch_config.chunk_producer_kickout_threshold,
            chunk_validator_only_kickout_threshold: epoch_config
                .chunk_validator_only_kickout_threshold,
            fishermen_threshold: epoch_config.fishermen_threshold,
            shard_layout: epoch_config.shard_layout,
            target_validator_mandates_per_shard: epoch_config.target_validator_mandates_per_shard,
            max_kickout_stake_perc: epoch_config.validator_max_kickout_stake_perc,
            online_min_threshold: epoch_config.online_min_threshold,
            online_max_threshold: epoch_config.online_max_threshold,
            minimum_stake_divisor: epoch_config.minimum_stake_divisor,
            num_chunk_producer_seats: epoch_config
                .validator_selection_config
                .num_chunk_producer_seats,
            num_chunk_validator_seats: epoch_config
                .validator_selection_config
                .num_chunk_validator_seats,
            num_chunk_only_producer_seats: epoch_config
                .validator_selection_config
                .num_chunk_only_producer_seats,
            minimum_validators_per_shard: epoch_config
                .validator_selection_config
                .minimum_validators_per_shard,
            minimum_stake_ratio: epoch_config.validator_selection_config.minimum_stake_ratio,
            chunk_producer_assignment_changes_limit: epoch_config
                .validator_selection_config
                .chunk_producer_assignment_changes_limit,
            shuffle_shard_assignment_for_chunk_producers: epoch_config
                .validator_selection_config
                .shuffle_shard_assignment_for_chunk_producers,

            ..Default::default()
        };
        Genesis::new(config, records.into()).unwrap()
    }

    pub fn test(accounts: Vec<AccountId>, num_validator_seats: NumSeats) -> Self {
        Self::test_with_seeds(
            Clock::real(),
            accounts,
            num_validator_seats,
            vec![num_validator_seats],
            ShardLayout::v0_single_shard(),
        )
    }

    pub fn test_sharded(
        clock: Clock,
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let num_shards = num_validator_seats_per_shard.len() as NumShards;
        Self::test_with_seeds(
            clock,
            accounts,
            num_validator_seats,
            num_validator_seats_per_shard,
            ShardLayout::v0(num_shards, 0),
        )
    }

    pub fn test_sharded_new_version(
        accounts: Vec<AccountId>,
        num_validator_seats: NumSeats,
        num_validator_seats_per_shard: Vec<NumSeats>,
    ) -> Self {
        let num_shards = num_validator_seats_per_shard.len() as NumShards;
        Self::test_with_seeds(
            Clock::real(),
            accounts,
            num_validator_seats,
            num_validator_seats_per_shard,
            ShardLayout::v0(num_shards, 1),
        )
    }
}

pub fn add_protocol_account(records: &mut Vec<StateRecord>) {
    let signer = InMemorySigner::from_seed(
        PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
        KeyType::ED25519,
        PROTOCOL_TREASURY_ACCOUNT,
    );
    add_account_with_key(
        records,
        PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
        &signer.public_key,
        TESTING_INIT_BALANCE,
        0,
        CryptoHash::default(),
    );
}

pub fn add_account_with_key(
    records: &mut Vec<StateRecord>,
    account_id: AccountId,
    public_key: &PublicKey,
    amount: u128,
    staked: u128,
    code_hash: CryptoHash,
) {
    records.push(StateRecord::Account {
        account_id: account_id.clone(),
        account: Account::new(amount, staked, 0, code_hash, 0, PROTOCOL_VERSION),
    });
    records.push(StateRecord::AccessKey {
        account_id,
        public_key: public_key.clone(),
        access_key: AccessKey::full_access(),
    });
}

pub fn random_chain_id() -> String {
    format!("test-chain-{}", generate_random_string(5))
}

pub fn get_initial_supply(records: &[StateRecord]) -> Balance {
    let mut total_supply = 0;
    for record in records {
        if let StateRecord::Account { account, .. } = record {
            total_supply += account.amount() + account.locked();
        }
    }
    total_supply
}
