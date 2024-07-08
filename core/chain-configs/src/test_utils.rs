use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_primitives::account::{AccessKey, Account};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeightDelta, NumSeats, NumShards,
};
use near_primitives::utils::{from_timestamp, generate_random_string};
use near_primitives::version::PROTOCOL_VERSION;
use near_time::Clock;
use num_rational::Ratio;

use crate::{
    Genesis, GenesisConfig, BLOCK_PRODUCER_KICKOUT_THRESHOLD, CHUNK_PRODUCER_KICKOUT_THRESHOLD,
    CHUNK_VALIDATOR_ONLY_KICKOUT_THRESHOLD, FISHERMEN_THRESHOLD, GAS_PRICE_ADJUSTMENT_RATE,
    INITIAL_GAS_LIMIT, MAX_INFLATION_RATE, MIN_GAS_PRICE, NEAR_BASE, NUM_BLOCKS_PER_YEAR,
    PROTOCOL_REWARD_RATE, PROTOCOL_TREASURY_ACCOUNT, PROTOCOL_UPGRADE_STAKE_THRESHOLD,
    TRANSACTION_VALIDITY_PERIOD,
};

/// Initial balance used in tests.
pub const TESTING_INIT_BALANCE: Balance = 1_000_000_000 * NEAR_BASE;

/// Validator's stake used in tests.
pub const TESTING_INIT_STAKE: Balance = 50_000_000 * NEAR_BASE;

pub const FAST_EPOCH_LENGTH: BlockHeightDelta = 60;

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
        num_validator_seats_per_shard: Vec<NumSeats>,
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
        let config = GenesisConfig {
            protocol_version: PROTOCOL_VERSION,
            genesis_time: from_timestamp(clock.now_utc().unix_timestamp_nanos() as u64),
            chain_id: random_chain_id(),
            num_block_producer_seats: num_validator_seats,
            num_block_producer_seats_per_shard: num_validator_seats_per_shard.clone(),
            avg_hidden_validator_seats_per_shard: vec![0; num_validator_seats_per_shard.len()],
            dynamic_resharding: false,
            protocol_upgrade_stake_threshold: PROTOCOL_UPGRADE_STAKE_THRESHOLD,
            epoch_length: FAST_EPOCH_LENGTH,
            gas_limit: INITIAL_GAS_LIMIT,
            gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
            block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
            chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
            chunk_validator_only_kickout_threshold: CHUNK_VALIDATOR_ONLY_KICKOUT_THRESHOLD,
            validators,
            protocol_reward_rate: PROTOCOL_REWARD_RATE,
            total_supply: get_initial_supply(&records),
            max_inflation_rate: MAX_INFLATION_RATE,
            num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
            protocol_treasury_account: PROTOCOL_TREASURY_ACCOUNT.parse().unwrap(),
            transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
            fishermen_threshold: FISHERMEN_THRESHOLD,
            min_gas_price: MIN_GAS_PRICE,
            shard_layout,
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
