use std::collections::{HashMap, HashSet};

use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, Account, AccountContract};
use near_primitives::epoch_manager::{EpochConfig, EpochConfigStore};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::state_record::StateRecord;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeight, BlockHeightDelta, Gas, NumBlocks, NumSeats,
    ProtocolVersion,
};
use near_primitives::utils::from_timestamp;
use near_primitives::version::PROTOCOL_VERSION;
use near_time::Clock;
use num_rational::Rational32;

use crate::{
    FISHERMEN_THRESHOLD, Genesis, GenesisConfig, GenesisContents, GenesisRecords,
    PROTOCOL_UPGRADE_STAKE_THRESHOLD,
};

#[derive(Debug, Clone)]
pub struct TestEpochConfigBuilder {
    epoch_length: BlockHeightDelta,
    shard_layout: ShardLayout,
    num_block_producer_seats: NumSeats,
    num_chunk_producer_seats: NumSeats,
    num_chunk_validator_seats: NumSeats,
    target_validator_mandates_per_shard: NumSeats,
    avg_hidden_validator_seats_per_shard: Vec<NumSeats>,
    minimum_validators_per_shard: NumSeats,
    block_producer_kickout_threshold: u8,
    chunk_producer_kickout_threshold: u8,
    chunk_validator_only_kickout_threshold: u8,
    validator_max_kickout_stake_perc: u8,
    online_min_threshold: Rational32,
    online_max_threshold: Rational32,
    fishermen_threshold: Balance,
    protocol_upgrade_stake_threshold: Rational32,
    minimum_stake_divisor: u64,
    minimum_stake_ratio: Rational32,
    chunk_producer_assignment_changes_limit: NumSeats,
    shuffle_shard_assignment_for_chunk_producers: bool,

    // not used any more
    num_block_producer_seats_per_shard: Vec<NumSeats>,
    genesis_protocol_version: Option<ProtocolVersion>,
}

/// A builder for constructing a valid genesis for testing.
///
/// The philosophy is that this can be used to generate a genesis that is
/// consistent, with flexibility to override specific settings, and with
/// defaults that are likely to be reasonable.
///
/// For parameters that are especially difficult to set correctly, the builder
/// should provide the ability to set them in a more intuitive way. For example,
/// since the validator selection algorithm is rather tricky, the builder
/// provides an option to specify exactly which accounts should be block and
/// chunk-only producers.
#[derive(Clone, Debug)]
pub struct TestGenesisBuilder {
    chain_id: String,
    protocol_version: ProtocolVersion,
    // TODO: remove when epoch length is no longer controlled by genesis
    epoch_length: BlockHeightDelta,
    // TODO: remove when shard layout is no longer controlled by genesis
    shard_layout: ShardLayout,
    validators_spec: ValidatorsSpec,
    genesis_time: chrono::DateTime<chrono::Utc>,
    genesis_height: BlockHeight,
    min_gas_price: Balance,
    max_gas_price: Balance,
    gas_limit: Gas,
    transaction_validity_period: NumBlocks,
    protocol_treasury_account: String,
    max_inflation_rate: Rational32,
    dynamic_resharding: bool,
    fishermen_threshold: Balance,
    online_min_threshold: Rational32,
    online_max_threshold: Rational32,
    gas_price_adjustment_rate: Rational32,
    num_blocks_per_year: NumBlocks,
    protocol_reward_rate: Rational32,
    max_kickout_stake_perc: u8,
    minimum_stake_divisor: u64,
    protocol_upgrade_stake_threshold: Rational32,
    chunk_producer_assignment_changes_limit: NumSeats,
    user_accounts: Vec<UserAccount>,
}

#[derive(Debug, Clone)]
pub enum ValidatorsSpec {
    DesiredRoles {
        block_and_chunk_producers: Vec<String>,
        chunk_validators_only: Vec<String>,
    },
    Raw {
        validators: Vec<AccountInfo>,
        num_block_producer_seats: NumSeats,
        num_chunk_producer_seats: NumSeats,
        num_chunk_validator_seats: NumSeats,
    },
}

#[derive(Debug, Clone)]
struct UserAccount {
    account_id: AccountId,
    balance: Balance,
    access_keys: Vec<PublicKey>,
}

impl Default for TestEpochConfigBuilder {
    // NOTE: The hardcoded defaults below are meticulously chosen for the purpose of testing. If you
    // want to override any of them, add corresponding functions to set the field. DO NOT just
    // modify the defaults.
    fn default() -> Self {
        Self {
            epoch_length: 5,
            shard_layout: ShardLayout::single_shard(),
            num_block_producer_seats: 1,
            num_chunk_producer_seats: 1,
            num_chunk_validator_seats: 1,
            target_validator_mandates_per_shard: 68,
            avg_hidden_validator_seats_per_shard: vec![],
            minimum_validators_per_shard: 1,
            block_producer_kickout_threshold: 0,
            chunk_producer_kickout_threshold: 0,
            chunk_validator_only_kickout_threshold: 0,
            validator_max_kickout_stake_perc: 100,
            online_min_threshold: Rational32::new(90, 100),
            online_max_threshold: Rational32::new(99, 100),
            fishermen_threshold: FISHERMEN_THRESHOLD,
            protocol_upgrade_stake_threshold: PROTOCOL_UPGRADE_STAKE_THRESHOLD,
            minimum_stake_divisor: 10,
            minimum_stake_ratio: Rational32::new(16i32, 1_000_000i32),
            chunk_producer_assignment_changes_limit: 5,
            shuffle_shard_assignment_for_chunk_producers: false,
            // consider them ineffective
            num_block_producer_seats_per_shard: vec![1],
            genesis_protocol_version: None,
        }
    }
}

impl TestEpochConfigBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn from_genesis(genesis: &Genesis) -> Self {
        let mut builder = Self::new();
        builder.epoch_length = genesis.config.epoch_length;
        builder.shard_layout = genesis.config.shard_layout.clone();
        builder.num_block_producer_seats = genesis.config.num_block_producer_seats;
        builder.num_chunk_producer_seats = genesis.config.num_chunk_producer_seats;
        builder.num_chunk_validator_seats = genesis.config.num_chunk_validator_seats;
        builder.genesis_protocol_version = Some(genesis.config.protocol_version);
        builder
    }

    pub fn build_store_from_genesis(genesis: &Genesis) -> EpochConfigStore {
        Self::from_genesis(genesis).build_store_for_genesis_protocol_version()
    }

    pub fn epoch_length(mut self, epoch_length: BlockHeightDelta) -> Self {
        self.epoch_length = epoch_length;
        self
    }

    pub fn shard_layout(mut self, shard_layout: ShardLayout) -> Self {
        self.shard_layout = shard_layout;
        self
    }

    pub fn validators_spec(mut self, validators_spec: ValidatorsSpec) -> Self {
        let DerivedValidatorSetup {
            validators: _,
            num_block_producer_seats,
            num_chunk_producer_seats,
            num_chunk_validator_seats,
        } = derive_validator_setup(validators_spec);
        self.num_block_producer_seats = num_block_producer_seats;
        self.num_chunk_producer_seats = num_chunk_producer_seats;
        self.num_chunk_validator_seats = num_chunk_validator_seats;
        self
    }

    pub fn minimum_validators_per_shard(mut self, minimum_validators_per_shard: NumSeats) -> Self {
        self.minimum_validators_per_shard = minimum_validators_per_shard;
        self
    }

    pub fn target_validator_mandates_per_shard(
        mut self,
        target_validator_mandates_per_shard: NumSeats,
    ) -> Self {
        self.target_validator_mandates_per_shard = target_validator_mandates_per_shard;
        self
    }

    pub fn shuffle_shard_assignment_for_chunk_producers(
        mut self,
        shuffle_shard_assignment_for_chunk_producers: bool,
    ) -> Self {
        self.shuffle_shard_assignment_for_chunk_producers =
            shuffle_shard_assignment_for_chunk_producers;
        self
    }

    // Validators with performance below 80% are kicked out, similarly to
    // mainnet as of 28 Jun 2024.
    pub fn kickouts_standard_80_percent(mut self) -> Self {
        self.block_producer_kickout_threshold = 80;
        self.chunk_producer_kickout_threshold = 80;
        self.chunk_validator_only_kickout_threshold = 80;
        self
    }

    // Only chunk validator-only nodes can be kicked out.
    pub fn kickouts_for_chunk_validators_only(mut self) -> Self {
        self.block_producer_kickout_threshold = 0;
        self.chunk_producer_kickout_threshold = 0;
        self.chunk_validator_only_kickout_threshold = 50;
        self
    }

    pub fn build(self) -> EpochConfig {
        let epoch_config = EpochConfig {
            epoch_length: self.epoch_length,
            shard_layout: self.shard_layout,
            num_block_producer_seats: self.num_block_producer_seats,
            num_chunk_producer_seats: self.num_chunk_producer_seats,
            num_chunk_validator_seats: self.num_chunk_validator_seats,
            num_chunk_only_producer_seats: 300,
            target_validator_mandates_per_shard: self.target_validator_mandates_per_shard,
            avg_hidden_validator_seats_per_shard: self.avg_hidden_validator_seats_per_shard,
            minimum_validators_per_shard: self.minimum_validators_per_shard,
            block_producer_kickout_threshold: self.block_producer_kickout_threshold,
            chunk_producer_kickout_threshold: self.chunk_producer_kickout_threshold,
            chunk_validator_only_kickout_threshold: self.chunk_validator_only_kickout_threshold,
            validator_max_kickout_stake_perc: self.validator_max_kickout_stake_perc,
            online_min_threshold: self.online_min_threshold,
            online_max_threshold: self.online_max_threshold,
            fishermen_threshold: self.fishermen_threshold,
            protocol_upgrade_stake_threshold: self.protocol_upgrade_stake_threshold,
            minimum_stake_divisor: self.minimum_stake_divisor,
            minimum_stake_ratio: self.minimum_stake_ratio,
            chunk_producer_assignment_changes_limit: self.chunk_producer_assignment_changes_limit,
            shuffle_shard_assignment_for_chunk_producers: self
                .shuffle_shard_assignment_for_chunk_producers,
            num_block_producer_seats_per_shard: self.num_block_producer_seats_per_shard,
        };
        tracing::debug!("Epoch config: {:#?}", epoch_config);
        epoch_config
    }

    /// Creates `EpochConfigStore` instance with single protocol version from genesis.
    /// This should be used only when the builder is created with `from_genesis` constructor.
    pub fn build_store_for_genesis_protocol_version(self) -> EpochConfigStore {
        let protocol_version =
            self.genesis_protocol_version.expect("genesis protocol version is not specified");
        let epoch_config = self.build();
        EpochConfigStore::test_single_version(protocol_version, epoch_config)
    }
}

impl Default for TestGenesisBuilder {
    // NOTE: The hardcoded defaults below are meticulously chosen for the purpose of testing. If you
    // want to override any of them, add corresponding functions to set the field. DO NOT just
    // modify the defaults.
    fn default() -> Self {
        Self {
            chain_id: "test".to_string(),
            protocol_version: PROTOCOL_VERSION,
            epoch_length: 100,
            shard_layout: ShardLayout::single_shard(),
            validators_spec: ValidatorsSpec::DesiredRoles {
                block_and_chunk_producers: vec!["validator0".to_string()],
                chunk_validators_only: vec![],
            },
            genesis_time: chrono::Utc::now(),
            genesis_height: 1,
            min_gas_price: 0,
            max_gas_price: 0,
            gas_limit: 1_000_000_000_000_000,
            transaction_validity_period: 100,
            protocol_treasury_account: "near".to_string().parse().unwrap(),
            max_inflation_rate: Rational32::new(1, 1),
            user_accounts: vec![],
            dynamic_resharding: false,
            fishermen_threshold: 0,
            online_min_threshold: Rational32::new(90, 100),
            online_max_threshold: Rational32::new(99, 100),
            gas_price_adjustment_rate: Rational32::new(0, 1),
            num_blocks_per_year: 86400,
            protocol_reward_rate: Rational32::new(0, 1),
            max_kickout_stake_perc: 100,
            minimum_stake_divisor: 10,
            protocol_upgrade_stake_threshold: Rational32::new(8, 10),
            chunk_producer_assignment_changes_limit: 5,
        }
    }
}

impl TestGenesisBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn chain_id(mut self, chain_id: String) -> Self {
        self.chain_id = chain_id;
        self
    }

    pub fn genesis_time(mut self, genesis_time: chrono::DateTime<chrono::Utc>) -> Self {
        self.genesis_time = genesis_time;
        self
    }

    pub fn genesis_time_from_clock(mut self, clock: &Clock) -> Self {
        self.genesis_time = from_timestamp(clock.now_utc().unix_timestamp_nanos() as u64);
        self
    }

    pub fn protocol_version(mut self, protocol_version: ProtocolVersion) -> Self {
        self.protocol_version = protocol_version;
        self
    }

    pub fn genesis_height(mut self, genesis_height: BlockHeight) -> Self {
        self.genesis_height = genesis_height;
        self
    }

    pub fn epoch_length(mut self, epoch_length: BlockHeightDelta) -> Self {
        self.epoch_length = epoch_length;
        self
    }

    pub fn shard_layout(mut self, shard_layout: ShardLayout) -> Self {
        self.shard_layout = shard_layout;
        self
    }

    pub fn gas_prices(mut self, min: Balance, max: Balance) -> Self {
        self.min_gas_price = min;
        self.max_gas_price = max;
        self
    }

    pub fn gas_limit(mut self, gas_limit: Gas) -> Self {
        self.gas_limit = gas_limit;
        self
    }

    pub fn gas_limit_one_petagas(mut self) -> Self {
        self.gas_limit = 1_000_000_000_000_000;
        self
    }

    pub fn transaction_validity_period(mut self, transaction_validity_period: NumBlocks) -> Self {
        self.transaction_validity_period = transaction_validity_period;
        self
    }

    pub fn validators_spec(mut self, validators_spec: ValidatorsSpec) -> Self {
        self.validators_spec = validators_spec;
        self
    }

    pub fn max_inflation_rate(mut self, max_inflation_rate: Rational32) -> Self {
        self.max_inflation_rate = max_inflation_rate;
        self
    }

    pub fn protocol_reward_rate(mut self, protocol_reward_rate: Rational32) -> Self {
        self.protocol_reward_rate = protocol_reward_rate;
        self
    }

    /// Specifies the protocol treasury account. If not specified, this will
    /// pick an arbitrary account name and ensure that it is included in the
    /// genesis records.
    pub fn protocol_treasury_account(mut self, protocol_treasury_account: String) -> Self {
        self.protocol_treasury_account = protocol_treasury_account;
        self
    }

    pub fn add_user_account_simple(
        mut self,
        account_id: AccountId,
        initial_balance: Balance,
    ) -> Self {
        self.user_accounts.push(UserAccount {
            balance: initial_balance,
            access_keys: vec![create_user_test_signer(&account_id).public_key()],
            account_id,
        });
        self
    }

    pub fn add_user_accounts_simple(
        mut self,
        accounts: &[AccountId],
        initial_balance: Balance,
    ) -> Self {
        for account_id in accounts {
            self.user_accounts.push(UserAccount {
                balance: initial_balance,
                access_keys: vec![create_user_test_signer(account_id).public_key()],
                account_id: account_id.clone(),
            });
        }
        self
    }

    pub fn build(self) -> Genesis {
        if self
            .user_accounts
            .iter()
            .map(|account| &account.account_id)
            .collect::<HashSet<_>>()
            .len()
            != self.user_accounts.len()
        {
            panic!("Duplicate user accounts specified.");
        }

        let protocol_treasury_account: AccountId = self.protocol_treasury_account.parse().unwrap();

        // We will merge the user accounts that were specified, with the
        // validator staking accounts from the validator setup, and ensure
        // that the protocol treasury account is included too. We will use all
        // of this to generate the genesis records and also calculate the
        // total supply.
        let mut user_accounts = self.user_accounts;
        if user_accounts.iter().all(|account| &account.account_id != &protocol_treasury_account) {
            tracing::warn!(
                "Protocol treasury account {:?} not found in user accounts;
                to keep genesis valid, adding it as a user account with zero balance.",
                protocol_treasury_account
            );
            user_accounts.push(UserAccount {
                account_id: protocol_treasury_account.clone(),
                balance: 0,
                access_keys: vec![],
            });
        }

        let DerivedValidatorSetup {
            validators,
            num_block_producer_seats,
            num_chunk_producer_seats,
            num_chunk_validator_seats,
        } = derive_validator_setup(self.validators_spec);

        let mut total_supply = 0;
        let mut validator_stake: HashMap<AccountId, Balance> = HashMap::new();
        for validator in &validators {
            total_supply += validator.amount;
            validator_stake.insert(validator.account_id.clone(), validator.amount);
        }
        let mut records = Vec::new();
        for user_account in &user_accounts {
            total_supply += user_account.balance;
            records.push(StateRecord::Account {
                account_id: user_account.account_id.clone(),
                account: Account::new(
                    user_account.balance,
                    validator_stake.remove(&user_account.account_id).unwrap_or(0),
                    AccountContract::None,
                    0,
                ),
            });
            for access_key in &user_account.access_keys {
                records.push(StateRecord::AccessKey {
                    account_id: user_account.account_id.clone(),
                    public_key: access_key.clone(),
                    access_key: AccessKey {
                        nonce: 0,
                        permission: near_primitives::account::AccessKeyPermission::FullAccess,
                    },
                });
            }
        }
        for (account_id, balance) in validator_stake {
            records.push(StateRecord::Account {
                account_id,
                account: Account::new(0, balance, AccountContract::None, 0),
            });
        }

        let genesis_config = GenesisConfig {
            chain_id: self.chain_id,
            genesis_time: self.genesis_time,
            genesis_height: self.genesis_height,
            epoch_length: self.epoch_length,
            min_gas_price: self.min_gas_price,
            max_gas_price: self.max_gas_price,
            gas_limit: self.gas_limit,
            dynamic_resharding: self.dynamic_resharding,
            fishermen_threshold: self.fishermen_threshold,
            transaction_validity_period: self.transaction_validity_period,
            protocol_version: self.protocol_version,
            protocol_treasury_account,
            online_min_threshold: self.online_min_threshold,
            online_max_threshold: self.online_max_threshold,
            gas_price_adjustment_rate: self.gas_price_adjustment_rate,
            num_blocks_per_year: self.num_blocks_per_year,
            protocol_reward_rate: self.protocol_reward_rate,
            total_supply,
            max_kickout_stake_perc: self.max_kickout_stake_perc,
            validators,
            shard_layout: self.shard_layout.clone(),
            num_block_producer_seats,
            num_block_producer_seats_per_shard: self
                .shard_layout
                .shard_ids()
                .map(|_| num_block_producer_seats)
                .collect(),
            minimum_stake_divisor: self.minimum_stake_divisor,
            max_inflation_rate: self.max_inflation_rate,
            protocol_upgrade_stake_threshold: self.protocol_upgrade_stake_threshold,
            num_chunk_producer_seats,
            num_chunk_validator_seats,
            chunk_producer_assignment_changes_limit: self.chunk_producer_assignment_changes_limit,
            ..Default::default()
        };
        tracing::debug!("Genesis config: {:#?}", genesis_config);

        Genesis {
            config: genesis_config,
            contents: GenesisContents::Records { records: GenesisRecords(records) },
        }
    }
}

impl ValidatorsSpec {
    /// Specifies that we want the validators to be exactly the specified accounts.
    /// This will generate a reasonable set of parameters so that the given
    /// validators are selected as specified.
    pub fn desired_roles(
        block_and_chunk_producers: &[&str],
        chunk_validators_only: &[&str],
    ) -> Self {
        ValidatorsSpec::DesiredRoles {
            block_and_chunk_producers: block_and_chunk_producers
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            chunk_validators_only: chunk_validators_only.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    /// Specifies the validator fields directly, relying on the validator selection
    /// algorithm to determine which validators are selected as block or chunk
    /// producers.
    pub fn raw(
        validators: Vec<AccountInfo>,
        num_block_producer_seats: NumSeats,
        num_chunk_producer_seats: NumSeats,
        num_chunk_validator_only_seats: NumSeats,
    ) -> Self {
        let num_chunk_validator_seats =
            std::cmp::max(num_block_producer_seats, num_chunk_producer_seats)
                + num_chunk_validator_only_seats;
        ValidatorsSpec::Raw {
            validators,
            num_block_producer_seats,
            num_chunk_producer_seats,
            num_chunk_validator_seats,
        }
    }
}

struct DerivedValidatorSetup {
    validators: Vec<AccountInfo>,
    num_block_producer_seats: NumSeats,
    num_chunk_producer_seats: NumSeats,
    num_chunk_validator_seats: NumSeats,
}

const ONE_NEAR: Balance = 1_000_000_000_000_000_000_000_000;

fn derive_validator_setup(specs: ValidatorsSpec) -> DerivedValidatorSetup {
    match specs {
        ValidatorsSpec::DesiredRoles { block_and_chunk_producers, chunk_validators_only } => {
            let num_block_and_chunk_producer_seats = block_and_chunk_producers.len() as NumSeats;
            let num_chunk_validator_only_seats = chunk_validators_only.len() as NumSeats;
            let mut validators = Vec::new();
            for i in 0..num_block_and_chunk_producer_seats as usize {
                let account_id: AccountId = block_and_chunk_producers[i].parse().unwrap();
                let account_info = AccountInfo {
                    public_key: create_test_signer(account_id.as_str()).public_key(),
                    account_id,
                    amount: ONE_NEAR * (10000 - i as Balance),
                };
                validators.push(account_info);
            }
            for i in 0..num_chunk_validator_only_seats as usize {
                let account_id: AccountId = chunk_validators_only[i].parse().unwrap();
                let account_info = AccountInfo {
                    public_key: create_test_signer(account_id.as_str()).public_key(),
                    account_id,
                    amount: ONE_NEAR
                        * (10000 - i as Balance - num_block_and_chunk_producer_seats as Balance),
                };
                validators.push(account_info);
            }
            DerivedValidatorSetup {
                validators,
                num_block_producer_seats: num_block_and_chunk_producer_seats,
                num_chunk_producer_seats: num_block_and_chunk_producer_seats,
                num_chunk_validator_seats: num_block_and_chunk_producer_seats
                    + num_chunk_validator_only_seats,
            }
        }
        ValidatorsSpec::Raw {
            validators,
            num_block_producer_seats,
            num_chunk_producer_seats,
            num_chunk_validator_seats,
        } => DerivedValidatorSetup {
            validators,
            num_block_producer_seats,
            num_chunk_producer_seats,
            num_chunk_validator_seats,
        },
    }
}
