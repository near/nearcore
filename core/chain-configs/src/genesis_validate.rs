use std::collections::{HashMap, HashSet};

use crate::genesis_config::{Genesis, GenesisConfig};
use near_crypto::key_conversion::is_valid_staking_key;
use near_primitives::state_record::StateRecord;
use near_primitives::types::AccountId;
use num_rational::Rational64;

/// Validate genesis config and records. Panics if genesis is ill-formed.
pub fn validate_genesis(genesis: &Genesis) {
    let mut genesis_validator = GenesisValidator::new(&genesis.config);
    genesis.for_each_record(|record: &StateRecord| {
        genesis_validator.process_record(record);
    });
    genesis_validator.validate();
}

struct GenesisValidator<'a> {
    genesis_config: &'a GenesisConfig,
    total_supply: u128,
    staked_accounts: HashMap<AccountId, u128>,
    account_ids: HashSet<AccountId>,
    access_key_account_ids: HashSet<AccountId>,
    contract_account_ids: HashSet<AccountId>,
}

impl<'a> GenesisValidator<'a> {
    pub fn new(genesis_config: &'a GenesisConfig) -> Self {
        Self {
            genesis_config,
            total_supply: 0,
            staked_accounts: HashMap::new(),
            account_ids: HashSet::new(),
            access_key_account_ids: HashSet::new(),
            contract_account_ids: HashSet::new(),
        }
    }

    pub fn process_record(&mut self, record: &StateRecord) {
        match record {
            StateRecord::Account { account_id, account } => {
                if self.account_ids.contains(account_id) {
                    panic!("Duplicate account id {} in genesis records", account_id);
                }
                self.total_supply += account.locked() + account.amount();
                self.account_ids.insert(account_id.clone());
                if account.locked() > 0 {
                    self.staked_accounts.insert(account_id.clone(), account.locked());
                }
            }
            StateRecord::AccessKey { account_id, .. } => {
                self.access_key_account_ids.insert(account_id.clone());
            }
            StateRecord::Contract { account_id, .. } => {
                if self.contract_account_ids.contains(account_id) {
                    panic!("account {} has more than one contract deployed", account_id);
                }
                self.contract_account_ids.insert(account_id.clone());
            }
            _ => {}
        }
    }

    pub fn validate(&self) {
        let validators = self
            .genesis_config
            .validators
            .clone()
            .into_iter()
            .map(|account_info| {
                assert!(
                    is_valid_staking_key(&account_info.public_key),
                    "validator staking key is not valid"
                );
                (account_info.account_id, account_info.amount)
            })
            .collect::<HashMap<_, _>>();
        assert_eq!(
            validators.len(),
            self.genesis_config.validators.len(),
            "Duplicate account in validators"
        );
        assert!(!validators.is_empty(), "no validators in genesis");

        assert_eq!(self.total_supply, self.genesis_config.total_supply, "wrong total supply");
        assert_eq!(
            validators, self.staked_accounts,
            "validator accounts do not match staked accounts"
        );
        for account_id in &self.access_key_account_ids {
            assert!(
                self.account_ids.contains(account_id),
                "access key account {} does not exist",
                account_id
            );
        }
        for account_id in &self.contract_account_ids {
            assert!(
                self.account_ids.contains(account_id),
                "contract account {} does not exist",
                account_id
            );
        }
        assert!(
            self.genesis_config.online_max_threshold > self.genesis_config.online_min_threshold,
            "Online max threshold smaller than min threshold"
        );
        assert!(
            self.genesis_config.online_max_threshold <= Rational64::from_integer(1_i64),
            "Online max threshold must be less or equal than 1"
        );
        assert!(
            *self.genesis_config.online_max_threshold.numer() < 10_000_000,
            "Numerator is too large, may lead to overflow."
        );
        assert!(
            *self.genesis_config.online_min_threshold.numer() < 10_000_000,
            "Numerator is too large, may lead to overflow."
        );
        assert!(
            *self.genesis_config.online_max_threshold.denom() < 10_000_000,
            "Denominator is too large, may lead to overflow."
        );
        assert!(
            *self.genesis_config.online_min_threshold.denom() < 10_000_000,
            "Denominator is too large, may lead to overflow."
        );
        assert!(
            self.genesis_config.gas_price_adjustment_rate < Rational64::from_integer(1_i64),
            "Gas price adjustment rate must be less than 1"
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use crate::GenesisRecords;
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::account::{AccessKey, Account};
    use near_primitives::types::AccountInfo;

    const VALID_ED25519_RISTRETTO_KEY: &str = "ed25519:KuTCtARNzxZQ3YvXDeLjx83FDqxv2SdQTSbiq876zR7";

    fn create_account() -> Account {
        Account::new(100, 10, Default::default(), 0)
    }

    #[test]
    #[should_panic(expected = "wrong total supply")]
    fn test_total_supply_not_match() {
        let mut genesis = Genesis::default();
        genesis.config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: VALID_ED25519_RISTRETTO_KEY.parse().unwrap(),
            amount: 10,
        }];
        genesis.records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".parse().unwrap(),
            account: create_account(),
        }]);
        validate_genesis(&genesis);
    }

    #[test]
    #[should_panic(expected = "validator staking key is not valid")]
    fn test_invalid_staking_key() {
        let mut genesis = Genesis::default();
        genesis.config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: PublicKey::empty(KeyType::ED25519),
            amount: 10,
        }];
        genesis.records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".parse().unwrap(),
            account: create_account(),
        }]);
        validate_genesis(&genesis);
    }

    #[test]
    #[should_panic(expected = "validator accounts do not match staked accounts")]
    fn test_validator_not_match() {
        let mut genesis = Genesis::default();
        genesis.config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: VALID_ED25519_RISTRETTO_KEY.parse().unwrap(),
            amount: 100,
        }];
        genesis.config.total_supply = 110;
        genesis.records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".parse().unwrap(),
            account: create_account(),
        }]);
        validate_genesis(&genesis);
    }

    #[test]
    #[should_panic(expected = "no validators in genesis")]
    fn test_empty_validator() {
        let mut genesis = Genesis::default();
        genesis.records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".parse().unwrap(),
            account: create_account(),
        }]);
        validate_genesis(&genesis);
    }

    #[test]
    #[should_panic(expected = "access key account test1 does not exist")]
    fn test_access_key_with_nonexistent_account() {
        let mut genesis = Genesis::default();
        genesis.config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: VALID_ED25519_RISTRETTO_KEY.parse().unwrap(),
            amount: 10,
        }];
        genesis.config.total_supply = 110;
        genesis.records = GenesisRecords(vec![
            StateRecord::Account { account_id: "test".parse().unwrap(), account: create_account() },
            StateRecord::AccessKey {
                account_id: "test1".parse().unwrap(),
                public_key: PublicKey::empty(KeyType::ED25519),
                access_key: AccessKey::full_access(),
            },
        ]);
        validate_genesis(&genesis);
    }

    #[test]
    #[should_panic(expected = "account test has more than one contract deployed")]
    fn test_more_than_one_contract() {
        let mut genesis = Genesis::default();
        genesis.config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: VALID_ED25519_RISTRETTO_KEY.parse().unwrap(),
            amount: 10,
        }];
        genesis.config.total_supply = 110;
        genesis.records = GenesisRecords(vec![
            StateRecord::Account { account_id: "test".parse().unwrap(), account: create_account() },
            StateRecord::Contract { account_id: "test".parse().unwrap(), code: [1, 2, 3].to_vec() },
            StateRecord::Contract {
                account_id: "test".parse().unwrap(),
                code: [1, 2, 3, 4].to_vec(),
            },
        ]);
        validate_genesis(&genesis);
    }
}
