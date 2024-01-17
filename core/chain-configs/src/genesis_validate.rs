use crate::genesis_config::{Genesis, GenesisConfig, GenesisContents};
use near_config_utils::{ValidationError, ValidationErrors};
use near_crypto::key_conversion::is_valid_staking_key;
use near_primitives::state_record::StateRecord;
use near_primitives::types::AccountId;
use num_rational::Rational32;
use std::collections::{HashMap, HashSet};

/// Validate genesis config and records. Returns ValidationError if semantic checks of genesis failed.
pub fn validate_genesis(genesis: &Genesis) -> Result<(), ValidationError> {
    if let GenesisContents::StateRoots { .. } = &genesis.contents {
        // TODO(robin-near): We don't have a great way of validating the
        // genesis records if we're given state roots directly, though we
        // could still validate things that aren't related to records.
        return Ok(());
    }
    let mut validation_errors = ValidationErrors::new();
    let mut genesis_validator = GenesisValidator::new(&genesis.config, &mut validation_errors);
    tracing::info!(target: "config", "Validating Genesis config and records. This could take a few minutes...");
    genesis.for_each_record(|record: &StateRecord| {
        genesis_validator.process_record(record);
    });
    genesis_validator.validate_processed_records();
    genesis_validator.result_with_full_error()
}

struct GenesisValidator<'a> {
    genesis_config: &'a GenesisConfig,
    total_supply: u128,
    staked_accounts: HashMap<AccountId, u128>,
    account_ids: HashSet<AccountId>,
    access_key_account_ids: HashSet<AccountId>,
    contract_account_ids: HashSet<AccountId>,
    validation_errors: &'a mut ValidationErrors,
}

impl<'a> GenesisValidator<'a> {
    pub fn new(
        genesis_config: &'a GenesisConfig,
        validation_errors: &'a mut ValidationErrors,
    ) -> Self {
        Self {
            genesis_config,
            total_supply: 0,
            staked_accounts: HashMap::new(),
            account_ids: HashSet::new(),
            access_key_account_ids: HashSet::new(),
            contract_account_ids: HashSet::new(),
            validation_errors: validation_errors,
        }
    }

    pub fn process_record(&mut self, record: &StateRecord) {
        match record {
            StateRecord::Account { account_id, account } => {
                if self.account_ids.contains(account_id) {
                    let error_message =
                        format!("Duplicate account id {} in genesis records", account_id);
                    self.validation_errors.push_genesis_semantics_error(error_message)
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
                    let error_message =
                        format!("account {} has more than one contract deployed", account_id);
                    self.validation_errors.push_genesis_semantics_error(error_message)
                }
                self.contract_account_ids.insert(account_id.clone());
            }
            _ => {}
        }
    }

    pub fn validate_processed_records(&mut self) {
        let validators = self
            .genesis_config
            .validators
            .clone()
            .into_iter()
            .map(|account_info| {
                if !is_valid_staking_key(&account_info.public_key) {
                    let error_message = format!("validator staking key is not valid");
                    self.validation_errors.push_genesis_semantics_error(error_message);
                }
                (account_info.account_id, account_info.amount)
            })
            .collect::<HashMap<_, _>>();

        if validators.len() != self.genesis_config.validators.len() {
            let error_message = format!("Duplicate account in validators. The number of account_ids: {} does not match the number of validators: {}.", self.account_ids.len(), validators.len());
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if validators.is_empty() {
            let error_message = format!("No validators in genesis");
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if self.total_supply != self.genesis_config.total_supply {
            let error_message = format!("wrong total supply. account.locked() + account.amount() = {} is not equal to the total supply = {} specified in genesis config.", self.total_supply, self.genesis_config.total_supply);
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if validators != self.staked_accounts {
            let error_message = format!("Validator accounts do not match staked accounts.");
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        for account_id in &self.access_key_account_ids {
            if !self.account_ids.contains(account_id) {
                let error_message = format!("access key account {} does not exist", account_id);
                self.validation_errors.push_genesis_semantics_error(error_message)
            }
        }

        for account_id in &self.contract_account_ids {
            if !self.account_ids.contains(account_id) {
                let error_message = format!("contract account {} does not exist,", account_id);
                self.validation_errors.push_genesis_semantics_error(error_message)
            }
        }

        if self.genesis_config.online_max_threshold <= self.genesis_config.online_min_threshold {
            let error_message = format!(
                "Online max threshold {} smaller than min threshold {}",
                self.genesis_config.online_max_threshold, self.genesis_config.online_min_threshold
            );
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if self.genesis_config.online_max_threshold > Rational32::from_integer(1) {
            let error_message = format!(
                "Online max threshold must be less or equal than 1, but current value is {}",
                self.genesis_config.online_max_threshold
            );
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if *self.genesis_config.online_max_threshold.numer() >= 10_000_000 {
            let error_message =
                format!("online_max_threshold's numerator is too large, may lead to overflow.");
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if *self.genesis_config.online_min_threshold.numer() >= 10_000_000 {
            let error_message =
                format!("online_min_threshold's numerator is too large, may lead to overflow.");
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if *self.genesis_config.online_max_threshold.denom() >= 10_000_000 {
            let error_message =
                format!("online_max_threshold's denominator is too large, may lead to overflow.");
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if *self.genesis_config.online_min_threshold.denom() >= 10_000_000 {
            let error_message =
                format!("online_min_threshold's denominator is too large, may lead to overflow.");
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if self.genesis_config.gas_price_adjustment_rate >= Rational32::from_integer(1) {
            let error_message = format!(
                "Gas price adjustment rate must be less than 1, value in config is {}",
                self.genesis_config.gas_price_adjustment_rate
            );
            self.validation_errors.push_genesis_semantics_error(error_message)
        }

        if self.genesis_config.epoch_length == 0 {
            let error_message = format!("Epoch Length must be greater than 0");
            self.validation_errors.push_genesis_semantics_error(error_message)
        }
    }

    fn result_with_full_error(&self) -> Result<(), ValidationError> {
        if self.validation_errors.is_empty() {
            Ok(())
        } else {
            let full_error = self.validation_errors.generate_error_message_per_type().unwrap();
            Err(ValidationError::GenesisSemanticsError { error_message: full_error })
        }
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
        let mut config = GenesisConfig::default();
        config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: VALID_ED25519_RISTRETTO_KEY.parse().unwrap(),
            amount: 10,
        }];
        let records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".parse().unwrap(),
            account: create_account(),
        }]);
        let genesis = &Genesis::new(config, records).unwrap();
        validate_genesis(genesis).unwrap();
    }

    #[test]
    #[should_panic(expected = "validator staking key is not valid")]
    fn test_invalid_staking_key() {
        let mut config = GenesisConfig::default();
        config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: PublicKey::empty(KeyType::ED25519),
            amount: 10,
        }];
        let records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".parse().unwrap(),
            account: create_account(),
        }]);
        let genesis = &Genesis::new(config, records).unwrap();
        validate_genesis(genesis).unwrap();
    }

    #[test]
    #[should_panic(expected = "Validator accounts do not match staked accounts")]
    fn test_validator_not_match() {
        let mut config = GenesisConfig::default();
        config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: VALID_ED25519_RISTRETTO_KEY.parse().unwrap(),
            amount: 100,
        }];
        config.total_supply = 110;
        let records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".parse().unwrap(),
            account: create_account(),
        }]);
        let genesis = &Genesis::new(config, records).unwrap();
        validate_genesis(genesis).unwrap();
    }

    #[test]
    #[should_panic(expected = "No validators in genesis")]
    fn test_empty_validator() {
        let config = GenesisConfig::default();
        let records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".parse().unwrap(),
            account: create_account(),
        }]);
        let genesis = &Genesis::new(config, records).unwrap();
        validate_genesis(genesis).unwrap();
    }

    #[test]
    #[should_panic(expected = "access key account test1 does not exist")]
    fn test_access_key_with_nonexistent_account() {
        let mut config = GenesisConfig::default();
        config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: VALID_ED25519_RISTRETTO_KEY.parse().unwrap(),
            amount: 10,
        }];
        config.total_supply = 110;
        let records = GenesisRecords(vec![
            StateRecord::Account { account_id: "test".parse().unwrap(), account: create_account() },
            StateRecord::AccessKey {
                account_id: "test1".parse().unwrap(),
                public_key: PublicKey::empty(KeyType::ED25519),
                access_key: AccessKey::full_access(),
            },
        ]);
        let genesis = &Genesis::new(config, records).unwrap();
        validate_genesis(genesis).unwrap();
    }

    #[test]
    #[should_panic(expected = "account test has more than one contract deployed")]
    fn test_more_than_one_contract() {
        let mut config = GenesisConfig::default();
        config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: VALID_ED25519_RISTRETTO_KEY.parse().unwrap(),
            amount: 10,
        }];
        config.total_supply = 110;
        let records = GenesisRecords(vec![
            StateRecord::Account { account_id: "test".parse().unwrap(), account: create_account() },
            StateRecord::Contract { account_id: "test".parse().unwrap(), code: [1, 2, 3].to_vec() },
            StateRecord::Contract {
                account_id: "test".parse().unwrap(),
                code: [1, 2, 3, 4].to_vec(),
            },
        ]);
        let genesis = &Genesis::new(config, records).unwrap();
        validate_genesis(genesis).unwrap();
    }
}
