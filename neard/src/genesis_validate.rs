use near_chain_configs::Genesis;
use near_primitives::state_record::StateRecord;
use std::collections::{HashMap, HashSet};

/// Validate genesis config and records. Panics if genesis is ill-formed.
pub fn validate_genesis(genesis: &Genesis) {
    let validators = genesis
        .config
        .validators
        .clone()
        .into_iter()
        .map(|account_info| (account_info.account_id, account_info.amount))
        .collect::<HashMap<_, _>>();
    assert_eq!(
        validators.len(),
        genesis.config.validators.len(),
        "Duplicate account in validators"
    );
    assert!(!validators.is_empty(), "no validators in genesis");

    let mut total_supply = 0;
    let mut staked_accounts = HashMap::new();
    let mut account_ids = HashSet::new();
    let mut access_key_account_ids = HashSet::new();
    let mut contract_account_ids = HashSet::new();
    for record in genesis.records.0.iter() {
        match record {
            StateRecord::Account { account_id, account } => {
                if account_ids.contains(account_id) {
                    panic!("Duplicate account id {} in genesis records", account_id);
                }
                total_supply += account.locked + account.amount;
                account_ids.insert(account_id.clone());
                if account.locked > 0 {
                    staked_accounts.insert(account_id.clone(), account.locked);
                }
            }
            StateRecord::AccessKey { account_id, .. } => {
                access_key_account_ids.insert(account_id.clone());
            }
            StateRecord::Contract { account_id, .. } => {
                if contract_account_ids.contains(account_id) {
                    panic!("account {} has more than one contract deployed", account_id);
                }
                contract_account_ids.insert(account_id.clone());
            }
            _ => {}
        }
    }
    assert_eq!(total_supply, genesis.config.total_supply, "wrong total supply");
    assert_eq!(validators, staked_accounts, "validator accounts do not match staked accounts");
    for account_id in access_key_account_ids {
        assert!(
            account_ids.contains(&account_id),
            "access key account {} does not exist",
            account_id
        );
    }
    for account_id in contract_account_ids {
        assert!(
            account_ids.contains(&account_id),
            "contract account {} does not exist",
            account_id
        );
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use near_chain_configs::GenesisRecords;
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::account::{AccessKey, Account};
    use near_primitives::types::AccountInfo;

    #[test]
    #[should_panic(expected = "wrong total supply")]
    fn test_total_supply_not_match() {
        let mut genesis = Genesis::default();
        genesis.config.validators = vec![AccountInfo {
            account_id: "test".to_string(),
            public_key: PublicKey::empty(KeyType::ED25519),
            amount: 10,
        }];
        genesis.records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".to_string(),
            account: Account {
                amount: 100,
                locked: 10,
                code_hash: Default::default(),
                storage_usage: 0,
            },
        }]);
        validate_genesis(&genesis);
    }

    #[test]
    #[should_panic(expected = "validator accounts do not match staked accounts")]
    fn test_validator_not_match() {
        let mut genesis = Genesis::default();
        genesis.config.validators = vec![AccountInfo {
            account_id: "test".to_string(),
            public_key: PublicKey::empty(KeyType::ED25519),
            amount: 100,
        }];
        genesis.config.total_supply = 110;
        genesis.records = GenesisRecords(vec![StateRecord::Account {
            account_id: "test".to_string(),
            account: Account {
                amount: 100,
                locked: 10,
                code_hash: Default::default(),
                storage_usage: 0,
            },
        }]);
        validate_genesis(&genesis);
    }

    #[test]
    #[should_panic(expected = "no validators in genesis")]
    fn test_empty_validator() {
        let genesis = Genesis::default();
        validate_genesis(&genesis);
    }

    #[test]
    #[should_panic(expected = "access key account test1 does not exist")]
    fn test_access_key_with_nonexistent_account() {
        let mut genesis = Genesis::default();
        genesis.config.validators = vec![AccountInfo {
            account_id: "test".to_string(),
            public_key: PublicKey::empty(KeyType::ED25519),
            amount: 10,
        }];
        genesis.config.total_supply = 110;
        genesis.records = GenesisRecords(vec![
            StateRecord::Account {
                account_id: "test".to_string(),
                account: Account {
                    amount: 100,
                    locked: 10,
                    code_hash: Default::default(),
                    storage_usage: 0,
                },
            },
            StateRecord::AccessKey {
                account_id: "test1".to_string(),
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
            account_id: "test".to_string(),
            public_key: PublicKey::empty(KeyType::ED25519),
            amount: 10,
        }];
        genesis.config.total_supply = 110;
        genesis.records = GenesisRecords(vec![
            StateRecord::Account {
                account_id: "test".to_string(),
                account: Account {
                    amount: 100,
                    locked: 10,
                    code_hash: Default::default(),
                    storage_usage: 0,
                },
            },
            StateRecord::Contract { account_id: "test".to_string(), code: [1, 2, 3].to_vec() },
            StateRecord::Contract { account_id: "test".to_string(), code: [1, 2, 3, 4].to_vec() },
        ]);
        validate_genesis(&genesis);
    }
}
