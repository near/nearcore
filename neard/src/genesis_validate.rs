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
    for record in genesis.records.0.iter() {
        if let StateRecord::Account { account_id, account } = record {
            if account_ids.contains(account_id) {
                panic!("Duplicate account id {} in genesis records", account_id);
            }
            total_supply += account.locked + account.amount;
            account_ids.insert(account_id.clone());
            if account.locked > 0 {
                staked_accounts.insert(account_id.clone(), account.locked);
            }
        }
    }
    assert_eq!(total_supply, genesis.config.total_supply, "wrong total supply");
    assert_eq!(validators, staked_accounts, "validator accounts do not match staked accounts");
}

#[cfg(test)]
mod test {
    use crate::genesis_validate::validate_genesis;
    use near_chain_configs::{Genesis, GenesisRecords};
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::state_record::StateRecord;
    use near_primitives::types::AccountInfo;
    use near_primitives::views::AccountView;

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
            account: AccountView {
                amount: 100,
                locked: 10,
                code_hash: Default::default(),
                storage_usage: 0,
                storage_paid_at: 0,
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
            account: AccountView {
                amount: 100,
                locked: 10,
                code_hash: Default::default(),
                storage_usage: 0,
                storage_paid_at: 0,
            },
        }]);
        validate_genesis(&genesis);
    }

    #[test]
    #[should_panic(expected = "no validators in genesis")]
    fn test_empty_validator() {
        let mut genesis = Genesis::default();
        validate_genesis(&genesis);
    }
}
