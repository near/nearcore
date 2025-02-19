use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use rayon::prelude::*;

use near_primitives::account::{
    AccessKey, AccessKeyPermission, Account, AccountContract, FunctionCallPermission,
};
use near_primitives::errors::InvalidAccessKeyError;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::{AccountId, Balance, StorageUsage};

#[allow(dead_code)]
fn dummy_public_key() -> near_crypto::PublicKey {
    near_crypto::PublicKey::ED25519(near_crypto::ED25519PublicKey([0u8; 32]))
}

/// Represents ephemeral changes to a single access key. We store:
/// The struct is written back to storage if needed (via a final "commit" step).
#[derive(Debug, Clone)]
pub struct EphemeralAccessKeyState {
    /// The original key loaded from storage.
    pub original_key: AccessKey,
    /// The final nonce after ephemeral updates.
    pub final_nonce: u64,
    /// If it's a function-call key, the final allowance. For full-access keys
    /// we keep `None`.
    pub final_allowance: Option<Balance>,
}

impl EphemeralAccessKeyState {
    /// Creates a new ephemeral state from an existing access key.
    pub fn new_from_key(original_key: AccessKey) -> Self {
        let final_nonce = original_key.nonce;
        let final_allowance = match &original_key.permission {
            AccessKeyPermission::FunctionCall(fc_perm) => fc_perm.allowance,
            AccessKeyPermission::FullAccess => None,
        };
        Self { original_key, final_nonce, final_allowance }
    }

    /// Bumps the nonce to the provided value if it’s higher.
    pub fn bump_nonce_to(&mut self, nonce: u64) {
        if nonce > self.final_nonce {
            self.final_nonce = nonce;
        }
    }

    /// Updates ephemeral allowance, if it’s a function-call key.
    pub fn subtract_allowance(&mut self, amount: Balance) -> Result<(), InvalidAccessKeyError> {
        match &mut self.original_key.permission {
            AccessKeyPermission::FullAccess => {
                // No allowance to subtract.
                Ok(())
            }
            AccessKeyPermission::FunctionCall(fc_perm) => {
                let old_allow = fc_perm.allowance.unwrap_or_default();
                if old_allow < amount {
                    return Err(InvalidAccessKeyError::NotEnoughAllowance {
                        // TODO(miloserdow): replace placeholder here
                        account_id: "placeholder.test".parse().unwrap(),
                        public_key: dummy_public_key().into(),
                        allowance: old_allow,
                        cost: amount,
                    });
                }
                let new_allow = old_allow - amount;
                self.final_allowance = Some(new_allow);
                Ok(())
            }
        }
    }

    /// Converts ephemeral data back into a real `AccessKey` for final write to storage.
    pub fn into_updated_key(mut self) -> AccessKey {
        self.original_key.nonce = self.final_nonce;
        if let AccessKeyPermission::FunctionCall(ref mut fc) = self.original_key.permission {
            fc.allowance = self.final_allowance;
        }
        self.original_key
    }
}

/// Holds ephemeral changes for a single account.
#[derive(Debug, Clone)]
pub struct EphemeralAccountView {
    pub account_id: AccountId,

    /// The original account loaded from storage
    pub original_account: Account,

    pub final_amount: Balance,
    pub final_locked: Balance,

    /// The final storage usage after ephemeral changes
    pub final_storage_usage: StorageUsage,

    /// Keyed by the hashed public key
    pub ephemeral_keys: BTreeMap<CryptoHash, EphemeralAccessKeyState>,
}

impl EphemeralAccountView {
    /// Creates a new ephemeral view given an already loaded `Account`.
    pub fn new(account_id: AccountId, original_account: Account) -> Self {
        let final_amount = original_account.amount();
        let final_locked = original_account.locked();
        let final_storage_usage = original_account.storage_usage();
        Self {
            account_id,
            original_account,
            final_amount,
            final_locked,
            final_storage_usage,
            ephemeral_keys: BTreeMap::new(),
        }
    }

    /// Returns a mutable reference to the ephemeral key state, creating if necessary.
    /// `public_key_bytes` is typically `public_key.as_bytes()`.
    pub fn get_or_create_key_state(
        &mut self,
        public_key_bytes: &[u8],
        original_key: AccessKey,
    ) -> &mut EphemeralAccessKeyState {
        let h = hash(public_key_bytes);
        self.ephemeral_keys
            .entry(h)
            .or_insert_with(|| EphemeralAccessKeyState::new_from_key(original_key))
    }

    /// Subtracts from ephemeral `final_amount`, returning `false` if there's insufficient funds.
    pub fn subtract_amount(&mut self, amount: Balance) -> bool {
        if self.final_amount >= amount {
            self.final_amount -= amount;
            true
        } else {
            false
        }
    }

    /// Converts ephemeral changes back into an updated `Account`.
    pub fn into_updated_account(self) -> Account {
        let mut updated = self.original_account;
        updated.set_amount(self.final_amount);
        updated.set_locked(self.final_locked);
        updated.set_storage_usage(self.final_storage_usage);
        updated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn test_aid(s: &str) -> AccountId {
        AccountId::from_str(s).unwrap()
    }

    fn test_account(amount: Balance, locked: Balance, storage_usage: u64) -> Account {
        let contract = AccountContract::None;
        Account::new(amount, locked, contract, storage_usage)
    }

    fn test_access_key(fc_allow: Option<Balance>) -> AccessKey {
        let permission = if let Some(allow) = fc_allow {
            AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: Some(allow),
                receiver_id: "some-contract.test".to_string(),
                method_names: vec!["foo".to_string()],
            })
        } else {
            AccessKeyPermission::FullAccess
        };
        AccessKey { nonce: 100, permission }
    }

    #[test]
    fn test_ephemeral_account_view() {
        let account_id = test_aid("alice.test");
        let original_account = test_account(2000, 10, 500);
        let mut ephemeral_view = EphemeralAccountView::new(account_id.clone(), original_account);

        let original_key = test_access_key(Some(1000));
        let ephemeral_key_state =
            ephemeral_view.get_or_create_key_state(b"mypublickey", original_key);

        ephemeral_key_state.bump_nonce_to(102);
        ephemeral_key_state.subtract_allowance(100).unwrap();

        let sub_ok = ephemeral_view.subtract_amount(700);
        assert!(sub_ok, "We had enough to subtract 700 from 2000");
        let sub_ok2 = ephemeral_view.subtract_amount(2000);
        assert!(!sub_ok2, "No longer enough to subtract 2000 now");

        let updated_account = ephemeral_view.clone().into_updated_account();
        assert_eq!(updated_account.amount(), 1300);
        assert_eq!(updated_account.locked(), 10);
        assert_eq!(updated_account.storage_usage(), 500);

        let ephemeral_key_state_cloned =
            ephemeral_view.ephemeral_keys.values().next().unwrap().clone();
        let updated_key = ephemeral_key_state_cloned.into_updated_key();
        assert_eq!(updated_key.nonce, 102);
        match updated_key.permission {
            AccessKeyPermission::FunctionCall(fc) => {
                assert_eq!(fc.allowance, Some(900));
                assert_eq!(fc.receiver_id, "some-contract.test");
            }
            _ => panic!("Expected a function-call permission"),
        }
    }

    #[test]
    fn test_full_access_key_ephemeral() {
        let account_id = test_aid("bob.test");
        let original_account = test_account(100, 0, 50);
        let mut ephemeral_view = EphemeralAccountView::new(account_id, original_account);

        let original_key = test_access_key(None);
        let ephemeral_key_state =
            ephemeral_view.get_or_create_key_state(b"anotherkey", original_key);

        ephemeral_key_state.bump_nonce_to(999);
        ephemeral_key_state.subtract_allowance(500).unwrap();

        let updated_key = ephemeral_key_state.clone().into_updated_key();
        assert_eq!(updated_key.nonce, 999);
        match updated_key.permission {
            AccessKeyPermission::FullAccess => {}
            _ => panic!("expected FullAccess"),
        }
    }

    #[derive(Debug, Clone)]
    struct MockTx {
        signer_id: AccountId,
        nonce: u64,
        transfer_amount: Balance,
    }

    #[derive(Debug)]
    struct MockVerificationResult {
        valid: bool,
        reason: Option<String>,
        ephemeral_nonce: u64,
    }

    /// Simulate verifying a single transaction using ephemeral state.
    /// If ephemeral balance is below transfer_amount, we mark fail, else subtract.
    fn verify_tx_on_ephemeral(
        ephemeral_accounts: &mut HashMap<AccountId, EphemeralAccountView>,
        tx: &MockTx,
    ) -> MockVerificationResult {
        let ephemeral = ephemeral_accounts.get_mut(&tx.signer_id);
        if ephemeral.is_none() {
            return MockVerificationResult {
                valid: false,
                reason: Some("Signer doesn't exist".into()),
                ephemeral_nonce: tx.nonce,
            };
        }
        let ephemeral = ephemeral.unwrap();

        if tx.nonce <= 100 {
            return MockVerificationResult {
                valid: false,
                reason: Some(format!("Nonce {} is <= 100", tx.nonce)),
                ephemeral_nonce: 100,
            };
        }

        // Subtract ephemeral balance
        let enough = ephemeral.subtract_amount(tx.transfer_amount);
        if !enough {
            return MockVerificationResult {
                valid: false,
                reason: Some("Not enough balance".into()),
                ephemeral_nonce: tx.nonce,
            };
        }

        // Mark success
        MockVerificationResult { valid: true, reason: None, ephemeral_nonce: tx.nonce }
    }

    #[test]
    fn test_ephemeral_seq_simulation() {
        let mut ephemeral_accounts = HashMap::new();
        ephemeral_accounts.insert(
            test_aid("alice.test"),
            EphemeralAccountView::new(test_aid("alice.test"), test_account(20_000, 0, 500)),
        );
        ephemeral_accounts.insert(
            test_aid("bob.test"),
            EphemeralAccountView::new(test_aid("bob.test"), test_account(10_000, 0, 100)),
        );

        let txs = vec![
            MockTx { signer_id: test_aid("alice.test"), nonce: 101, transfer_amount: 5000 },
            MockTx { signer_id: test_aid("bob.test"), nonce: 101, transfer_amount: 8000 },
            MockTx { signer_id: test_aid("bob.test"), nonce: 102, transfer_amount: 5000 },
            MockTx { signer_id: test_aid("alice.test"), nonce: 102, transfer_amount: 12000 },
        ];

        let mut results = Vec::new();
        for tx in &txs {
            let r = verify_tx_on_ephemeral(&mut ephemeral_accounts, tx);
            results.push((tx.clone(), r));
        }

        // Tx1 => success => leftover alice=15,000
        // Tx2 => success => leftover bob=2,000
        // Tx3 => fail => leftover bob=2,000
        // Tx4 => success => leftover alice=3,000

        assert_eq!(results[0].1.valid, true); // first is success
        assert_eq!(results[1].1.valid, true); // second is success
        assert_eq!(results[2].1.valid, false); // third is fail
        assert_eq!(results[3].1.valid, true); // fourth is success

        let alice = ephemeral_accounts.get(&test_aid("alice.test")).unwrap();
        assert_eq!(alice.final_amount, 3000);

        let bob = ephemeral_accounts.get(&test_aid("bob.test")).unwrap();
        // after success(8k) => leftover 2k, then fail(5k) => leftover still 2k
        assert_eq!(bob.final_amount, 2000);
    }

    #[test]
    fn test_ephemeral_parallel_simulation() {
        let account_ids = vec![
            test_aid("alice.test"),
            test_aid("bob.test"),
            test_aid("carol.test"),
            test_aid("dave.test"),
        ];
        let mut ephemeral_accounts = HashMap::new();
        ephemeral_accounts.insert(
            account_ids[0].clone(),
            EphemeralAccountView::new(account_ids[0].clone(), test_account(15_000, 0, 500)),
        );
        ephemeral_accounts.insert(
            account_ids[1].clone(),
            EphemeralAccountView::new(account_ids[1].clone(), test_account(5_000, 0, 100)),
        );
        ephemeral_accounts.insert(
            account_ids[2].clone(),
            EphemeralAccountView::new(account_ids[2].clone(), test_account(20_000, 0, 1000)),
        );
        ephemeral_accounts.insert(
            account_ids[3].clone(),
            EphemeralAccountView::new(account_ids[3].clone(), test_account(1_000, 0, 100)),
        );

        let txs: Vec<MockTx> = vec![
            MockTx { signer_id: account_ids[0].clone(), nonce: 101, transfer_amount: 4000 },
            MockTx { signer_id: account_ids[1].clone(), nonce: 101, transfer_amount: 2000 },
            MockTx { signer_id: account_ids[2].clone(), nonce: 101, transfer_amount: 5000 },
            MockTx { signer_id: account_ids[3].clone(), nonce: 101, transfer_amount: 300 },
        ];

        let ephemeral_map_arc = Arc::new(Mutex::new(ephemeral_accounts));
        let results: Vec<(MockTx, MockVerificationResult)> = txs
            .into_par_iter()
            .map(|tx| {
                let mut locked = ephemeral_map_arc.lock().unwrap();
                let r = verify_tx_on_ephemeral(&mut locked, &tx);
                (tx, r)
            })
            .collect();

        let ephemeral_accounts = Arc::try_unwrap(ephemeral_map_arc)
            .expect("Arc still has references")
            .into_inner()
            .unwrap();

        // - alice: 15_000 => -4,000 => 11_000 leftover
        // - bob: 5_000 => -2,000 => 3,000 leftover
        // - carol: 20_000 => -5,000 => 15,000 leftover
        // - dave: 1,000 => -300 => 700 leftover
        for (_tx, r) in &results {
            assert!(r.valid, "All should succeed in this scenario");
        }

        let alice_leftover = ephemeral_accounts.get(&account_ids[0]).unwrap().final_amount;
        assert_eq!(alice_leftover, 11000);
        let bob_leftover = ephemeral_accounts.get(&account_ids[1]).unwrap().final_amount;
        assert_eq!(bob_leftover, 3000);
        let carol_leftover = ephemeral_accounts.get(&account_ids[2]).unwrap().final_amount;
        assert_eq!(carol_leftover, 15000);
        let dave_leftover = ephemeral_accounts.get(&account_ids[3]).unwrap().final_amount;
        assert_eq!(dave_leftover, 700);
    }
}
