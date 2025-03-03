use near_crypto::PublicKey;
use near_parameters::RuntimeConfig;
use near_primitives::account::{AccessKey, AccessKeyPermission, Account};
use near_primitives::action::Action;
use near_primitives::checked_feature;
use near_primitives::errors::{InvalidAccessKeyError, InvalidTxError};
use near_primitives::errors::{RuntimeError, StorageError};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::version::ProtocolVersion;
use near_store::{get_access_key, get_account, set_access_key, set_account, TrieUpdate};

use crate::config::TransactionCost;
use crate::verifier::{check_storage_stake, StorageStakingError};
use crate::VerificationResult;

/// EphemeralAccount = ephemeral copy of an `Account` + `AccessKey`.
/// We store the `signer_id` and `public_key` for easy commit later.
#[derive(Debug)]
pub struct EphemeralAccount {
    pub signer_id: AccountId,
    pub public_key: PublicKey,

    #[allow(dead_code)]
    pub original_account: Account,
    #[allow(dead_code)]
    pub original_access_key: AccessKey,

    pub ephemeral_account: Account,
    pub ephemeral_access_key: AccessKey,
}

impl EphemeralAccount {
    /// Create ephemeral state by reading from the real state.
    pub fn new(
        state_update: &mut TrieUpdate,
        signer_id: AccountId,
        public_key: PublicKey,
    ) -> Result<Self, RuntimeError> {
        let account = match get_account(state_update, &signer_id) {
            Ok(Some(acct)) => acct,
            Ok(None) => {
                return Err(RuntimeError::InvalidTxError(InvalidTxError::SignerDoesNotExist {
                    signer_id: signer_id.clone(),
                }));
            }
            Err(e) => {
                return Err(RuntimeError::StorageError(StorageError::StorageInconsistentState(
                    e.to_string(),
                )));
            }
        };

        let access_key = match get_access_key(state_update, &signer_id, &public_key) {
            Ok(Some(ak)) => ak,
            Ok(None) => {
                return Err(RuntimeError::InvalidTxError(InvalidTxError::InvalidAccessKeyError(
                    InvalidAccessKeyError::AccessKeyNotFound {
                        account_id: signer_id.clone(),
                        public_key: public_key.clone().into(),
                    },
                )));
            }
            Err(e) => {
                return Err(RuntimeError::StorageError(StorageError::StorageInconsistentState(
                    e.to_string(),
                )));
            }
        };

        Ok(Self {
            signer_id,
            public_key,
            original_account: account.clone(),
            original_access_key: access_key.clone(),
            ephemeral_account: account,
            ephemeral_access_key: access_key,
        })
    }

    /// Test-only helper that creates an ephemeral state given an account and access key.
    #[cfg(test)]
    pub fn new_for_test(
        account: Account,
        access_key: AccessKey,
        signer_id: AccountId,
        public_key: PublicKey,
    ) -> Self {
        Self {
            signer_id,
            public_key,
            original_account: account.clone(),
            original_access_key: access_key.clone(),
            ephemeral_account: account,
            ephemeral_access_key: access_key,
        }
    }

    /// Applies one transaction to the ephemeral state.
    /// If any check fails, reverts ephemeral changes and returns an error.
    /// On success, returns a VerificationResult.
    pub fn apply_transaction_ephemeral(
        &mut self,
        config: &RuntimeConfig,
        tx: &SignedTransaction,
        cost: &TransactionCost,
        block_height: Option<BlockHeight>,
        current_protocol_version: ProtocolVersion,
    ) -> Result<VerificationResult, InvalidTxError> {
        let old_account = self.ephemeral_account.clone();
        let old_key = self.ephemeral_access_key.clone();

        let mut guard = EphemeralTxGuard {
            ephemeral: self,
            old_account,
            old_access_key: old_key,
            success: false,
        };

        guard.check_transaction(config, tx, cost, block_height, current_protocol_version)?;

        guard.success = true;
        Ok(VerificationResult {
            gas_burnt: cost.gas_burnt,
            gas_remaining: cost.gas_remaining,
            receipt_gas_price: cost.receipt_gas_price,
            burnt_amount: cost.burnt_amount,
        })
    }

    /// Commits ephemeral changes into the `TrieUpdate`.
    pub fn commit(self, state_update: &mut TrieUpdate) {
        set_account(state_update, self.signer_id.clone(), &self.ephemeral_account);
        set_access_key(
            state_update,
            self.signer_id.clone(),
            self.public_key.clone(),
            &self.ephemeral_access_key,
        );
    }

    /// Reverts ephemeral changes to the original.
    fn revert_all(&mut self, old_account: Account, old_key: AccessKey) {
        self.ephemeral_account = old_account;
        self.ephemeral_access_key = old_key;
    }
}

/// A guard that reverts ephemeral changes if we never set success = true.
struct EphemeralTxGuard<'a> {
    ephemeral: &'a mut EphemeralAccount,
    old_account: Account,
    old_access_key: AccessKey,
    success: bool,
}

impl<'a> EphemeralTxGuard<'a> {
    fn check_transaction(
        &mut self,
        config: &RuntimeConfig,
        tx: &SignedTransaction,
        cost: &TransactionCost,
        block_height: Option<BlockHeight>,
        current_protocol_version: ProtocolVersion,
    ) -> Result<(), InvalidTxError> {
        let old_nonce = self.ephemeral.ephemeral_access_key.nonce;
        let tx_nonce = tx.transaction.nonce();
        if tx_nonce <= old_nonce {
            return Err(InvalidTxError::InvalidNonce { tx_nonce, ak_nonce: old_nonce });
        }
        if checked_feature!("stable", AccessKeyNonceRange, current_protocol_version) {
            if let Some(height) = block_height {
                let upper_bound =
                    height * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
                if tx_nonce >= upper_bound {
                    return Err(InvalidTxError::NonceTooLarge { tx_nonce, upper_bound });
                }
            }
        }
        self.ephemeral.ephemeral_access_key.nonce = tx_nonce;

        let old_balance = self.ephemeral.ephemeral_account.amount();
        let new_balance = old_balance.checked_sub(cost.total_cost).ok_or_else(|| {
            InvalidTxError::NotEnoughBalance {
                signer_id: tx.transaction.signer_id().clone(),
                balance: old_balance,
                cost: cost.total_cost,
            }
        })?;
        self.ephemeral.ephemeral_account.set_amount(new_balance);

        if let AccessKeyPermission::FunctionCall(fc_perm) =
            &mut self.ephemeral.ephemeral_access_key.permission
        {
            if let Some(old_allow) = fc_perm.allowance {
                let new_allow = old_allow.checked_sub(cost.total_cost).ok_or_else(|| {
                    InvalidTxError::InvalidAccessKeyError(
                        InvalidAccessKeyError::NotEnoughAllowance {
                            account_id: tx.transaction.signer_id().clone(),
                            public_key: tx.transaction.public_key().clone().into(),
                            allowance: old_allow,
                            cost: cost.total_cost,
                        },
                    )
                })?;
                fc_perm.allowance = Some(new_allow);
            }
        }

        match check_storage_stake(
            &self.ephemeral.ephemeral_account,
            config,
            current_protocol_version,
        ) {
            Ok(()) => {}
            Err(StorageStakingError::LackBalanceForStorageStaking(amount)) => {
                return Err(InvalidTxError::LackBalanceForState {
                    signer_id: tx.transaction.signer_id().clone(),
                    amount,
                });
            }
            Err(StorageStakingError::StorageError(e)) => {
                return Err(StorageError::StorageInconsistentState(e).into());
            }
        }

        if let AccessKeyPermission::FunctionCall(fc_perm) =
            &self.ephemeral.ephemeral_access_key.permission
        {
            let actions = tx.transaction.actions();
            if actions.len() != 1 {
                return Err(InvalidTxError::InvalidAccessKeyError(
                    InvalidAccessKeyError::RequiresFullAccess,
                ));
            }
            match &actions[0] {
                Action::FunctionCall(fc) => {
                    if fc.deposit > 0 {
                        return Err(InvalidTxError::InvalidAccessKeyError(
                            InvalidAccessKeyError::DepositWithFunctionCall,
                        ));
                    }
                    if tx.transaction.receiver_id() != &fc_perm.receiver_id {
                        return Err(InvalidTxError::InvalidAccessKeyError(
                            InvalidAccessKeyError::ReceiverMismatch {
                                tx_receiver: tx.transaction.receiver_id().clone(),
                                ak_receiver: fc_perm.receiver_id.clone(),
                            },
                        ));
                    }
                    if !fc_perm.method_names.is_empty()
                        && fc_perm.method_names.iter().all(|m| m != &fc.method_name)
                    {
                        return Err(InvalidTxError::InvalidAccessKeyError(
                            InvalidAccessKeyError::MethodNameMismatch {
                                method_name: fc.method_name.clone(),
                            },
                        ));
                    }
                }
                _ => {
                    return Err(InvalidTxError::InvalidAccessKeyError(
                        InvalidAccessKeyError::RequiresFullAccess,
                    ));
                }
            }
        }
        Ok(())
    }
}

impl<'a> Drop for EphemeralTxGuard<'a> {
    fn drop(&mut self) {
        if !self.success {
            self.ephemeral.revert_all(self.old_account.clone(), self.old_access_key.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TransactionCost;
    use near_crypto::{InMemorySigner, KeyType};
    use near_primitives::hash::CryptoHash;
    use near_primitives::version::PROTOCOL_VERSION;

    #[test]
    fn test_ephemeral_account_creation_for_test() {
        let signer_id: AccountId = "test.near".parse().unwrap();
        let pk =
            InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, "seed").public_key();
        let account = Account::new(1000, 0, near_primitives::account::AccountContract::None, 0);
        let access_key = AccessKey::full_access();
        let ephemeral = EphemeralAccount::new_for_test(account, access_key, signer_id, pk);
        assert_eq!(ephemeral.ephemeral_account.amount(), 1000);
    }

    #[test]
    fn test_apply_transaction_ephemeral_revert_insufficient_balance() {
        let signer_id: AccountId = "test.near".parse().unwrap();
        let pk =
            InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, "seed").public_key();
        let account = Account::new(10, 0, near_primitives::account::AccountContract::None, 0);
        let access_key = AccessKey::full_access();
        let mut ephemeral =
            EphemeralAccount::new_for_test(account, access_key, signer_id.clone(), pk.clone());

        let dummy_tx = SignedTransaction::from_actions(
            1,
            signer_id.clone(),
            "bob.near".parse().unwrap(),
            &InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, "seed"),
            vec![],
            CryptoHash::default(),
            0,
        );
        let cost = TransactionCost {
            gas_burnt: 1,
            gas_remaining: 10,
            receipt_gas_price: 100,
            total_cost: 9999,
            burnt_amount: 9999,
        };

        let res = ephemeral.apply_transaction_ephemeral(
            &RuntimeConfig::test(),
            &dummy_tx,
            &cost,
            Some(100),
            PROTOCOL_VERSION,
        );
        assert!(res.is_err(), "should fail due to insufficient balance");
        assert_eq!(ephemeral.ephemeral_account.amount(), 10);
    }

    #[test]
    fn test_apply_transaction_ephemeral_ok() {
        let signer_id: AccountId = "test2.near".parse().unwrap();
        let pk =
            InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, "seed").public_key();
        let account = Account::new(500, 0, near_primitives::account::AccountContract::None, 0);
        let access_key = AccessKey::full_access();
        let mut ephemeral =
            EphemeralAccount::new_for_test(account, access_key, signer_id.clone(), pk.clone());

        let dummy_tx = SignedTransaction::from_actions(
            1,
            signer_id.clone(),
            "bob.near".parse().unwrap(),
            &InMemorySigner::from_seed(signer_id.clone(), KeyType::ED25519, "seed"),
            vec![],
            CryptoHash::default(),
            0,
        );
        let cost = TransactionCost {
            gas_burnt: 10,
            gas_remaining: 5,
            receipt_gas_price: 100,
            total_cost: 50,
            burnt_amount: 50,
        };
        let res = ephemeral.apply_transaction_ephemeral(
            &RuntimeConfig::test(),
            &dummy_tx,
            &cost,
            None,
            PROTOCOL_VERSION,
        );
        assert!(res.is_ok(), "ephemeral check should succeed");
        assert_eq!(ephemeral.ephemeral_account.amount(), 450, "expected balance 500-50=450");
    }
}
