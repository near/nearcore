use near_crypto::PublicKey;
use near_parameters::RuntimeConfig;
use near_primitives::account::{AccessKey, Account};
use near_primitives::errors::{InvalidAccessKeyError, InvalidTxError};
use near_primitives::errors::{RuntimeError, StorageError};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::version::ProtocolVersion;
use near_store::{TrieUpdate, get_access_key, get_account, set_access_key, set_account};

use crate::config::TransactionCost;
use crate::{VerificationResult, verify_and_charge_tx_ephemeral};

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
        state_update: &TrieUpdate,
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
        state_update: &TrieUpdate,
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

        let vr = guard.check_transaction(
            config,
            state_update,
            tx,
            cost,
            block_height,
            current_protocol_version,
        )?;

        guard.success = true;
        Ok(vr)
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
        state_update: &TrieUpdate,
        tx: &SignedTransaction,
        cost: &TransactionCost,
        block_height: Option<BlockHeight>,
        current_protocol_version: ProtocolVersion,
    ) -> Result<VerificationResult, InvalidTxError> {
        let vr = verify_and_charge_tx_ephemeral(
            config,
            state_update,
            tx,
            cost,
            block_height,
            current_protocol_version,
        )?;
        self.ephemeral.ephemeral_account = vr.signer.clone();
        self.ephemeral.ephemeral_access_key = vr.access_key.clone();
        Ok(vr)
    }
}

impl<'a> Drop for EphemeralTxGuard<'a> {
    fn drop(&mut self) {
        if !self.success {
            self.ephemeral.revert_all(self.old_account.clone(), self.old_access_key.clone());
        }
    }
}
