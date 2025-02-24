use near_crypto::PublicKey;
use near_parameters::RuntimeConfig;
use near_primitives::account::{AccessKey, AccessKeyPermission, Account};
use near_primitives::action::Action;
use near_primitives::checked_feature;
use near_primitives::errors::InvalidAccessKeyError;
use near_primitives::errors::InvalidTxError;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::AccountId;
use near_primitives::types::BlockHeight;
use near_primitives::version::ProtocolVersion;
use near_store::trie::mem::memtries::FrozenMemTries;
use near_store::{
    get_access_key, get_account, set_access_key, set_account, StorageError, TrieUpdate,
};

use crate::config::TransactionCost;
use crate::verifier::{check_storage_stake, StorageStakingError};
use crate::VerificationResult;
/// EphemeralAccount holds a working copy of an Account and its AccessKey.
#[derive(Debug)]
pub struct EphemeralAccount {
    original_account: Account,
    original_access_key: AccessKey,

    pub ephemeral_account: Account,
    pub ephemeral_access_key: AccessKey,

    original_fc_allowance: Option<u128>,
}

impl EphemeralAccount {
    /// Create ephemeral state by reading from the real state.
    pub fn new(
        state_update: &mut TrieUpdate,
        signer_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Self, InvalidTxError> {
        let account = get_account(state_update, signer_id)?
            .ok_or_else(|| InvalidTxError::SignerDoesNotExist { signer_id: signer_id.clone() })?;
        let ak = get_access_key(state_update, signer_id, public_key)?.ok_or_else(|| {
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::AccessKeyNotFound {
                account_id: signer_id.clone(),
                public_key: public_key.clone().into(),
            })
        })?;
        let orig_allow = match &ak.permission {
            AccessKeyPermission::FunctionCall(fc) => fc.allowance,
            _ => None,
        };
        Ok(Self {
            original_account: account.clone(),
            original_access_key: ak.clone(),
            ephemeral_account: account,
            ephemeral_access_key: ak,
            original_fc_allowance: orig_allow,
        })
    }

    /// Create ephemeral state from a frozen snapshot.
    /// TODO(miloserdow): .get_account, .get_access_key are not yet implemented for FrozenMemTries.
    pub fn new_from_frozen(
        frozen: &FrozenMemTries,
        signer_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<Self, InvalidTxError> {
        let account = frozen
            .get_account(signer_id)?
            .ok_or_else(|| InvalidTxError::SignerDoesNotExist { signer_id: signer_id.clone() })?;
        let ak = frozen.get_access_key(signer_id, public_key)?.ok_or_else(|| {
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::AccessKeyNotFound {
                account_id: signer_id.clone(),
                public_key: public_key.clone().into(),
            })
        })?;
        let orig_allow = match &ak.permission {
            AccessKeyPermission::FunctionCall(fc) => fc.allowance,
            _ => None,
        };
        Ok(Self {
            original_account: account.clone(),
            original_access_key: ak.clone(),
            ephemeral_account: account,
            ephemeral_access_key: ak,
            original_fc_allowance: orig_allow,
        })
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
        let old_amount = self.ephemeral_account.amount();
        let old_nonce = self.ephemeral_access_key.nonce;
        let old_allowance = match &self.ephemeral_access_key.permission {
            AccessKeyPermission::FunctionCall(fc) => fc.allowance,
            _ => None,
        };

        let tx_nonce = tx.transaction.nonce();
        if tx_nonce <= self.ephemeral_access_key.nonce {
            self.revert(old_amount, old_nonce, old_allowance);
            return Err(InvalidTxError::InvalidNonce {
                tx_nonce,
                ak_nonce: self.ephemeral_access_key.nonce,
            }
            .into());
        }
        if checked_feature!("stable", AccessKeyNonceRange, current_protocol_version) {
            if let Some(height) = block_height {
                let upper_bound =
                    height * near_primitives::account::AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
                if tx_nonce >= upper_bound {
                    self.revert(old_amount, old_nonce, old_allowance);
                    return Err(InvalidTxError::NonceTooLarge { tx_nonce, upper_bound }.into());
                }
            }
        }
        self.ephemeral_access_key.nonce = tx_nonce;

        let new_amount =
            self.ephemeral_account.amount().checked_sub(cost.total_cost).ok_or_else(|| {
                self.revert(old_amount, old_nonce, old_allowance);
                InvalidTxError::NotEnoughBalance {
                    signer_id: tx.transaction.signer_id().clone(),
                    balance: self.ephemeral_account.amount(),
                    cost: cost.total_cost,
                }
            })?;
        self.ephemeral_account.set_amount(new_amount);

        {
            // Create a new inner block to drop the mutable borrow of fc_perm
            if let AccessKeyPermission::FunctionCall(fc_perm) =
                &mut self.ephemeral_access_key.permission
            {
                let current_allow = fc_perm.allowance.unwrap_or(0);
                if current_allow < cost.total_cost {
                    self.revert(old_amount, old_nonce, old_allowance);
                    return Err(InvalidTxError::InvalidAccessKeyError(
                        InvalidAccessKeyError::NotEnoughAllowance {
                            account_id: tx.transaction.signer_id().clone(),
                            public_key: tx.transaction.public_key().clone().into(),
                            allowance: current_allow,
                            cost: cost.total_cost,
                        },
                    )
                    .into());
                }
                fc_perm.allowance = Some(current_allow - cost.total_cost);
            }
        }

        match check_storage_stake(&self.ephemeral_account, config, current_protocol_version) {
            Ok(()) => {}
            Err(StorageStakingError::LackBalanceForStorageStaking(amount)) => {
                self.revert(old_amount, old_nonce, old_allowance);
                return Err(InvalidTxError::LackBalanceForState {
                    signer_id: tx.transaction.signer_id().clone(),
                    amount,
                }
                .into());
            }
            Err(StorageStakingError::StorageError(e)) => {
                self.revert(old_amount, old_nonce, old_allowance);
                return Err(StorageError::StorageInconsistentState(e).into());
            }
        }

        if let AccessKeyPermission::FunctionCall(fc_perm) = &self.ephemeral_access_key.permission {
            let actions = tx.transaction.actions();
            if actions.len() != 1 {
                self.revert(old_amount, old_nonce, old_allowance);
                return Err(InvalidTxError::InvalidAccessKeyError(
                    InvalidAccessKeyError::RequiresFullAccess,
                )
                .into());
            }
            match actions.get(0) {
                Some(Action::FunctionCall(fc)) => {
                    if fc.deposit > 0 {
                        self.revert(old_amount, old_nonce, old_allowance);
                        return Err(InvalidTxError::InvalidAccessKeyError(
                            InvalidAccessKeyError::DepositWithFunctionCall,
                        )
                        .into());
                    }
                    if tx.transaction.receiver_id() != &fc_perm.receiver_id {
                        self.revert(old_amount, old_nonce, old_allowance);
                        return Err(InvalidTxError::InvalidAccessKeyError(
                            InvalidAccessKeyError::ReceiverMismatch {
                                tx_receiver: tx.transaction.receiver_id().clone(),
                                ak_receiver: fc_perm.receiver_id.clone(),
                            },
                        )
                        .into());
                    }
                    if !fc_perm.method_names.is_empty()
                        && fc_perm.method_names.iter().all(|mn| &fc.method_name != mn)
                    {
                        self.revert(old_amount, old_nonce, old_allowance);
                        return Err(InvalidTxError::InvalidAccessKeyError(
                            InvalidAccessKeyError::MethodNameMismatch {
                                method_name: fc.method_name.clone(),
                            },
                        )
                        .into());
                    }
                }
                _ => {
                    self.revert(old_amount, old_nonce, old_allowance);
                    return Err(InvalidTxError::InvalidAccessKeyError(
                        InvalidAccessKeyError::RequiresFullAccess,
                    )
                    .into());
                }
            }
        }

        Ok(VerificationResult {
            gas_burnt: cost.gas_burnt,
            gas_remaining: cost.gas_remaining,
            receipt_gas_price: cost.receipt_gas_price,
            burnt_amount: cost.burnt_amount,
        })
    }

    fn revert(&mut self, old_amount: u128, old_nonce: u64, old_allowance: Option<u128>) {
        self.ephemeral_account.set_amount(old_amount);
        self.ephemeral_access_key.nonce = old_nonce;
        if let AccessKeyPermission::FunctionCall(fc) = &mut self.ephemeral_access_key.permission {
            fc.allowance = old_allowance;
        }
    }

    /// Commits the ephemeral state into the given real state update
    pub fn commit(
        self,
        state_update: &mut TrieUpdate,
        signer_id: &AccountId,
        public_key: &PublicKey,
    ) {
        set_account(state_update, signer_id.clone(), &self.ephemeral_account);
        set_access_key(
            state_update,
            signer_id.clone(),
            public_key.clone(),
            &self.ephemeral_access_key,
        );
    }
}
