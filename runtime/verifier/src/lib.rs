use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, AccessKeyPermission, Account};
use near_primitives::errors::{InvalidAccessKeyError, InvalidTxError};
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::AccountId;
use near_primitives::utils::is_valid_account_id;
use near_store::{get_access_key, get_account, TrieUpdate};

pub struct VerificationData {
    pub signer_id: AccountId,
    pub signer: Account,
    pub public_key: PublicKey,
    pub access_key: AccessKey,
}

pub struct TransactionVerifier<'a> {
    state_update: &'a TrieUpdate,
}

impl<'a> TransactionVerifier<'a> {
    pub fn new(state_update: &'a TrieUpdate) -> Self {
        TransactionVerifier { state_update }
    }

    pub fn verify_transaction(
        &self,
        signed_transaction: &SignedTransaction,
    ) -> Result<VerificationData, InvalidTxError> {
        let transaction = &signed_transaction.transaction;
        let signer_id = &transaction.signer_id;
        if !is_valid_account_id(&signer_id) {
            return Err(InvalidTxError::InvalidSigner(signer_id));
        }
        let signer = match get_account(self.state_update, signer_id)? {
            Some(signer) => signer,
            None => {
                return Err(InvalidTxError::SignerDoesNotExist(signer_id));
            }
        };
        let access_key =
            match get_access_key(self.state_update, &signer_id, &transaction.public_key)? {
                Some(access_key) => access_key,
                None => {
                    return Err(InvalidAccessKeyError::AccessKeyNotFound(
                        signer_id,
                        transaction.public_key,
                    )
                    .into());
                }
            };

        if transaction.nonce <= access_key.nonce {
            return Err(InvalidTxError::InvalidNonce(transaction.nonce, access_key.nonce));
        }

        if !is_valid_account_id(&transaction.receiver_id) {
            return Err(InvalidTxError::InvalidReceiver(transaction.receiver_id));
        }

        let hash = signed_transaction.get_hash();
        if !signed_transaction.signature.verify(hash.as_ref(), &transaction.public_key) {
            return Err(InvalidTxError::InvalidSignature);
        }

        // TODO: Calculate transaction cost

        match access_key.permission {
            AccessKeyPermission::FullAccess => Ok(VerificationData {
                signer_id: signer_id.clone(),
                signer,
                public_key: transaction.public_key.clone(),
                access_key,
            }),
            AccessKeyPermission::FunctionCall(ref function_call_permission) => {
                if transaction.actions.len() != 1 {
                    return Err(InvalidAccessKeyError::ActionError.into());
                }
                if let Some(Action::FunctionCall(ref function_call)) = transaction.actions.get(0) {
                    if transaction.receiver_id != function_call_permission.receiver_id {
                        return Err(InvalidAccessKeyError::ReceiverMismatch(
                            transaction.receiver_id,
                            function_call_permission.receiver_id,
                        )
                        .into());
                    }
                    if !function_call_permission.method_names.is_empty() {
                        if function_call_permission
                            .method_names
                            .iter()
                            .find(|method_name| &function_call.method_name == *method_name)
                            .is_none()
                        {
                            return Err(InvalidAccessKeyError::MethodNameMismatch(
                                function_call.method_name,
                            )
                            .into());
                        }
                    }
                    Ok(VerificationData {
                        signer_id: signer_id.clone(),
                        signer,
                        public_key: transaction.public_key.clone(),
                        access_key,
                    })
                } else {
                    Err(InvalidAccessKeyError::ActionError.into())
                }
            }
        }
    }
}
