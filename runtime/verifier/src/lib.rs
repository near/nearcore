use near_primitives::account::{AccessKey, AccessKeyPermission, Account};
use near_primitives::crypto::signature::{verify, PublicKey};
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
    ) -> Result<VerificationData, String> {
        let transaction = &signed_transaction.transaction;
        let signer_id = &transaction.signer_id;
        if !is_valid_account_id(&signer_id) {
            return Err(format!(
                "Invalid signer account ID {:?} according to requirements",
                signer_id
            ));
        }
        let signer = match get_account(self.state_update, signer_id) {
            Some(signer) => signer,
            None => {
                return Err(format!("Signer {:?} does not exist", signer_id));
            }
        };
        let access_key =
            match get_access_key(self.state_update, &signer_id, &transaction.public_key) {
                Some(access_key) => access_key,
                None => {
                    return Err(format!(
                        "Signer {:?} doesn't have access key with the given public_key {:?}",
                        signer_id, &transaction.public_key,
                    ));
                }
            };

        if transaction.nonce <= access_key.nonce {
            return Err(format!(
                "Transaction nonce {} must be larger than nonce of the used access key {}",
                transaction.nonce, access_key.nonce,
            ));
        }

        if !is_valid_account_id(&transaction.receiver_id) {
            return Err(format!(
                "Invalid receiver account ID {:?} according to requirements",
                transaction.receiver_id
            ));
        }

        let hash = signed_transaction.get_hash();
        if !verify(hash.as_ref(), &signed_transaction.signature, &transaction.public_key) {
            return Err(format!(
                "Transaction is not signed with a public key of the signer {:?}",
                signer_id,
            ));
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
                    return Err(
                        "Transaction has more than 1 actions and is using function call access key"
                            .to_string(),
                    );
                }
                if let Some(Action::FunctionCall(ref function_call)) = transaction.actions.get(0) {
                    if transaction.receiver_id != function_call_permission.receiver_id {
                        return Err(format!(
                            "Transaction receiver_id {:?} doesn't match the access key receiver_id {:?}",
                            &transaction.receiver_id,
                            &function_call_permission.receiver_id,
                        ));
                    }
                    if !function_call_permission.method_names.is_empty() {
                        if function_call_permission
                            .method_names
                            .iter()
                            .find(|method_name| &function_call.method_name == *method_name)
                            .is_none()
                        {
                            return Err(format!(
                                "Transaction method name {:?} isn't allowed by the access key",
                                &function_call.method_name
                            ));
                        }
                    }
                    Ok(VerificationData {
                        signer_id: signer_id.clone(),
                        signer,
                        public_key: transaction.public_key.clone(),
                        access_key,
                    })
                } else {
                    Err("The used access key requires exactly one FunctionCall action".to_string())
                }
            }
        }
    }
}
