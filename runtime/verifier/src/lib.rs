use near_primitives::account::{AccessKey, Account};
use near_primitives::crypto::signature::{verify, PublicKey};
use near_primitives::logging;
use near_primitives::transaction::{Action, SignedTransaction};
use near_primitives::types::AccountId;
use near_primitives::utils::is_valid_account_id;
use near_store::{get_access_key, get_account, TrieUpdate};

pub struct VerificationData {
    pub signer_id: AccountId,
    pub signer: Account,
    pub public_key: PublicKey,
    pub access_key: Option<AccessKey>,
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
        let signer = get_account(self.state_update, signer_id);
        match signer {
            Some(signer) => {
                if transaction.nonce <= signer.nonce {
                    return Err(format!(
                        "Transaction nonce {} must be larger than originator's nonce {}",
                        transaction.nonce, signer.nonce,
                    ));
                }

                if !is_valid_account_id(&transaction.receiver_id) {
                    return Err(format!(
                        "Invalid receiver account ID {:?} according to requirements",
                        transaction.receiver_id
                    ));
                }

                let hash = signed_transaction.get_hash();
                let hash = hash.as_ref();
                let public_key = signer
                    .public_keys
                    .iter()
                    .find(|key| verify(&hash, &signed_transaction.signature, &key))
                    .cloned();

                if let Some(public_key) = public_key {
                    if &public_key != &transaction.public_key {
                        return Err("Transaction is signed with different public key than given"
                            .to_string());
                    }
                    Ok(VerificationData {
                        signer_id: signer_id.clone(),
                        signer,
                        public_key,
                        access_key: None,
                    })
                } else {
                    let access_key =
                        get_access_key(self.state_update, &signer_id, &transaction.public_key);
                    if let Some(access_key) = access_key {
                        if transaction.actions.len() != 1 {
                            return Err(
                                "Transaction has more than 1 actions and is using access key"
                                    .to_string(),
                            );
                        }
                        if let Some(Action::FunctionCall(ref function_call)) =
                            transaction.actions.get(0)
                        {
                            let access_contract_id =
                                access_key.contract_id.as_ref().unwrap_or(&signer_id);

                            if &transaction.receiver_id != access_contract_id {
                                return Err(format!(
                                    "Access key contract ID {:?} doesn't match TX receiver account ID {:?}",
                                    access_contract_id,
                                    &transaction.receiver_id,
                                ));
                            }
                            if let Some(ref access_method_name) = access_key.method_name {
                                let access_method_name_str =
                                    std::str::from_utf8(access_method_name).map_err(|_| {
                                        "Can't convert access_key.method_name to utf8 string"
                                            .to_string()
                                    })?;
                                if &function_call.method_name != access_method_name_str {
                                    return Err(format!(
                                        "Transaction method name `{}` doesn't match the access key method name {:?}",
                                        &function_call.method_name,
                                        logging::pretty_utf8(access_method_name),
                                    ));
                                }
                            }
                            Ok(VerificationData {
                                signer_id: signer_id.clone(),
                                signer,
                                public_key: transaction.public_key.clone(),
                                access_key: Some(access_key),
                            })
                        } else {
                            Err("Access key can only be used with the exactly one FunctionCall action"
                                .to_string())
                        }
                    } else {
                        Err(format!(
                            "Transaction is not signed with a public key of the signer {:?}",
                            signer_id,
                        ))
                    }
                }
            }
            _ => Err(format!("Signer {:?} does not exist", signer_id)),
        }
    }
}
