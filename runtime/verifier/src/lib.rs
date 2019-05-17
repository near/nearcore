use primitives::account::{AccessKey, Account};
use primitives::crypto::signature::{verify, PublicKey};
use primitives::logging;
use primitives::transaction::{SignedTransaction, TransactionBody};
use primitives::types::AccountId;
use primitives::utils::{is_valid_account_id, key_for_access_key, key_for_account};
use storage::{get, TrieUpdate};

pub struct VerificationData {
    pub originator_id: AccountId,
    pub originator: Account,
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
        transaction: &SignedTransaction,
    ) -> Result<VerificationData, String> {
        let originator_id = transaction.body.get_originator();
        let originator: Option<Account> = get(self.state_update, &key_for_account(&originator_id));
        match originator {
            Some(originator) => {
                if transaction.body.get_nonce() <= originator.nonce {
                    return Err(format!(
                        "Transaction nonce {} must be larger than originator's nonce {}",
                        transaction.body.get_nonce(),
                        originator.nonce,
                    ));
                }

                let contract_id = transaction.body.get_contract_id();
                if let Some(ref contract_id) = contract_id {
                    if !is_valid_account_id(&contract_id) {
                        return Err(format!(
                            "Invalid contract_id / receiver {} according to requirements",
                            contract_id
                        ));
                    }
                }

                let hash = transaction.get_hash();
                let hash = hash.as_ref();
                let public_key = originator
                    .public_keys
                    .iter()
                    .find(|key| verify(&hash, &transaction.signature, &key))
                    .cloned();

                if let Some(public_key) = public_key {
                    if let Some(ref tx_public_key) = transaction.public_key {
                        if &public_key != tx_public_key {
                            return Err(
                                "Transaction is signed with different public key than given"
                                    .to_string(),
                            );
                        }
                    }
                    Ok(VerificationData { originator_id, originator, public_key, access_key: None })
                } else {
                    if let Some(ref public_key) = transaction.public_key {
                        let access_key: Option<AccessKey> = get(
                            self.state_update,
                            &key_for_access_key(&originator_id, &public_key),
                        );
                        if let Some(access_key) = access_key {
                            if let TransactionBody::FunctionCall(ref function_call) =
                                transaction.body
                            {
                                let access_contract_id =
                                    access_key.contract_id.as_ref().unwrap_or(&originator_id);

                                if &function_call.contract_id != access_contract_id {
                                    return Err(format!(
                                        "Access key contract ID {:?} doesn't match TX contract account ID {:?}",
                                        access_contract_id,
                                        function_call.contract_id,
                                    ));
                                }
                                if let Some(ref access_method_name) = access_key.method_name {
                                    if &function_call.method_name != access_method_name {
                                        return Err(format!(
                                            "Transaction method name {:?} doesn't match the access key method name {:?}",
                                            logging::pretty_utf8(&function_call.method_name),
                                            logging::pretty_utf8(access_method_name),
                                        ));
                                    }
                                }
                                Ok(VerificationData {
                                    originator_id,
                                    originator,
                                    public_key: *public_key,
                                    access_key: Some(access_key),
                                })
                            } else {
                                Err("Access key can only be used with the FunctionCall transactions".to_string())
                            }
                        } else {
                            Err(format!(
                                "Transaction is not signed with a public key of the originator {:?}",
                                originator_id,
                            ))
                        }
                    } else {
                        Err(format!(
                            "Transaction is not signed with a public key of the originator {:?}",
                            originator_id,
                        ))
                    }
                }
            }
            _ => Err(format!("Originator {:?} does not exist", originator_id)),
        }
    }
}
