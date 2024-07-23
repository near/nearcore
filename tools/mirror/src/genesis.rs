use near_crypto::PublicKey;
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum};
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::{Action, AddKeyAction};
use near_primitives_core::account::id::AccountType;
use near_primitives_core::account::{AccessKey, AccessKeyPermission};
use serde::ser::{SerializeSeq, Serializer};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

// map all the account IDs and keys in this receipt and its actions, and skip any stake actions
fn map_action_receipt(
    receipt: &mut ActionReceipt,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
    default_key: &PublicKey,
) {
    receipt.signer_id = crate::key_mapping::map_account(&receipt.signer_id, secret);
    receipt.signer_public_key =
        crate::key_mapping::map_key(&receipt.signer_public_key, secret).public_key();
    for receiver in receipt.output_data_receivers.iter_mut() {
        receiver.receiver_id = crate::key_mapping::map_account(&receiver.receiver_id, secret);
    }

    let mut actions = Vec::with_capacity(receipt.actions.len());
    let mut account_created = false;
    let mut full_key_added = false;
    for action in receipt.actions.iter() {
        if let Some(a) = crate::map_action(action, secret, default_key, true) {
            match &a {
                Action::AddKey(add_key) => {
                    if add_key.access_key.permission == AccessKeyPermission::FullAccess {
                        full_key_added = true;
                    }
                }
                Action::CreateAccount(_) => {
                    account_created = true;
                }
                _ => {}
            };
            actions.push(a);
        }
    }
    if account_created && !full_key_added {
        actions.push(Action::AddKey(Box::new(AddKeyAction {
            public_key: default_key.clone(),
            access_key: AccessKey::full_access(),
        })));
    }
    receipt.actions = actions;
}

// map any account IDs or keys referenced in the receipt
pub fn map_receipt(
    receipt: &mut Receipt,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
    default_key: &PublicKey,
) {
    receipt.set_predecessor_id(crate::key_mapping::map_account(receipt.predecessor_id(), secret));
    receipt.set_receiver_id(crate::key_mapping::map_account(receipt.receiver_id(), secret));
    match receipt.receipt_mut() {
        ReceiptEnum::Action(r) | ReceiptEnum::PromiseYield(r) => {
            map_action_receipt(r, secret, default_key);
        }
        _ => {}
    }
}

/// Reads records, makes changes to them and writes them to a new file.
/// `records_file_in` must be different from `records_file_out`.
/// Writes a secret to `secret_file_out`.
pub(crate) fn map_records<P: AsRef<Path>>(
    records_file_in: P,
    records_file_out: P,
    no_secret: bool,
    secret_file_out: P,
) -> anyhow::Result<()> {
    let secret = if no_secret {
        crate::secret::write_empty(secret_file_out)?;
        None
    } else {
        Some(crate::secret::generate(secret_file_out)?)
    };
    let reader = BufReader::new(File::open(records_file_in)?);
    let records_out = BufWriter::new(File::create(records_file_out)?);
    let mut records_ser = serde_json::Serializer::new(records_out);
    let mut records_seq = records_ser.serialize_seq(None).unwrap();

    let mut has_full_key = HashSet::new();
    let mut accounts = HashSet::new();

    let default_key = crate::key_mapping::default_extra_key(secret.as_ref()).public_key();
    near_chain_configs::stream_records_from_file(reader, |mut r| {
        match &mut r {
            StateRecord::AccessKey { account_id, public_key, access_key } => {
                let replacement = crate::key_mapping::map_key(&public_key, secret.as_ref());
                let new_record = StateRecord::AccessKey {
                    account_id: crate::key_mapping::map_account(&account_id, secret.as_ref()),
                    public_key: replacement.public_key(),
                    access_key: access_key.clone(),
                };
                // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                if account_id.get_account_type() != AccountType::NearImplicitAccount
                    && access_key.permission == AccessKeyPermission::FullAccess
                {
                    has_full_key.insert(account_id.clone());
                }
                // TODO: would be nice for stream_records_from_file() to let you return early on error so
                // we dont have to unwrap here
                records_seq.serialize_element(&new_record).unwrap();
            }
            StateRecord::Account { account_id, .. } => {
                // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                if account_id.get_account_type() == AccountType::NearImplicitAccount {
                    *account_id = crate::key_mapping::map_account(&account_id, secret.as_ref());
                } else {
                    accounts.insert(account_id.clone());
                }
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::Data { account_id, .. } => {
                // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                if account_id.get_account_type() == AccountType::NearImplicitAccount {
                    *account_id = crate::key_mapping::map_account(&account_id, secret.as_ref());
                }
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::Contract { account_id, .. } => {
                // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                if account_id.get_account_type() == AccountType::NearImplicitAccount {
                    *account_id = crate::key_mapping::map_account(&account_id, secret.as_ref());
                }
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::PostponedReceipt(receipt) => {
                map_receipt(receipt, secret.as_ref(), &default_key);
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::ReceivedData { account_id, .. } => {
                // TODO(eth-implicit) Change back to is_implicit() when ETH-implicit accounts are supported.
                if account_id.get_account_type() == AccountType::NearImplicitAccount {
                    *account_id = crate::key_mapping::map_account(&account_id, secret.as_ref());
                }
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::DelayedReceipt(receipt) => {
                map_receipt(receipt, secret.as_ref(), &default_key);
                records_seq.serialize_element(&r).unwrap();
            }
        };
    })?;

    for account_id in accounts {
        if !has_full_key.contains(&account_id) {
            records_seq.serialize_element(&StateRecord::AccessKey {
                account_id,
                public_key: default_key.clone(),
                access_key: AccessKey::full_access(),
            })?;
        }
    }
    records_seq.end()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use near_crypto::{KeyType, SecretKey};
    use near_primitives::account::{AccessKeyPermission, FunctionCallPermission};
    use near_primitives::action::delegate::{DelegateAction, SignedDelegateAction};
    use near_primitives::hash::CryptoHash;
    use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptV0};
    use near_primitives::transaction::{Action, AddKeyAction, CreateAccountAction};
    use near_primitives_core::account::AccessKey;

    #[test]
    fn test_map_receipt() {
        let default_key = crate::key_mapping::default_extra_key(None).public_key();

        let mut receipt0 = Receipt::V0(ReceiptV0 {
            predecessor_id: "foo.near".parse().unwrap(),
            receiver_id: "foo.foo.near".parse().unwrap(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: "foo.near".parse().unwrap(),
                signer_public_key: "ed25519:He7QeRuwizNEhBioYG3u4DZ8jWXyETiyNzFD3MkTjDMf"
                    .parse()
                    .unwrap(),
                gas_price: 100,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::AddKey(Box::new(AddKeyAction {
                        public_key: "ed25519:FXXrTXiKWpXj1R6r5fBvMLpstd8gPyrBq3qMByqKVzKF"
                            .parse()
                            .unwrap(),
                        access_key: AccessKey {
                            nonce: 0,
                            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                                allowance: None,
                                receiver_id: "foo.near".parse().unwrap(),
                                method_names: vec![String::from("do_thing")],
                            }),
                        },
                    })),
                ],
            }),
        });
        let want_receipt0 = Receipt::V0(ReceiptV0 {
            predecessor_id: "foo.near".parse().unwrap(),
            receiver_id: "foo.foo.near".parse().unwrap(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: "foo.near".parse().unwrap(),
                signer_public_key: "ed25519:6rL9HcTfinxxcVURLeQ3Y3nkietL4LQ3WxhPn51bCo4V"
                    .parse()
                    .unwrap(),
                gas_price: 100,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![
                    Action::CreateAccount(CreateAccountAction {}),
                    Action::AddKey(Box::new(AddKeyAction {
                        public_key: "ed25519:FYcGnVNM6wTcvm9b4UenJuCiiL9wDaJ3mpoebF4Go4mc"
                            .parse()
                            .unwrap(),
                        access_key: AccessKey {
                            nonce: 0,
                            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                                allowance: None,
                                receiver_id: "foo.near".parse().unwrap(),
                                method_names: vec![String::from("do_thing")],
                            }),
                        },
                    })),
                    Action::AddKey(Box::new(AddKeyAction {
                        public_key: default_key.clone(),
                        access_key: AccessKey::full_access(),
                    })),
                ],
            }),
        });

        let secret_key = SecretKey::from_random(KeyType::ED25519);
        let delegate_action = DelegateAction {
            sender_id: "d4156e03cb09f47117ddfde4fdcd5f3b8b087dccb364e228b8b3ed91d69054f4"
                .parse()
                .unwrap(),
            receiver_id: "foo.near".parse().unwrap(),
            nonce: 0,
            max_block_height: 1234,
            public_key: secret_key.public_key(),
            actions: vec![Action::AddKey(Box::new(AddKeyAction {
                public_key: "ed25519:Eo9W44tRMwcYcoua11yM7Xfr1DjgR4EWQFM3RU27MEX8".parse().unwrap(),
                access_key: AccessKey::full_access(),
            }))
            .try_into()
            .unwrap()],
        };
        let tx_hash = delegate_action.get_nep461_hash();
        let signature = secret_key.sign(tx_hash.as_ref());

        let mut receipt1 = Receipt::V0(ReceiptV0 {
            predecessor_id: "757a45019f9a3e5bd475586c31f63d6e15d50f5366caf4643f6f69731a222cad"
                .parse()
                .unwrap(),
            receiver_id: "d4156e03cb09f47117ddfde4fdcd5f3b8b087dccb364e228b8b3ed91d69054f4"
                .parse()
                .unwrap(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: "757a45019f9a3e5bd475586c31f63d6e15d50f5366caf4643f6f69731a222cad"
                    .parse()
                    .unwrap(),
                signer_public_key: "ed25519:He7QeRuwizNEhBioYG3u4DZ8jWXyETiyNzFD3MkTjDMf"
                    .parse()
                    .unwrap(),
                gas_price: 100,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![Action::Delegate(Box::new(SignedDelegateAction {
                    delegate_action,
                    signature,
                }))],
            }),
        });

        let mapped_secret_key = crate::key_mapping::map_key(&secret_key.public_key(), None);
        let delegate_action = DelegateAction {
            sender_id: "799185fe8173d8adf46b0c088d57887b2550642c08aafdc20ccce67b5ad51976"
                .parse()
                .unwrap(),
            receiver_id: "foo.near".parse().unwrap(),
            nonce: 0,
            max_block_height: 1234,
            public_key: mapped_secret_key.public_key(),
            actions: vec![Action::AddKey(Box::new(AddKeyAction {
                public_key: "ed25519:4etp3kcYH2rwGdbwbLbUd1AKHMEPLKosCMSQFqYqPL6V".parse().unwrap(),
                access_key: AccessKey::full_access(),
            }))
            .try_into()
            .unwrap()],
        };
        let tx_hash = delegate_action.get_nep461_hash();
        let signature = mapped_secret_key.sign(tx_hash.as_ref());
        let want_receipt1 = Receipt::V0(ReceiptV0 {
            predecessor_id: "3f8c3be8929e5fa61907f13a6247e7e452b92bb7d224cf691a9aa67814eb509b"
                .parse()
                .unwrap(),
            receiver_id: "799185fe8173d8adf46b0c088d57887b2550642c08aafdc20ccce67b5ad51976"
                .parse()
                .unwrap(),
            receipt_id: CryptoHash::default(),
            receipt: ReceiptEnum::Action(ActionReceipt {
                signer_id: "3f8c3be8929e5fa61907f13a6247e7e452b92bb7d224cf691a9aa67814eb509b"
                    .parse()
                    .unwrap(),
                signer_public_key: "ed25519:6rL9HcTfinxxcVURLeQ3Y3nkietL4LQ3WxhPn51bCo4V"
                    .parse()
                    .unwrap(),
                gas_price: 100,
                output_data_receivers: vec![],
                input_data_ids: vec![],
                actions: vec![Action::Delegate(Box::new(SignedDelegateAction {
                    delegate_action,
                    signature,
                }))],
            }),
        });

        crate::genesis::map_receipt(&mut receipt0, None, &default_key);
        assert_eq!(receipt0, want_receipt0);
        crate::genesis::map_receipt(&mut receipt1, None, &default_key);
        assert_eq!(receipt1, want_receipt1);
    }
}
