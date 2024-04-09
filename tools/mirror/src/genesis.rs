use near_crypto::PublicKey;
use near_primitives::action::delegate::{DelegateAction, SignedDelegateAction};
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum};
use near_primitives::state_record::StateRecord;
use near_primitives::transaction::{Action, AddKeyAction, DeleteAccountAction, DeleteKeyAction};
use near_primitives_core::account::id::AccountType;
use near_primitives_core::account::{AccessKey, AccessKeyPermission};
use serde::ser::{SerializeSeq, Serializer};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

fn map_action(
    action: &Action,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
    default_key: &PublicKey,
    delegate_allowed: bool,
) -> Option<Action> {
    match action {
        Action::AddKey(add_key) => {
            let public_key = crate::key_mapping::map_key(&add_key.public_key, secret).public_key();

            Some(Action::AddKey(Box::new(AddKeyAction {
                public_key,
                access_key: add_key.access_key.clone(),
            })))
        }
        Action::DeleteKey(delete_key) => {
            let public_key =
                crate::key_mapping::map_key(&delete_key.public_key, secret).public_key();

            Some(Action::DeleteKey(Box::new(DeleteKeyAction { public_key })))
        }
        Action::DeleteAccount(delete_account) => {
            let beneficiary_id =
                crate::key_mapping::map_account(&delete_account.beneficiary_id, secret);
            Some(Action::DeleteAccount(DeleteAccountAction { beneficiary_id }))
        }
        Action::Delegate(delegate) => {
            if delegate_allowed {
                map_delegate_action(delegate, secret, default_key)
            } else {
                // This should not happen, but we handle the case here defensively
                tracing::warn!(target: "mirror", "a delegate action was contained inside another delegate action: {:?}", delegate);
                None
            }
        }
        // We don't want to mess with the set of validators in the target chain
        Action::Stake(_) => None,
        _ => Some(action.clone()),
    }
}

fn map_delegate_action(
    delegate: &SignedDelegateAction,
    secret: Option<&[u8; crate::secret::SECRET_LEN]>,
    default_key: &PublicKey,
) -> Option<Action> {
    let source_actions = delegate.delegate_action.get_actions();
    let mut actions = Vec::with_capacity(source_actions.len());

    let mut account_created = false;
    let mut full_key_added = false;
    for action in source_actions.iter() {
        if let Some(a) = map_action(action, secret, default_key, false) {
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
            actions.push(a.try_into().unwrap());
        }
    }
    if actions.is_empty() {
        return None;
    }
    if account_created && !full_key_added {
        actions.push(
            Action::AddKey(Box::new(AddKeyAction {
                public_key: default_key.clone(),
                access_key: AccessKey::full_access(),
            }))
            .try_into()
            .unwrap(),
        );
    }
    let mapped_key = crate::key_mapping::map_key(&delegate.delegate_action.public_key, secret);
    let mapped_action = DelegateAction {
        sender_id: crate::key_mapping::map_account(&delegate.delegate_action.sender_id, secret),
        receiver_id: crate::key_mapping::map_account(&delegate.delegate_action.receiver_id, secret),
        actions,
        nonce: delegate.delegate_action.nonce,
        max_block_height: delegate.delegate_action.max_block_height,
        public_key: mapped_key.public_key(),
    };
    let tx_hash = mapped_action.get_nep461_hash();
    let d = SignedDelegateAction {
        delegate_action: mapped_action,
        signature: mapped_key.sign(tx_hash.as_ref()),
    };
    Some(Action::Delegate(Box::new(d)))
}

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
        if let Some(a) = map_action(action, secret, default_key, true) {
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
    receipt.predecessor_id = crate::key_mapping::map_account(&receipt.predecessor_id, secret);
    receipt.receiver_id = crate::key_mapping::map_account(&receipt.receiver_id, secret);
    match &mut receipt.receipt {
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
