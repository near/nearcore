use near_primitives::state_record::StateRecord;
use near_primitives_core::account::{AccessKey, AccessKeyPermission};
use serde::ser::{SerializeSeq, Serializer};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

pub fn map_records<P: AsRef<Path>>(
    records_file_in: P,
    records_file_out: P,
    no_secret: bool,
    secret_file_out: P,
) -> anyhow::Result<()> {
    let secret = if !no_secret {
        Some(crate::secret::generate(secret_file_out)?)
    } else {
        crate::secret::write_empty(secret_file_out)?;
        None
    };
    let reader = BufReader::new(File::open(records_file_in)?);
    let records_out = BufWriter::new(File::create(records_file_out)?);
    let mut records_ser = serde_json::Serializer::new(records_out);
    let mut records_seq = records_ser.serialize_seq(None).unwrap();

    let mut has_full_key = HashSet::new();
    let mut accounts = HashSet::new();

    near_chain_configs::stream_records_from_file(reader, |mut r| {
        match &mut r {
            StateRecord::AccessKey { account_id, public_key, access_key } => {
                let replacement = crate::key_mapping::map_key(&public_key, secret.as_ref());
                let new_record = StateRecord::AccessKey {
                    account_id: crate::key_mapping::map_account(&account_id, secret.as_ref()),
                    public_key: replacement.public_key(),
                    access_key: access_key.clone(),
                };
                if !account_id.is_implicit()
                    && access_key.permission == AccessKeyPermission::FullAccess
                {
                    has_full_key.insert(account_id.clone());
                }
                // TODO: would be nice for stream_records_from_file() to let you return early on error so
                // we dont have to unwrap here
                records_seq.serialize_element(&new_record).unwrap();
            }
            StateRecord::Account { account_id, .. } => {
                if account_id.is_implicit() {
                    *account_id = crate::key_mapping::map_account(&account_id, secret.as_ref());
                } else {
                    accounts.insert(account_id.clone());
                }
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::Data { account_id, .. } => {
                if account_id.is_implicit() {
                    *account_id = crate::key_mapping::map_account(&account_id, secret.as_ref());
                }
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::Contract { account_id, .. } => {
                if account_id.is_implicit() {
                    *account_id = crate::key_mapping::map_account(&account_id, secret.as_ref());
                }
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::PostponedReceipt(receipt) => {
                if receipt.predecessor_id.is_implicit() || receipt.receiver_id.is_implicit() {
                    receipt.predecessor_id =
                        crate::key_mapping::map_account(&receipt.predecessor_id, secret.as_ref());
                    receipt.receiver_id =
                        crate::key_mapping::map_account(&receipt.receiver_id, secret.as_ref());
                }
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::ReceivedData { account_id, .. } => {
                if account_id.is_implicit() {
                    *account_id = crate::key_mapping::map_account(&account_id, secret.as_ref());
                }
                records_seq.serialize_element(&r).unwrap();
            }
            StateRecord::DelayedReceipt(receipt) => {
                if receipt.predecessor_id.is_implicit() || receipt.receiver_id.is_implicit() {
                    receipt.predecessor_id =
                        crate::key_mapping::map_account(&receipt.predecessor_id, secret.as_ref());
                    receipt.receiver_id =
                        crate::key_mapping::map_account(&receipt.receiver_id, secret.as_ref());
                }
                records_seq.serialize_element(&r).unwrap();
            }
        };
    })?;
    for account_id in accounts {
        if !has_full_key.contains(&account_id) {
            records_seq.serialize_element(&StateRecord::AccessKey {
                account_id,
                public_key: crate::key_mapping::EXTRA_KEY.public_key(),
                access_key: AccessKey::full_access(),
            })?;
        }
    }
    records_seq.end()?;
    Ok(())
}
