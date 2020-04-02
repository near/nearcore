use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

use near_crypto::PublicKey;

use crate::account::{AccessKey, Account};
use crate::hash::{hash, CryptoHash};
use crate::receipt::{Receipt, ReceivedData};
use crate::serialize::{from_base64, option_base64_format, to_base64};
use crate::trie_key::col;
use crate::trie_key::trie_key_parsers::{
    parse_account_id_from_access_key_key, parse_account_id_from_account_key,
    parse_account_id_from_contract_code_key, parse_account_id_from_contract_data_key,
    parse_account_id_from_received_data_key, parse_data_id_from_received_data_key,
    parse_data_key_from_contract_data_key, parse_public_key_from_access_key_key,
};
use crate::types::AccountId;
use crate::views::{AccessKeyView, AccountView, ReceiptView};

/// Record in the state storage.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum StateRecord {
    /// Account information.
    Account { account_id: AccountId, account: AccountView },
    /// Data records inside the contract, encoded in base64.
    Data { key: String, value: String },
    /// Contract code encoded in base64.
    Contract { account_id: AccountId, code: String },
    /// Access key associated with some account.
    AccessKey { account_id: AccountId, public_key: PublicKey, access_key: AccessKeyView },
    /// Postponed Action Receipt.
    PostponedReceipt(Box<ReceiptView>),
    /// Received data from DataReceipt encoded in base64 for the given account_id and data_id.
    ReceivedData {
        account_id: AccountId,
        data_id: CryptoHash,
        #[serde(with = "option_base64_format")]
        data: Option<Vec<u8>>,
    },
}

impl StateRecord {
    pub fn from_raw_key_value(key: Vec<u8>, value: Vec<u8>) -> Option<StateRecord> {
        let column = &key[0..1];
        match column {
            col::ACCOUNT => {
                let account_id = parse_account_id_from_contract_data_key(&key);
                if account_id.is_ok() {
                    Some(StateRecord::Data { key: to_base64(&key), value: to_base64(&value) })
                } else {
                    Some(StateRecord::Account {
                        account_id: parse_account_id_from_account_key(&key).unwrap(),
                        account: Account::try_from_slice(&value).unwrap().into(),
                    })
                }
            }
            col::CONTRACT_CODE => Some(StateRecord::Contract {
                account_id: parse_account_id_from_contract_code_key(&key).unwrap(),
                code: to_base64(&value),
            }),
            col::ACCESS_KEY => {
                let access_key = AccessKey::try_from_slice(&value).unwrap();
                let account_id = parse_account_id_from_access_key_key(&key).unwrap();
                let public_key = parse_public_key_from_access_key_key(&key, &account_id).unwrap();
                Some(StateRecord::AccessKey {
                    account_id,
                    public_key,
                    access_key: access_key.into(),
                })
            }
            col::RECEIVED_DATA => {
                let data = ReceivedData::try_from_slice(&value).unwrap().data;
                let account_id = parse_account_id_from_received_data_key(&key).unwrap();
                let data_id = parse_data_id_from_received_data_key(&key, &account_id).unwrap();
                Some(StateRecord::ReceivedData { account_id, data_id, data })
            }
            col::POSTPONED_RECEIPT_ID => None,
            col::PENDING_DATA_COUNT => None,
            col::POSTPONED_RECEIPT => {
                let receipt = Receipt::try_from_slice(&value).unwrap();
                Some(StateRecord::PostponedReceipt(Box::new(receipt.into())))
            }
            _ => unreachable!(),
        }
    }
}

impl Display for StateRecord {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        match self {
            StateRecord::Account { account_id, account } => {
                write!(f, "Account {:?}: {:?}", account_id, account)
            }
            StateRecord::Data { key, value } => {
                let key = from_base64(&key).unwrap();
                let account_id = parse_account_id_from_contract_data_key(&key).unwrap();
                let data_key = parse_data_key_from_contract_data_key(&key, &account_id).unwrap();
                let contract_key = to_printable(data_key);
                write!(
                    f,
                    "Storage {:?},{:?}: {:?}",
                    account_id,
                    contract_key,
                    to_printable(&from_base64(&value).unwrap())
                )
            }
            StateRecord::Contract { account_id, code: _ } => {
                write!(f, "Code for {:?}: ...", account_id)
            }
            StateRecord::AccessKey { account_id, public_key, access_key } => {
                write!(f, "Access key {:?},{:?}: {:?}", account_id, public_key, access_key)
            }
            StateRecord::ReceivedData { account_id, data_id, data } => write!(
                f,
                "Received data {:?},{:?}: {:?}",
                account_id,
                data_id,
                data.as_ref().map(|v| to_printable(&v))
            ),
            StateRecord::PostponedReceipt(receipt) => write!(f, "Postponed receipt {:?}", receipt),
        }
    }
}

fn to_printable(blob: &[u8]) -> String {
    if blob.len() > 60 {
        format!("{} bytes, hash: {}", blob.len(), hash(blob))
    } else {
        let ugly = blob.iter().any(|&x| x < b' ');
        if ugly {
            return format!("0x{}", hex::encode(blob));
        }
        match String::from_utf8(blob.to_vec()) {
            Ok(v) => v,
            Err(_e) => format!("0x{}", hex::encode(blob)),
        }
    }
}
