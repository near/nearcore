use bs58;
use byteorder::{LittleEndian, WriteBytesExt};
use lazy_static::lazy_static;
use protobuf::{well_known_types::StringValue, SingularPtrField};
use std::convert::{TryFrom, TryInto};

use crate::crypto::signature::PublicKey;
use crate::hash::{hash, CryptoHash};
use crate::types::{AccountId, ShardId};
use regex::Regex;

pub mod col {
    pub const ACCOUNT: &[u8] = &[0];
    pub const CALLBACK: &[u8] = &[1];
    pub const CODE: &[u8] = &[2];
    pub const TX_STAKE: &[u8] = &[3];
    pub const TX_STAKE_SEPARATOR: &[u8] = &[4];
    pub const ACCESS_KEY: &[u8] = &[5];
}

fn key_for_column_account_id(column: &[u8], account_key: &AccountId) -> Vec<u8> {
    let mut key = column.to_vec();
    key.append(&mut account_key.clone().into_bytes());
    key
}

pub fn key_for_account(account_key: &AccountId) -> Vec<u8> {
    key_for_column_account_id(col::ACCOUNT, account_key)
}

pub fn prefix_for_access_key(account_id: &AccountId) -> Vec<u8> {
    let mut key = key_for_column_account_id(col::ACCESS_KEY, account_id);
    key.extend_from_slice(col::ACCESS_KEY);
    key
}

pub fn key_for_access_key(account_id: &AccountId, public_key: &PublicKey) -> Vec<u8> {
    let mut key = key_for_column_account_id(col::ACCESS_KEY, account_id);
    key.extend_from_slice(col::ACCESS_KEY);
    key.extend_from_slice(public_key.as_ref());
    key
}

pub fn key_for_code(account_key: &AccountId) -> Vec<u8> {
    key_for_column_account_id(col::CODE, account_key)
}

pub fn key_for_callback(id: &[u8]) -> Vec<u8> {
    let mut key = col::CALLBACK.to_vec();
    key.extend_from_slice(id);
    key
}

pub fn key_for_tx_stake(account_id: &AccountId, contract_id: &Option<AccountId>) -> Vec<u8> {
    let mut key = key_for_column_account_id(col::TX_STAKE, &account_id);
    if let Some(ref contract_id) = contract_id {
        key.append(&mut key_for_column_account_id(col::TX_STAKE_SEPARATOR, contract_id));
    }
    key
}

pub fn create_nonce_with_nonce(base: &CryptoHash, salt: u64) -> CryptoHash {
    let mut nonce: Vec<u8> = base.as_ref().to_owned();
    nonce.append(&mut index_to_bytes(salt));
    hash(&nonce)
}

pub fn index_to_bytes(index: u64) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.write_u64::<LittleEndian>(index).expect("writing to bytes failed");
    bytes
}

#[allow(unused)]
pub fn account_to_shard_id(account_id: &AccountId) -> ShardId {
    // TODO: change to real sharding
    0
}

pub fn bs58_vec2str(buf: &[u8]) -> String {
    bs58::encode(buf).into_string()
}

lazy_static! {
    static ref VALID_ACCOUNT_ID: Regex = Regex::new(r"^[a-z0-9@._\-]{5,32}$").unwrap();
}

pub fn is_valid_account_id(account_id: &AccountId) -> bool {
    VALID_ACCOUNT_ID.is_match(account_id)
}

pub fn to_string_value(s: String) -> StringValue {
    let mut res = StringValue::new();
    res.set_value(s);
    res
}

pub fn proto_to_result<T>(proto: SingularPtrField<T>) -> Result<T, String> {
    proto.into_option().ok_or_else(|| "Bad Proto".to_string())
}

pub fn proto_to_type<T, U>(proto: SingularPtrField<T>) -> Result<U, String>
where
    U: TryFrom<T, Error = String>,
{
    proto_to_result(proto).and_then(TryInto::try_into)
}
