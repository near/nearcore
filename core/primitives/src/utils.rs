use bs58;
use byteorder::{LittleEndian, WriteBytesExt};

use regex::Regex;
use crate::types::{AccountId, ShardId};

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

pub fn is_valid_account_id(account_id: &AccountId) -> bool {
    let re = Regex::new(r"^[a-z0-9@._\-]{5,32}$").unwrap();
    re.is_match(account_id)
}