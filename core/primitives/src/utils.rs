use bs58;
use byteorder::{LittleEndian, WriteBytesExt};
use protobuf::{well_known_types::StringValue, SingularPtrField};
use std::convert::{TryFrom, TryInto};

use crate::types::{AccountId, ShardId};
use regex::Regex;

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
