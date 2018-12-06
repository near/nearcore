use byteorder::{LittleEndian, WriteBytesExt};
use types::{AccountId, ShardId};

pub fn index_to_bytes(index: u64) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.write_u64::<LittleEndian>(index).expect("writing to bytes failed");
    bytes
}

pub fn concat<T>(args: Vec<Vec<T>>) -> Vec<T> {
    args.into_iter().fold(vec![], |mut acc, mut v| {
        acc.append(&mut v);
        acc
    })
}

#[allow(unused)]
pub fn account_to_shard_id(account_id: AccountId) -> ShardId {
    // TODO: change to real sharding
    0
}