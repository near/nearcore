use std::collections::HashMap;

use crate::adapter::StoreAdapter;
use crate::adapter::chain_store::ChainStoreAdapter;
use crate::{DBCol, Store};
use borsh::BorshDeserialize;
use near_chain_primitives::Error;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::types::BlockHeight;

use super::timer::WorkTimer;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct HashIndex(u32);

#[derive(Debug)]
pub struct ReadDbData {
    pub hash_to_index: HashMap<CryptoHash, HashIndex>,
    pub height_to_block_hash: Vec<(u32, HashIndex)>,
    pub block_hash_to_ordinal: HashMap<HashIndex, u32>,
    pub ordinal_to_block_hash: HashMap<u32, HashIndex>,
}

pub fn read_db_data(store: &Store) -> Result<ReadDbData, Error> {
    let chain_store = ChainStoreAdapter::new(store.clone());

    let tip = chain_store.head()?;
    let last_block_ordinal = chain_store.get_block_merkle_tree(&tip.last_block_hash)?.size();
    let estimated_block_count: usize = (last_block_ordinal + 1).try_into().unwrap();

    let (db_update_sender, db_update_receiver) =
        std::sync::mpsc::sync_channel::<DbReadUpdate>(4096);
    let store = chain_store.store();

    let mut read_height_to_block_hash_timer =
        WorkTimer::new("Read DBCol::BlockHeight", estimated_block_count);
    let read_height_to_block_hash_thread = {
        let store = store.clone();
        let db_update_sender = db_update_sender.clone();
        std::thread::spawn(move || read_height_to_block_hash(&store, &db_update_sender))
    };
    let mut read_block_hash_to_ordinal_timer =
        WorkTimer::new("Read DBCol::BlockMerkleTree", estimated_block_count);
    let read_block_hash_to_ordinal_thread = {
        let store = store.clone();
        let db_update_sender = db_update_sender.clone();
        std::thread::spawn(move || read_block_hash_to_ordinal(&store, &db_update_sender))
    };
    let mut read_ordinal_to_block_hash_timer =
        WorkTimer::new("Read DBCol::BlockOrdinal", estimated_block_count);
    let read_ordinal_to_block_hash_thread = {
        let db_update_sender = db_update_sender.clone();
        std::thread::spawn(move || read_ordinal_to_block_hash(&store, &db_update_sender))
    };
    std::mem::drop(db_update_sender);

    let mut hash_to_index: HashMap<CryptoHash, HashIndex> =
        HashMap::with_capacity(estimated_block_count);
    let mut height_to_hash: Vec<(u32, HashIndex)> = Vec::with_capacity(estimated_block_count);
    let mut ordinal_to_hash: HashMap<u32, HashIndex> =
        HashMap::with_capacity(estimated_block_count);
    let mut hash_to_ordinal: HashMap<HashIndex, u32> =
        HashMap::with_capacity(estimated_block_count);

    let mut get_hash_index = |hash: CryptoHash| -> HashIndex {
        let next_index = HashIndex(hash_to_index.len().try_into().unwrap());
        *hash_to_index.entry(hash).or_insert(next_index)
    };

    while let Ok(db_update) = db_update_receiver.recv() {
        match db_update {
            DbReadUpdate::HeightToBlockHash(entries) => {
                read_height_to_block_hash_timer.add_processed(entries.len());
                for (height, block_hash) in entries {
                    let hash_index = get_hash_index(block_hash);
                    height_to_hash.push((height.try_into().unwrap(), hash_index));
                }
            }
            DbReadUpdate::BlockHashToOrdinal(entries) => {
                read_block_hash_to_ordinal_timer.add_processed(entries.len());
                for (block_hash, ordinal) in entries {
                    let hash_index = get_hash_index(block_hash);
                    hash_to_ordinal.insert(hash_index, ordinal.try_into().unwrap());
                }
            }
            DbReadUpdate::OrdinalToBlockHash(entries) => {
                read_ordinal_to_block_hash_timer.add_processed(entries.len());
                for (ordinal, block_hash) in entries {
                    let hash_index = get_hash_index(block_hash);
                    ordinal_to_hash.insert(ordinal.try_into().unwrap(), hash_index);
                }
            }
            DbReadUpdate::FinishedReadingHeightToBlockHash => {
                read_height_to_block_hash_timer.finish();
            }
            DbReadUpdate::FinishedReadingBlockHashToOrdinal => {
                read_block_hash_to_ordinal_timer.finish();
            }
            DbReadUpdate::FinishedReadingOrdinalToBlockHash => {
                read_ordinal_to_block_hash_timer.finish();
            }
        }
    }

    read_height_to_block_hash_thread.join().unwrap()?;
    read_block_hash_to_ordinal_thread.join().unwrap()?;
    read_ordinal_to_block_hash_thread.join().unwrap()?;

    Ok(ReadDbData {
        hash_to_index,
        height_to_block_hash: height_to_hash,
        block_hash_to_ordinal: hash_to_ordinal,
        ordinal_to_block_hash: ordinal_to_hash,
    })
}

enum DbReadUpdate {
    HeightToBlockHash(Vec<(BlockHeight, CryptoHash)>),
    BlockHashToOrdinal(Vec<(CryptoHash, u64)>),
    OrdinalToBlockHash(Vec<(u64, CryptoHash)>),
    FinishedReadingHeightToBlockHash,
    FinishedReadingBlockHashToOrdinal,
    FinishedReadingOrdinalToBlockHash,
}

impl DbReadUpdate {
    fn batch_size() -> usize {
        4096
    }
}

fn read_height_to_block_hash(
    store: &Store,
    db_update_sender: &std::sync::mpsc::SyncSender<DbReadUpdate>,
) -> Result<(), Error> {
    read_db_column(store, DBCol::BlockHeight, |entries: Vec<(u64, CryptoHash)>| {
        db_update_sender.send(DbReadUpdate::HeightToBlockHash(entries)).unwrap();
    })?;
    db_update_sender.send(DbReadUpdate::FinishedReadingHeightToBlockHash).unwrap();
    Ok(())
}

fn read_block_hash_to_ordinal(
    store: &Store,
    db_update_sender: &std::sync::mpsc::SyncSender<DbReadUpdate>,
) -> Result<(), Error> {
    read_db_column(
        store,
        DBCol::BlockMerkleTree,
        |entries: Vec<(CryptoHash, PartialMerkleTree)>| {
            let entries = entries.into_iter().map(|(h, tree)| (h, tree.size())).collect();
            db_update_sender.send(DbReadUpdate::BlockHashToOrdinal(entries)).unwrap();
        },
    )?;
    db_update_sender.send(DbReadUpdate::FinishedReadingBlockHashToOrdinal).unwrap();
    Ok(())
}

fn read_ordinal_to_block_hash(
    store: &Store,
    db_update_sender: &std::sync::mpsc::SyncSender<DbReadUpdate>,
) -> Result<(), Error> {
    read_db_column(store, DBCol::BlockOrdinal, |entries: Vec<(u64, CryptoHash)>| {
        db_update_sender.send(DbReadUpdate::OrdinalToBlockHash(entries)).unwrap();
    })?;
    db_update_sender.send(DbReadUpdate::FinishedReadingOrdinalToBlockHash).unwrap();
    Ok(())
}

fn read_db_column<KeyType: BorshDeserialize, ValueType: BorshDeserialize>(
    store: &Store,
    column: DBCol,
    send_batch: impl Fn(Vec<(KeyType, ValueType)>),
) -> Result<(), Error> {
    let mut cur_batch = Vec::with_capacity(DbReadUpdate::batch_size());
    let mut iter = store.iter_ser::<ValueType>(column);
    while let Some(res) = iter.next() {
        let (key_bytes, value) = res?;
        let key = KeyType::try_from_slice(&key_bytes)?;

        cur_batch.push((key, value));
        if cur_batch.len() >= DbReadUpdate::batch_size() {
            send_batch(cur_batch);
            cur_batch = Vec::with_capacity(DbReadUpdate::batch_size());
        }
    }
    if !cur_batch.is_empty() {
        send_batch(cur_batch);
    }
    Ok(())
}
