use borsh::BorshDeserialize;
use near_epoch_manager::types::EpochInfoAggregator;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{get_block_shard_uid_rev, ShardUId};
use near_primitives::sharding::{ChunkHash, ShardChunk, StateSyncInfo};
use near_primitives::state::{FlatStateValue, ValueRef};
use near_primitives::syncing::{ShardStateSyncResponseHeader, StateHeaderKey, StatePartKey};
use near_primitives::transaction::{ExecutionOutcomeWithProof, SignedTransaction};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::EpochId;
use near_primitives::utils::{get_block_shard_id_rev, get_outcome_id_block_hash_rev};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::BlockHeight;
use near_store::flat::delta::KeyForFlatStateDelta;
use near_store::flat::{FlatStateChanges, FlatStateDeltaMetadata};
use near_store::{DBCol, RawTrieNodeWithSize, Store, TrieChanges};
use std::collections::HashSet;
use std::fmt::Debug;
use strum::IntoEnumIterator;

pub(crate) fn scan_db_column(col: &str, store: Store) {
    for db_col in DBCol::iter() {
        if format!("{}", db_col) == col {
            println!("db_col: {}", db_col);
            for item in store.iter(db_col) {
                let (key, value) = item.unwrap();
                let key_ref = key.as_ref();
                let value_ref = value.as_ref();
                // TODO: Support more columns.
                let (key_ser, val_ser): (Box<dyn Debug>, Box<dyn Debug>) = match db_col {
                    DBCol::Block => (
                        Box::new(CryptoHash::try_from(key_ref).unwrap()),
                        Box::new(Block::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::BlockHeader => (
                        Box::new(CryptoHash::try_from(key_ref).unwrap()),
                        Box::new(BlockHeader::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::BlockHeight => (
                        Box::new(BlockHeight::try_from_slice(key_ref).unwrap()),
                        Box::new(CryptoHash::try_from(value_ref).unwrap()),
                    ),
                    DBCol::BlockInfo => (
                        Box::new(CryptoHash::try_from(key_ref).unwrap()),
                        Box::new(BlockInfo::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::BlockMisc => (
                        Box::new(String::from_utf8_lossy(key_ref).to_string()),
                        // TODO: Show some values as strings and some as uints.
                        Box::new(value.to_vec()),
                    ),
                    DBCol::BlockRefCount => (
                        Box::new(CryptoHash::try_from(key_ref).unwrap()),
                        Box::new(u64::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::ChunkExtra => (
                        Box::new(get_block_shard_uid_rev(key_ref).unwrap()),
                        Box::new(ChunkExtra::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::ChunkHashesByHeight => (
                        // TODO: Fix
                        Box::new(BlockHeight::try_from_slice(key_ref).unwrap()),
                        Box::new(CryptoHash::try_from(value_ref).unwrap()),
                    ),
                    DBCol::Chunks => (
                        Box::new(ChunkHash::try_from_slice(key_ref).unwrap()),
                        Box::new(ShardChunk::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::DbVersion => (
                        Box::new(String::from_utf8_lossy(key_ref).to_string()),
                        Box::new(String::from_utf8_lossy(value_ref).to_string()),
                    ),
                    DBCol::EpochInfo => {
                        if key_ref != AGGREGATOR_KEY {
                            (
                                Box::new(EpochId::try_from_slice(key_ref).unwrap()),
                                Box::new(EpochInfo::try_from_slice(value_ref).unwrap()),
                            )
                        } else {
                            (
                                Box::new(String::from_utf8_lossy(key_ref).to_string()),
                                Box::new(EpochInfoAggregator::try_from_slice(value_ref).unwrap()),
                            )
                        }
                    }
                    DBCol::EpochStart => (
                        // TODO: Fix
                        Box::new(EpochId::try_from_slice(key_ref).unwrap()),
                        Box::new(BlockHeight::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::FlatState => (
                        // TODO: Fix
                        Box::new(key.to_vec()),
                        Box::new(FlatStateValue::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::FlatStateChanges => (
                        // TODO: Format keys as nibbles.
                        Box::new(KeyForFlatStateDelta::try_from_slice(key_ref).unwrap()),
                        Box::new(FlatStateChanges::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::FlatStateDeltaMetadata => (
                        // TODO: Format keys as nibbles.
                        Box::new(KeyForFlatStateDelta::try_from_slice(key_ref).unwrap()),
                        Box::new(FlatStateDeltaMetadata::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::HeaderHashesByHeight => (
                        Box::new(BlockHeight::try_from_slice(key_ref).unwrap()),
                        Box::new(HashSet::<CryptoHash>::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::OutcomeIds => (
                        Box::new(get_block_shard_id_rev(key_ref).unwrap()),
                        Box::new(Vec::<CryptoHash>::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::Receipts => {
                        // Handle refcounting by querying the value.
                        let value = store.get(db_col, key_ref).unwrap().unwrap();
                        (
                            Box::new(CryptoHash::try_from(key_ref).unwrap()),
                            Box::new(Receipt::try_from_slice(&value).unwrap()),
                        )
                    }
                    DBCol::State => {
                        let s: ShardUId = ShardUId::try_from(&key_ref[..8]).unwrap();
                        let h: CryptoHash = CryptoHash::try_from_slice(&key_ref[8..]).unwrap();
                        // TODO: Fix
                        // Handle refcounting by querying the value.
                        let value = store.get(db_col, key_ref).unwrap().unwrap();
                        (
                            Box::new(CryptoHash::try_from(key_ref).unwrap()),
                            Box::new(RawTrieNodeWithSize::try_from_slice(&value).unwrap()),
                        )
                    }
                    DBCol::StateDlInfos => (
                        Box::new(CryptoHash::try_from(key_ref).unwrap()),
                        Box::new(StateSyncInfo::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::StateHeaders => (
                        Box::new(StateHeaderKey::try_from_slice(key_ref).unwrap()),
                        Box::new(ShardStateSyncResponseHeader::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::StateParts => (
                        Box::new(StatePartKey::try_from_slice(key_ref).unwrap()),
                        // TODO: Print the trie containing in the state part.
                        Box::new(value_ref),
                    ),
                    DBCol::TransactionResultForBlock => (
                        Box::new(get_outcome_id_block_hash_rev(key_ref).unwrap()),
                        Box::new(ExecutionOutcomeWithProof::try_from_slice(value_ref).unwrap()),
                    ),
                    DBCol::Transactions => {
                        // Handle refcounting by querying the value.
                        let value = store.get(db_col, key_ref).unwrap().unwrap();
                        (
                            Box::new(CryptoHash::try_from(key_ref).unwrap()),
                            Box::new(SignedTransaction::try_from_slice(&value).unwrap()),
                        )
                    }
                    DBCol::TrieChanges => (
                        Box::new(get_block_shard_uid_rev(key_ref).unwrap()),
                        Box::new(TrieChanges::try_from_slice(value_ref).unwrap()),
                    ),
                    _ => (Box::new(key_ref), Box::new(value_ref)),
                };
                println!("{:?}: {:?}", key_ser, val_ser);
            }
            return;
        }
    }
    println!("Unknown DBCol");
    std::process::exit(1);
}
