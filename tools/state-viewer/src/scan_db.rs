use borsh::BorshDeserialize;
use near_chain::types::LatestKnown;
use near_epoch_manager::types::EpochInfoAggregator;
use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::receipt::Receipt;
use near_primitives::shard_layout::{get_block_shard_uid_rev, ShardUId};
use near_primitives::sharding::{ChunkHash, ShardChunk, StateSyncInfo};
use near_primitives::state::FlatStateValue;
use near_primitives::syncing::{
    ShardStateSyncResponseHeader, StateHeaderKey, StatePartKey, StateSyncDumpProgress,
};
use near_primitives::transaction::{ExecutionOutcomeWithProof, SignedTransaction};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{EpochId, StateRoot};
use near_primitives::utils::{get_block_shard_id_rev, get_outcome_id_block_hash_rev};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::BlockHeight;
use near_store::flat::delta::KeyForFlatStateDelta;
use near_store::flat::{FlatStateChanges, FlatStateDeltaMetadata};
use near_store::{DBCol, RawTrieNodeWithSize, Store, TrieChanges};
use std::collections::HashSet;
use std::fmt::Debug;
use strum::IntoEnumIterator;

fn find_db_col(col: &str) -> DBCol {
    for db_col in DBCol::iter() {
        if format!("{}", db_col) == col {
            return db_col;
        }
    }
    panic!("Wrong columnn")
}

/// Scans a DB column, deserializes keys and values and prints them.
/// Note that this implementation doesn't support all columns, and formatting is the best it can be.
/// Refcounting is hard, and is handled by lookup by keys during a scan.
pub(crate) fn scan_db_column(
    col: &str,
    lower_bound: Option<&[u8]>,
    upper_bound: Option<&[u8]>,
    max_keys: Option<usize>,
    no_value: bool,
    store: Store,
) {
    let db_col: DBCol = find_db_col(col);
    tracing::info!(target: "scan", ?db_col);
    for item in
        store.iter_range(db_col, lower_bound, upper_bound).take(max_keys.unwrap_or(usize::MAX))
    {
        let (key, value) = item.unwrap();
        let (key_ser, value_ser) =
            format_key_and_value(key.as_ref(), value.as_ref(), db_col, &store);
        if no_value {
            println!("{:?}", key_ser);
        } else {
            println!("{:?}: {:?}", key_ser, value_ser);
        }
    }
}

// TODO: Support more columns.
fn format_key_and_value<'a>(
    key: &'a [u8],
    value: &'a [u8],
    db_col: DBCol,
    store: &'a Store,
) -> (Box<dyn Debug + 'a>, Box<dyn Debug + 'a>) {
    match db_col {
        DBCol::Block => (
            Box::new(CryptoHash::try_from(key).unwrap()),
            Box::new(Block::try_from_slice(value).unwrap()),
        ),
        DBCol::BlockHeader => (
            Box::new(CryptoHash::try_from(key).unwrap()),
            Box::new(BlockHeader::try_from_slice(value).unwrap()),
        ),
        DBCol::BlockHeight => (
            Box::new(BlockHeight::try_from_slice(key).unwrap()),
            Box::new(CryptoHash::try_from(value).unwrap()),
        ),
        DBCol::BlockInfo => (
            Box::new(CryptoHash::try_from(key).unwrap()),
            Box::new(BlockInfo::try_from_slice(value).unwrap()),
        ),
        DBCol::BlockMisc => (
            Box::new(String::from_utf8_lossy(key).to_string()),
            format_block_misc_value(key, value),
        ),
        DBCol::BlockRefCount => (
            Box::new(CryptoHash::try_from(key).unwrap()),
            Box::new(u64::try_from_slice(value).unwrap()),
        ),
        DBCol::ChunkExtra => (
            Box::new(get_block_shard_uid_rev(key).unwrap()),
            Box::new(ChunkExtra::try_from_slice(value).unwrap()),
        ),
        DBCol::ChunkHashesByHeight => (
            // TODO: Fix
            Box::new(BlockHeight::try_from_slice(key).unwrap()),
            Box::new(CryptoHash::try_from(value).unwrap()),
        ),
        DBCol::Chunks => (
            Box::new(ChunkHash::try_from_slice(key).unwrap()),
            Box::new(ShardChunk::try_from_slice(value).unwrap()),
        ),
        DBCol::DbVersion => (
            Box::new(String::from_utf8_lossy(key).to_string()),
            Box::new(String::from_utf8_lossy(value).to_string()),
        ),
        DBCol::EpochInfo => {
            if key != AGGREGATOR_KEY {
                (
                    Box::new(EpochId::try_from_slice(key).unwrap()),
                    Box::new(EpochInfo::try_from_slice(value).unwrap()),
                )
            } else {
                (
                    Box::new(String::from_utf8_lossy(key).to_string()),
                    Box::new(EpochInfoAggregator::try_from_slice(value).unwrap()),
                )
            }
        }
        DBCol::EpochStart => (
            // TODO: Fix
            Box::new(EpochId::try_from_slice(key).unwrap()),
            Box::new(BlockHeight::try_from_slice(value).unwrap()),
        ),
        DBCol::FlatState => {
            let (shard_uid, key) =
                near_store::flat::store_helper::decode_flat_state_db_key(key).unwrap();
            (Box::new((shard_uid, key)), Box::new(FlatStateValue::try_from_slice(value).unwrap()))
        }
        DBCol::FlatStateChanges => (
            // TODO: Format keys as nibbles.
            Box::new(KeyForFlatStateDelta::try_from_slice(key).unwrap()),
            Box::new(FlatStateChanges::try_from_slice(value).unwrap()),
        ),
        DBCol::FlatStateDeltaMetadata => (
            // TODO: Format keys as nibbles.
            Box::new(KeyForFlatStateDelta::try_from_slice(key).unwrap()),
            Box::new(FlatStateDeltaMetadata::try_from_slice(value).unwrap()),
        ),
        DBCol::FlatStorageStatus => (
            // TODO: Format keys as nibbles.
            Box::new(ShardUId::try_from_slice(key).unwrap()),
            Box::new(near_store::flat::FlatStorageStatus::try_from_slice(value).unwrap()),
        ),
        DBCol::HeaderHashesByHeight => (
            Box::new(BlockHeight::try_from_slice(key).unwrap()),
            Box::new(HashSet::<CryptoHash>::try_from_slice(value).unwrap()),
        ),
        DBCol::OutcomeIds => (
            Box::new(get_block_shard_id_rev(key).unwrap()),
            Box::new(Vec::<CryptoHash>::try_from_slice(value).unwrap()),
        ),
        DBCol::Receipts => {
            // Handle refcounting by querying the value.
            let value = store.get(db_col, key).unwrap().unwrap();
            (
                Box::new(CryptoHash::try_from(key).unwrap()),
                Box::new(Receipt::try_from_slice(&value).unwrap()),
            )
        }
        DBCol::State => {
            // This logic is exactly the same as KeyForFlatStateDelta.
            let s: ShardUId = ShardUId::try_from(&key[..8]).unwrap();
            let h: CryptoHash = CryptoHash::try_from_slice(&key[8..]).unwrap();
            // Handle refcounting by querying the value.
            let value = store.get(db_col, key).unwrap().unwrap();
            let res = if let Ok(node) = RawTrieNodeWithSize::try_from_slice(&value) {
                format!("Node: {node:?}")
            } else {
                format!("Value: {value:?}")
            };
            (Box::new((s, h)), Box::new(res))
        }
        DBCol::StateDlInfos => (
            Box::new(CryptoHash::try_from(key).unwrap()),
            Box::new(StateSyncInfo::try_from_slice(value).unwrap()),
        ),
        DBCol::StateHeaders => (
            Box::new(StateHeaderKey::try_from_slice(key).unwrap()),
            Box::new(ShardStateSyncResponseHeader::try_from_slice(value).unwrap()),
        ),
        DBCol::StateParts => (
            Box::new(StatePartKey::try_from_slice(key).unwrap()),
            // TODO: Print the trie containing in the state part.
            Box::new(value),
        ),
        DBCol::TransactionResultForBlock => (
            Box::new(get_outcome_id_block_hash_rev(key).unwrap()),
            Box::new(ExecutionOutcomeWithProof::try_from_slice(value).unwrap()),
        ),
        DBCol::Transactions => {
            // Handle refcounting by querying the value.
            let value = store.get(db_col, key).unwrap().unwrap();
            (
                Box::new(CryptoHash::try_from(key).unwrap()),
                Box::new(SignedTransaction::try_from_slice(&value).unwrap()),
            )
        }
        DBCol::TrieChanges => (
            Box::new(get_block_shard_uid_rev(key).unwrap()),
            Box::new(TrieChanges::try_from_slice(value).unwrap()),
        ),
        _ => (Box::new(key), Box::new(value)),
    }
}

fn format_block_misc_value<'a>(key: &'a [u8], value: &'a [u8]) -> Box<dyn Debug + 'a> {
    if key == near_store::HEAD_KEY
        || key == near_store::HEADER_HEAD_KEY
        || key == near_store::FINAL_HEAD_KEY
        || key == near_store::COLD_HEAD_KEY
        || key == b"SYNC_HEAD"
    {
        Box::new(Tip::try_from_slice(value).unwrap())
    } else if key == near_store::TAIL_KEY
        || key == near_store::CHUNK_TAIL_KEY
        || key == near_store::FORK_TAIL_KEY
        || key == near_store::LARGEST_TARGET_HEIGHT_KEY
    {
        Box::new(BlockHeight::try_from_slice(value).unwrap())
    } else if key == near_store::LATEST_KNOWN_KEY {
        Box::new(LatestKnown::try_from_slice(value).unwrap())
    } else if key == near_store::GENESIS_JSON_HASH_KEY {
        Box::new(CryptoHash::try_from(value).unwrap())
    } else if key == near_store::GENESIS_STATE_ROOTS_KEY {
        Box::new(Vec::<StateRoot>::try_from_slice(value).unwrap())
    } else if key.starts_with(near_store::STATE_SYNC_DUMP_KEY) {
        Box::new(StateSyncDumpProgress::try_from_slice(value).unwrap())
    } else {
        Box::new(value)
    }
}
