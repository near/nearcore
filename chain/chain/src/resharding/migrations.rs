use std::str::FromStr;

use near_chain_configs::GenesisConfig;
use near_epoch_manager::{EpochManager, EpochManagerAdapter};
use near_primitives::hash::CryptoHash;
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::get_shard_uid_mapping;
use near_store::archive::cold_storage::{join_two_keys, rc_aware_set};
use near_store::db::DBTransaction;
use near_store::db::metadata::DbKind;
use near_store::flat::{BlockInfo, FlatStorageManager};
use near_store::trie::ops::resharding::RetainMode;
use near_store::{DBCol, ShardTries, StateSnapshotConfig, Store, StoreConfig, TrieConfig};

use crate::resharding::event_type::{ReshardingEventType, ReshardingSplitShardParams};

// Hashes of the blocks where resharding happened.
// These are from resharding at protocol versions 75, 76, and 78 respectively.
const RESHARDING_BLOCK_HASHES: [&str; 1] = [
    // "CRixt9b6FhASJyTyc8YNj6g7tvjrnf37CSWbhGBwL3EP",
    "ATvDbPZYJSnu2j2CA9Dj7Q6aSBigi2aKuBGxbbnUZthU",
    // "BpuCWLLпMQupM5Dm5VqxpKwZJb6fiFsU5nVhfHkoTQQs",
];

// const RESHARDING_BLOCK_HASHES: [&str; 3] = [
//     "CHCS27e5THXkC5ZQoG7XgY8CakhCPuuBvubP93Avp9X4",
//     "4UzCQ5T2Nk2xAYY1Aakb62wExfZXGcMX2EnnRNhyVoPW",
//     "GxQ3WBW9RFp5L4DTdJa7KRRRCtPZFgNc1L8f9R8AtgKo",
// ];

/// Migrates the database from version 46 to 47.
///
/// This is the migration for the data loss that happened during the recent reshardings. As the
/// resharding implementation does not store any TrieChanges, the trie nodes inserted during
/// resharding are never moved to the cold storage. Once the garbage collection removes those
/// nodes they are lost and the archival node cannot serve some queries for some range of blocks
/// after resharding.
pub fn migrate_46_to_47(
    store: &Store,
    genesis_config: &GenesisConfig,
    store_config: &StoreConfig,
) -> anyhow::Result<()> {
    // // Check if this is a cold store, otherwise early return
    // let db_kind = store.get_db_kind()?;
    // if db_kind != Some(DbKind::Cold) {
    //     tracing::info!(target: "migrations", ?db_kind, "skipping migration 46->47",);
    //     return Ok(());
    // }

    tracing::info!(target: "migrations", "Starting migration 46->47 for cold store");

    let chain_store = store.chain_store();
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), genesis_config, None);
    let tries = ShardTries::new(
        store.trie_store(),
        TrieConfig::from_store_config(store_config),
        FlatStorageManager::new(store.flat_store()),
        StateSnapshotConfig::Disabled,
    );

    let mut transaction = DBTransaction::new();
    for resharding_block_hash in RESHARDING_BLOCK_HASHES {
        tracing::info!(target: "migrations", ?resharding_block_hash, "processing resharding block");

        let resharding_block_hash = CryptoHash::from_str(resharding_block_hash).unwrap();
        let shard_layout =
            epoch_manager.get_shard_layout_from_prev_block(&resharding_block_hash)?;
        let resharding_block = chain_store.get_block_header(&resharding_block_hash)?;
        let resharding_block_info = BlockInfo {
            hash: resharding_block_hash,
            height: resharding_block.height(),
            prev_hash: *resharding_block.prev_hash(),
        };
        let ReshardingEventType::SplitShard(split_shard_params) =
            ReshardingEventType::from_shard_layout(&shard_layout, resharding_block_info)?.unwrap();
        let ReshardingSplitShardParams {
            parent_shard,
            left_child_shard,
            right_child_shard,
            boundary_account,
            ..
        } = split_shard_params;

        let chunk_extra = chain_store.get_chunk_extra(&resharding_block_hash, &parent_shard)?;
        let parent_trie = tries
            .get_trie_for_shard(parent_shard, *chunk_extra.state_root())
            .recording_reads_new_recorder();

        for (new_shard_uid, retain_mode) in
            [(left_child_shard, RetainMode::Left), (right_child_shard, RetainMode::Right)]
        {
            let trie_changes = parent_trie.retain_split_shard(&boundary_account, retain_mode)?;
            let mapped_shard_uid = get_shard_uid_mapping(&store, new_shard_uid);
            for op in trie_changes.insertions() {
                let key = join_two_keys(&mapped_shard_uid.to_bytes(), op.hash().as_bytes());
                let value = op.payload().to_vec();
                rc_aware_set(&mut transaction, DBCol::State, key, value);
            }
        }
    }

    // tracing::info!(target: "migrations", ?transaction, "Writing changes to the database");
    // store.database().write(transaction)?;

    println!("Transactions...............");
    println!("...............");
    println!("...............");
    println!("{:?}", transaction);
    println!("...............");
    println!("...............");

    Ok(())
}
