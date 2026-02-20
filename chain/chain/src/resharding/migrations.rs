use std::str::FromStr;

use near_chain_configs::GenesisConfig;
use near_primitives::chains::MAINNET;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::get_block_shard_uid;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_store::adapter::StoreAdapter;
use near_store::adapter::trie_store::get_shard_uid_mapping;
use near_store::archive::cold_storage::{join_two_keys, rc_aware_set};
use near_store::db::{ColdDB, DBTransaction, Database};
use near_store::flat::{BlockInfo, FlatStorageManager};
use near_store::trie::ops::resharding::RetainMode;
use near_store::{DBCol, ShardTries, StateSnapshotConfig, Store, StoreConfig, TrieConfig};
use near_vm_runner::logic::ProtocolVersion;

use crate::resharding::event_type::{ReshardingEventType, ReshardingSplitShardParams};

/// Hashes of the blocks where resharding happened on mainnet.
/// These are from resharding at protocol versions 75, 76, and 78 respectively.
const MAINNET_RESHARDING_BLOCK_HASHES: [(ProtocolVersion, &str); 3] = [
    (75, "CRixt9b6FhASJyTyc8YNj6g7tvjrnf37CSWbhGBwL3EP"),
    (76, "ATvDbPZYJSnu2j2CA9Dj7Q6aSBigi2aKuBGxbbnUZthU"),
    (78, "BpuCWLLnMQupM5Dm5VqxpKwZJb6fiFsU5nVhfHkoTQQs"),
];

/// Migrates the database from version 46 to 47.
///
/// This is the migration for the data loss that happened during the recent reshardings. As the
/// resharding implementation does not store any TrieChanges, the trie nodes inserted during
/// resharding are never moved to the cold storage. Once the garbage collection removes those
/// nodes they are lost and the archival node cannot serve some queries for some range of blocks
/// after resharding.
///
/// Note: This migration only applies to cold stores, and is only for mainnet resharding events.
pub fn migrate_46_to_47(
    hot_store: &Store,
    cold_db: Option<&ColdDB>,
    genesis_config: &GenesisConfig,
    store_config: &StoreConfig,
) -> anyhow::Result<()> {
    let Some(cold_db) = cold_db else {
        tracing::info!(target: "migrations", "skipping migration 46->47 for hot store only",);
        return Ok(());
    };

    // Current migration is targeted only for mainnet
    if genesis_config.chain_id != MAINNET {
        tracing::info!(target: "migrations", chain_id = ?genesis_config.chain_id, "skipping migration 46->47",);
        return Ok(());
    }

    tracing::info!(target: "migrations", "starting migration 46->47 for cold store");

    let cold_store = cold_db.as_store();
    let epoch_config_store =
        EpochConfigStore::for_chain_id(&genesis_config.chain_id, None).unwrap();
    let tries = ShardTries::new(
        cold_store.trie_store(),
        TrieConfig::from_store_config(store_config),
        FlatStorageManager::new(cold_store.flat_store()),
        StateSnapshotConfig::Disabled,
    );

    let mut transaction = DBTransaction::new();
    for (protocol_version, resharding_block_hash) in MAINNET_RESHARDING_BLOCK_HASHES {
        tracing::info!(target: "migrations", ?resharding_block_hash, "processing resharding block");

        let resharding_block_hash = CryptoHash::from_str(resharding_block_hash).unwrap();
        let shard_layout = &epoch_config_store
            .get_config(protocol_version)
            .static_shard_layout()
            .ok_or_else(|| {
            anyhow::anyhow!("static shard layout expected for protocol version {protocol_version}")
        })?;
        let resharding_block = hot_store.chain_store().get_block_header(&resharding_block_hash)?;
        let resharding_block_info = BlockInfo {
            hash: resharding_block_hash,
            height: resharding_block.height(),
            prev_hash: *resharding_block.prev_hash(),
        };
        let ReshardingEventType::SplitShard(split_shard_params) =
            ReshardingEventType::from_shard_layout(shard_layout, resharding_block_info)?.unwrap();
        let ReshardingSplitShardParams {
            parent_shard,
            left_child_shard,
            right_child_shard,
            boundary_account,
            ..
        } = split_shard_params;

        let chunk_extra: ChunkExtra = cold_store
            .get_ser(DBCol::ChunkExtra, &get_block_shard_uid(&resharding_block_hash, &parent_shard))
            .unwrap();
        let parent_trie = tries
            .get_trie_for_shard(parent_shard, *chunk_extra.state_root())
            .recording_reads_new_recorder();

        for (new_shard_uid, retain_mode) in
            [(left_child_shard, RetainMode::Left), (right_child_shard, RetainMode::Right)]
        {
            let trie_changes = parent_trie.retain_split_shard(&boundary_account, retain_mode)?;
            let mapped_shard_uid = get_shard_uid_mapping(&cold_store, new_shard_uid);
            for op in trie_changes.insertions() {
                let key = join_two_keys(&mapped_shard_uid.to_bytes(), op.hash().as_bytes());
                let value = op.payload().to_vec();
                rc_aware_set(&mut transaction, DBCol::State, key, value);
            }
        }
    }

    tracing::info!(target: "migrations", "Writing changes to the database");
    cold_db.write(transaction);

    Ok(())
}
