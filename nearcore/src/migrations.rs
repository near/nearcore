use near_chain::{ChainStore, ChainStoreAccess};
use near_primitives::receipt::ReceiptResult;
use near_primitives::runtime::migration_data::MigrationData;
use near_primitives::types::Gas;
use near_primitives::utils::index_to_bytes;
use near_store::migrations::{set_store_version, BatchedStoreUpdate};
use near_store::DBCol;

lazy_static_include::lazy_static_include_bytes! {
    /// File with receipts which were lost because of a bug in apply_chunks to the runtime config.
    /// Follows the ReceiptResult format which is HashMap<ShardId, Vec<Receipt>>.
    /// See https://github.com/near/nearcore/pull/4248/ for more details.
    MAINNET_RESTORED_RECEIPTS => "res/mainnet_restored_receipts.json",
}

/// Fix an issue with block ordinal (#5761)
// This migration takes at least 3 hours to complete on mainnet
pub fn migrate_30_to_31(store_opener: &near_store::StoreOpener, near_config: &crate::NearConfig) {
    let store = store_opener.open();
    if near_config.client_config.archive && &near_config.genesis.config.chain_id == "mainnet" {
        let genesis_height = near_config.genesis.config.genesis_height;
        let chain_store = ChainStore::new(store.clone(), genesis_height, false);
        let head = chain_store.head().unwrap();
        let mut store_update = BatchedStoreUpdate::new(&store, 10_000_000);
        let mut count = 0;
        // we manually checked mainnet archival data and the first block where the discrepancy happened is `47443088`.
        for height in 47443088..=head.height {
            if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
                let block_ordinal = chain_store.get_block_merkle_tree(&block_hash).unwrap().size();
                let block_hash_from_block_ordinal =
                    chain_store.get_block_hash_from_ordinal(block_ordinal).unwrap();
                if block_hash_from_block_ordinal != block_hash {
                    println!("Inconsistency in block ordinal to block hash mapping found at block height {}", height);
                    count += 1;
                    store_update
                        .set_ser(DBCol::BlockOrdinal, &index_to_bytes(block_ordinal), &block_hash)
                        .expect("BorshSerialize should not fail");
                }
            }
        }
        println!("total inconsistency count: {}", count);
        store_update.finish().expect("Failed to migrate");
    }
    set_store_version(&store, 31);
}

lazy_static_include::lazy_static_include_bytes! {
    /// File with account ids and deltas that need to be applied in order to fix storage usage
    /// difference between actual and stored usage, introduced due to bug in access key deletion,
    /// see https://github.com/near/nearcore/issues/3824
    /// This file was generated using tools/storage-usage-delta-calculator
    MAINNET_STORAGE_USAGE_DELTA => "res/storage_usage_delta.json",
}

/// In test runs reads and writes here used 442 TGas, but in test on live net migration take
/// between 4 and 4.5s. We do not want to process any receipts in this block
const GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION: Gas = 1_000_000_000_000_000;

pub fn load_migration_data(chain_id: &str) -> MigrationData {
    let is_mainnet = chain_id == "mainnet";
    MigrationData {
        storage_usage_delta: if is_mainnet {
            serde_json::from_slice(&MAINNET_STORAGE_USAGE_DELTA).unwrap()
        } else {
            Vec::new()
        },
        storage_usage_fix_gas: if is_mainnet {
            GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION
        } else {
            0
        },
        restored_receipts: if is_mainnet {
            serde_json::from_slice(&MAINNET_RESTORED_RECEIPTS)
                .expect("File with receipts restored after apply_chunks fix have to be correct")
        } else {
            ReceiptResult::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::hash::hash;
    use near_primitives::serialize::to_base;

    #[test]
    fn test_migration_data() {
        assert_eq!(
            to_base(&hash(&MAINNET_STORAGE_USAGE_DELTA)),
            "6CFkdSZZVj4v83cMPD3z6Y8XSQhDh3EQjFh3PRAqFEAx"
        );
        let mainnet_migration_data = load_migration_data("mainnet");
        assert_eq!(mainnet_migration_data.storage_usage_delta.len(), 3112);
        let testnet_migration_data = load_migration_data("testnet");
        assert_eq!(testnet_migration_data.storage_usage_delta.len(), 0);
    }

    #[test]
    fn test_restored_receipts_data() {
        assert_eq!(
            to_base(&hash(&MAINNET_RESTORED_RECEIPTS)),
            "3ZHK51a2zVnLnG8Pq1y7fLaEhP9SGU1CGCmspcBUi5vT"
        );
        let mainnet_migration_data = load_migration_data("mainnet");
        assert_eq!(mainnet_migration_data.restored_receipts.get(&0u64).unwrap().len(), 383);
        let testnet_migration_data = load_migration_data("testnet");
        assert!(testnet_migration_data.restored_receipts.is_empty());
    }
}
