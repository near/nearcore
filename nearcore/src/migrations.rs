use near_chain::{ChainStore, ChainStoreAccess};
use near_primitives::receipt::ReceiptResult;
use near_primitives::runtime::migration_data::MigrationData;
use near_primitives::types::Gas;
use near_primitives::utils::index_to_bytes;
use near_store::metadata::{DbVersion, DB_VERSION};
use near_store::migrations::BatchedStoreUpdate;
use near_store::{DBCol, Store};

/// Fix an issue with block ordinal (#5761)
// This migration takes at least 3 hours to complete on mainnet
pub fn migrate_30_to_31(store: &Store, near_config: &crate::NearConfig) -> anyhow::Result<()> {
    if near_config.client_config.archive && &near_config.genesis.config.chain_id == "mainnet" {
        do_migrate_30_to_31(store, &near_config.genesis.config)?;
    }
    Ok(())
}

/// Migrates the database from version 30 to 31.
///
/// Recomputes block ordinal due to a bug fixed in #5761.
pub fn do_migrate_30_to_31(
    store: &Store,
    genesis_config: &near_chain_configs::GenesisConfig,
) -> anyhow::Result<()> {
    let genesis_height = genesis_config.genesis_height;
    let chain_store = ChainStore::new(store.clone(), genesis_height, false);
    let head = chain_store.head()?;
    let mut store_update = BatchedStoreUpdate::new(store, 10_000_000);
    let mut count = 0;
    // we manually checked mainnet archival data and the first block where the discrepancy happened is `47443088`.
    for height in 47443088..=head.height {
        if let Ok(block_hash) = chain_store.get_block_hash_by_height(height) {
            let block_ordinal = chain_store.get_block_merkle_tree(&block_hash)?.size();
            let block_hash_from_block_ordinal =
                chain_store.get_block_hash_from_ordinal(block_ordinal)?;
            if block_hash_from_block_ordinal != block_hash {
                println!(
                    "Inconsistency in block ordinal to block hash mapping found at block height {}",
                    height
                );
                count += 1;
                store_update
                    .set_ser(DBCol::BlockOrdinal, &index_to_bytes(block_ordinal), &block_hash)
                    .expect("BorshSerialize should not fail");
            }
        }
    }
    println!("total inconsistency count: {}", count);
    store_update.finish()?;
    Ok(())
}

/// In test runs reads and writes here used 442 TGas, but in test on live net migration take
/// between 4 and 4.5s. We do not want to process any receipts in this block
const GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION: Gas = 1_000_000_000_000_000;

pub fn load_migration_data(chain_id: &str) -> MigrationData {
    let is_mainnet = chain_id == "mainnet";
    MigrationData {
        storage_usage_delta: if is_mainnet {
            near_mainnet_res::mainnet_storage_usage_delta()
        } else {
            Vec::new()
        },
        storage_usage_fix_gas: if is_mainnet {
            GAS_USED_FOR_STORAGE_USAGE_DELTA_MIGRATION
        } else {
            0
        },
        restored_receipts: if is_mainnet {
            near_mainnet_res::mainnet_restored_receipts()
        } else {
            ReceiptResult::default()
        },
    }
}

pub(super) struct Migrator<'a> {
    config: &'a crate::config::NearConfig,
}

impl<'a> Migrator<'a> {
    pub fn new(config: &'a crate::config::NearConfig) -> Self {
        Self { config }
    }
}

impl<'a> near_store::StoreMigrator for Migrator<'a> {
    fn check_support(&self, version: DbVersion) -> Result<(), &'static str> {
        // TODO(mina86): Once open ranges in match are stabilised, get rid of
        // this constant and change the match to be 27..DB_VERSION.
        const LAST_SUPPORTED: DbVersion = DB_VERSION - 1;
        match version {
            0..=26 => Err("1.26"),
            27..=LAST_SUPPORTED => Ok(()),
            _ => unreachable!(),
        }
    }

    fn migrate(&self, store: &Store, version: DbVersion) -> anyhow::Result<()> {
        match version {
            0..=31 => unreachable!(),
            32 => near_store::migrations::migrate_32_to_33(store),
            33 => {
                near_store::migrations::migrate_33_to_34(store, self.config.client_config.archive)
            }
            34 => near_store::migrations::migrate_34_to_35(store),
            35 => {
                tracing::info!(target: "migrations", "Migrating DB version from 35 to 36. Flat storage data will be created on disk.");
                tracing::info!(target: "migrations", "It will happen in parallel with regular block processing. ETA is 15h for RPC node and 2d for archival node.");
                Ok(())
            }
            36 => near_store::migrations::migrate_36_to_37(store),
            DB_VERSION.. => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_mainnet_res::mainnet_restored_receipts;
    use near_mainnet_res::mainnet_storage_usage_delta;
    use near_primitives::hash::hash;

    #[test]
    fn test_migration_data() {
        assert_eq!(
            hash(serde_json::to_string(&mainnet_storage_usage_delta()).unwrap().as_bytes())
                .to_string(),
            "2fEgaLFBBJZqgLQEvHPsck4NS3sFzsgyKaMDqTw5HVvQ"
        );
        let mainnet_migration_data = load_migration_data("mainnet");
        assert_eq!(mainnet_migration_data.storage_usage_delta.len(), 3112);
        let testnet_migration_data = load_migration_data("testnet");
        assert_eq!(testnet_migration_data.storage_usage_delta.len(), 0);
    }

    #[test]
    fn test_restored_receipts_data() {
        assert_eq!(
            hash(serde_json::to_string(&mainnet_restored_receipts()).unwrap().as_bytes())
                .to_string(),
            "48ZMJukN7RzvyJSW9MJ5XmyQkQFfjy2ZxPRaDMMHqUcT"
        );
        let mainnet_migration_data = load_migration_data("mainnet");
        assert_eq!(mainnet_migration_data.restored_receipts.get(&0u64).unwrap().len(), 383);
        let testnet_migration_data = load_migration_data("testnet");
        assert!(testnet_migration_data.restored_receipts.is_empty());
    }
}
