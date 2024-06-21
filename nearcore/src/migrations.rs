use near_chain::{ChainStore, ChainStoreAccess};
use near_primitives::utils::index_to_bytes;
use near_store::metadata::{DbVersion, DB_VERSION};
use near_store::migrations::BatchedStoreUpdate;
use near_store::{DBCol, Store};

/// Fix an issue with block ordinal (#5761)
// This migration takes at least 3 hours to complete on mainnet
pub fn migrate_30_to_31(store: &Store, near_config: &crate::NearConfig) -> anyhow::Result<()> {
    if near_config.client_config.archive
        && &near_config.genesis.config.chain_id == near_primitives::chains::MAINNET
    {
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
            37 => near_store::migrations::migrate_37_to_38(store),
            38 => near_store::migrations::migrate_38_to_39(store),
            DB_VERSION.. => unreachable!(),
        }
    }
}
