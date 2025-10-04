use near_store::Store;
use near_store::db::metadata::{DB_VERSION, DbVersion};

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
        match version {
            0..=26 => Err("1.26"),
            27..DB_VERSION => Ok(()),
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
            39 => near_store::migrations::migrate_39_to_40(store),
            40 => near_store::migrations::migrate_40_to_41(store),
            41 => near_store::migrations::migrate_41_to_42(store),
            42 => near_store::migrations::migrate_42_to_43(store),
            43 => Ok(()), // DBCol::ChunkApplyStats column added, no need to perform a migration
            44 => near_store::migrations::migrate_44_to_45(store),
            45 => Ok(()), // DBCol::StatePartsApplied column added, no need to perform a migration
            DB_VERSION.. => unreachable!(),
        }
    }
}
