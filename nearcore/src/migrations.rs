use near_store::Store;
use near_store::db::metadata::{DB_VERSION, DbVersion, MIN_SUPPORTED_DB_VERSION};

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
            0..MIN_SUPPORTED_DB_VERSION => Err("database version is too old and not supported"),
            MIN_SUPPORTED_DB_VERSION..DB_VERSION => Ok(()),
            _ => unreachable!(),
        }
    }

    fn migrate(&self, _store: &Store, version: DbVersion) -> anyhow::Result<()> {
        match version {
            0..MIN_SUPPORTED_DB_VERSION => unreachable!(),
            45 => Ok(()), // DBCol::StatePartsApplied column added, no need to perform a migration
            46 => Ok(()), // no migration needed for hot store
            DB_VERSION.. => unreachable!(),
        }
    }

    fn migrate_cold(
        &self,
        latest_version_hot_store: &Store,
        cold_store: &Store,
        version: DbVersion,
    ) -> anyhow::Result<()> {
        match version {
            0..MIN_SUPPORTED_DB_VERSION => unreachable!(),
            45 => Ok(()), // DBCol::StatePartsApplied column added, no need to perform a migration
            46 => near_chain::resharding::migrations::migrate_46_to_47(
                latest_version_hot_store,
                cold_store,
                &self.config.genesis.config,
                &self.config.config.store,
            ),
            DB_VERSION.. => unreachable!(),
        }
    }
}
