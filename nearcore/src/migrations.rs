use near_store::Store;
use near_store::db::metadata::{DB_VERSION, DbVersion};

pub(super) struct Migrator<'a> {
    #[allow(dead_code)]
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
            0..=45 => Err("1.45"),
            _ => unreachable!(),
        }
    }

    fn migrate(&self, _store: &Store, version: DbVersion) -> anyhow::Result<()> {
        match version {
            0..=45 => unreachable!(),
            DB_VERSION.. => unreachable!(),
        }
    }
}
