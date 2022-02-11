use near_store::db::DBCol::ColBlockMisc;
use near_store::Store;
use tracing::{error, info};

const MIGRATION_IS_IN_PROGRESS_STORE_KEY: &'static [u8] = b"migration_is_in_progress";
const STORE_IS_PROBABLY_CORRUPTED_STORE_KEY: &'static [u8] = b"store_is_probably_corrupted";

/// This is a RAII class to mark in the state an ongoing DB migration.
/// It's needed to notify the user about DB probably being corrupted.
/// Possible scenarios: the user interruted node startup during the migration or the migration
/// had panicked.
pub struct MigrationIsInProgress<'a> {
    store: &'a Store,
    current_version: u32,
}

impl<'a> MigrationIsInProgress<'a> {
    pub fn new(store: &'a Store, current_version: u32) -> MigrationIsInProgress {
        let mut store_update = store.store_update();
        store_update.set_ser(ColBlockMisc, &MIGRATION_IS_IN_PROGRESS_STORE_KEY, &true).unwrap();
        store_update.commit().unwrap();
        info!(target: "near", concat!("DB migration (v{} to v{}) is in progress please don't interrupt ",
                      " the node otherwise DB can become corrupted."),
                      current_version, current_version + 1);
        MigrationIsInProgress { store, current_version }
    }
}

impl<'a> Drop for MigrationIsInProgress<'a> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let mut store_update = self.store.store_update();
            store_update
                .set_ser(ColBlockMisc, &MIGRATION_IS_IN_PROGRESS_STORE_KEY, &false)
                .unwrap();
            store_update.commit().unwrap();
            info!(target: "near", "DB migration (v{} to v{}) succeeded.", self.current_version, self.current_version + 1);
        }
    }
}

/// Responsible for notifying the user about previous migrations being interrupted.
/// The behaviour (panic or ignore) can be controlled with the config flag fail_if_migration_is_in_progress.
pub fn check_if_migration_is_in_progress(store: &Store, fail_if_migration_is_in_progress: bool) {
    let store_is_probably_corrupted =
        match store.get_ser::<bool>(ColBlockMisc, &STORE_IS_PROBABLY_CORRUPTED_STORE_KEY) {
            Ok(Some(x)) => x,
            Ok(None) => false,
            Err(e) => panic!("Can't read DB, {:?}", e),
        };
    if store_is_probably_corrupted {
        error!(target: "near", concat!("DB was earlier migrated while another migration was interrupted",
                       " so the current state can be corrupted and lead to undefined failures, ",
                       " please consider recovering the state from backup."));
    }

    let migration_is_in_progress =
        match store.get_ser::<bool>(ColBlockMisc, &MIGRATION_IS_IN_PROGRESS_STORE_KEY) {
            Ok(Some(x)) => x,
            Ok(None) => false,
            Err(e) => panic!("Can't read DB, {:?}", e),
        };

    if migration_is_in_progress {
        error!(target: "near", concat!("DB migration was earlier interrupted so the current state can ",
               "be corrupted, please consider recovering the state from backup."));
        if fail_if_migration_is_in_progress {
            panic!(concat!(
                "Failed to start because DB is probably corrupted if you want to proceed anyway, ",
                "rerun with --fail_if_migration_is_in_progress=false."
            ));
        } else {
            let mut store_update = store.store_update();
            store_update
                .set_ser(ColBlockMisc, &STORE_IS_PROBABLY_CORRUPTED_STORE_KEY, &true)
                .unwrap();
            store_update.commit().unwrap();
        }
    }
}
