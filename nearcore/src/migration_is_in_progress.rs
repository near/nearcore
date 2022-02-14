use near_store::db::DBCol::ColBlockMisc;
use near_store::Store;
use tracing::{error, info};

const MIGRATION_IS_IN_PROGRESS_STORE_KEY: &'static [u8] = b"migration_is_in_progress";
const STORE_IS_PROBABLY_CORRUPTED_STORE_KEY: &'static [u8] = b"store_is_probably_corrupted";

fn get_bool_flag_from_db(store: &Store, key: &[u8]) -> bool {
    match store.get_ser::<bool>(ColBlockMisc, &key) {
        Ok(Some(x)) => x,
        Ok(None) => false,
        Err(e) => panic!("Can't read DB, {:?}", e),
    }
}

fn set_bool_flag_in_db(store: &Store, key: &[u8], value: bool) {
    let mut store_update = store.store_update();
    store_update.set_ser(ColBlockMisc, &key, &value).unwrap();
    store_update.commit().unwrap();
}

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
        set_bool_flag_in_db(&store, &MIGRATION_IS_IN_PROGRESS_STORE_KEY, true);
        info!(target: "near", concat!("DB migration (v{} to v{}) is in progress please don't interrupt ",
                      " the node otherwise DB can become corrupted."),
                      current_version, current_version + 1);
        MigrationIsInProgress { store, current_version }
    }
}

impl<'a> Drop for MigrationIsInProgress<'a> {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            set_bool_flag_in_db(&self.store, &MIGRATION_IS_IN_PROGRESS_STORE_KEY, false);
            info!(target: "near", "DB migration (v{} to v{}) succeeded.", self.current_version, self.current_version + 1);
        }
    }
}

/// Responsible for notifying the user about previous migrations being interrupted.
/// The behaviour (panic or ignore) can be controlled with the config flag fail_if_migration_is_in_progress.
pub fn check_if_migration_is_in_progress(store: &Store, fail_if_migration_is_in_progress: bool) {
    let store_is_probably_corrupted =
        get_bool_flag_from_db(&store, &STORE_IS_PROBABLY_CORRUPTED_STORE_KEY);
    if store_is_probably_corrupted {
        error!(target: "near", concat!("DB was earlier migrated while another migration was interrupted",
                       " so the current state can be corrupted and lead to undefined failures, ",
                       " please consider recovering the state from backup."));
    }

    let migration_is_in_progress =
        get_bool_flag_from_db(&store, &MIGRATION_IS_IN_PROGRESS_STORE_KEY);
    if migration_is_in_progress {
        error!(target: "near", concat!("DB migration was earlier interrupted so the current state can ",
               "be corrupted, please consider recovering the state from backup."));
        if fail_if_migration_is_in_progress {
            panic!(concat!(
                "Failed to start because DB is probably corrupted if you want to proceed anyway, ",
                "rerun with --fail_if_migration_is_in_progress=false."
            ));
        } else {
            set_bool_flag_in_db(&store, &STORE_IS_PROBABLY_CORRUPTED_STORE_KEY, true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_store::db::TestDB;
    use std::sync::Arc;

    fn create_test_store() -> Store {
        let db = Arc::new(TestDB::new());
        Store::new(db)
    }

    #[test]
    fn test_migration_is_in_progress_set() {
        let store = create_test_store();
        let _migration_is_in_progress = MigrationIsInProgress::new(&store, 1);
        assert_eq!(get_bool_flag_from_db(&store, &MIGRATION_IS_IN_PROGRESS_STORE_KEY), true);
    }

    #[test]
    fn test_migration_is_in_progress_dropped() {
        let store = create_test_store();
        {
            let _migration_is_in_progress = MigrationIsInProgress::new(&store, 1);
        }
        assert_eq!(get_bool_flag_from_db(&store, &MIGRATION_IS_IN_PROGRESS_STORE_KEY), false);
    }

    #[test]
    fn test_check_if_migration_is_in_progress_no_panic() {
        let store = create_test_store();
        let _migration_is_in_progress = MigrationIsInProgress::new(&store, 1);
        check_if_migration_is_in_progress(&store, false);
    }

    #[test]
    #[should_panic]
    fn test_check_if_migration_is_in_progress_panic() {
        let store = create_test_store();
        let _migration_is_in_progress = MigrationIsInProgress::new(&store, 1);
        check_if_migration_is_in_progress(&store, true);
    }

    #[test]
    fn test_store_is_probably_corrupted_set_and_no_panic() {
        let store = create_test_store();
        let _migration_is_in_progress = MigrationIsInProgress::new(&store, 1);
        check_if_migration_is_in_progress(&store, false);
        assert_eq!(get_bool_flag_from_db(&store, &STORE_IS_PROBABLY_CORRUPTED_STORE_KEY), true);
        check_if_migration_is_in_progress(&store, false);
    }
}
