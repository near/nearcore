//! Epoch sync data reset for nodes that have fallen too far behind.
//!
//! When a node is so far behind the network that epoch sync classifies it as a
//! "stale node" (its stored data cannot have an epoch sync proof applied in
//! place), the only recovery is to wipe the data directory and re-bootstrap
//! from genesis. The `ClientActor` requests this by sending
//! [`ShutdownReason::EpochSyncDataReset`] on its shutdown signal; the actual
//! wipe-and-restart is performed here.
//!
//! The `neard` binary drives this itself (it owns the shutdown signal and its
//! process lifecycle). Embedders that start a node via
//! [`crate::start_with_config`] do not provide a shutdown signal, so without
//! this they would loop on epoch sync forever. Such embedders can opt in by
//! setting `epoch_sync.automatic_data_reset = true` in their config, which
//! makes [`crate::start_with_config_and_synchronization`] install
//! [`spawn_automatic_reset_handler`] on their behalf — no reset logic lives in
//! the embedding binary.

use crate::config::NearConfig;
use near_client::client_actor::ShutdownReason;
use std::path::{Path, PathBuf};
use tokio::sync::broadcast;

/// Marker file written inside the hot store directory to signal that the data
/// directory should be wiped on the next startup for epoch sync re-bootstrap.
pub const EPOCH_SYNC_DATA_RESET_MARKER_FILE_NAME: &str = ".EPOCH_SYNC_DATA_RESET";

/// Resolves the hot store path from the node config, matching `open_storage`.
pub fn hot_store_path(home_dir: &Path, near_config: &NearConfig) -> PathBuf {
    home_dir.join(near_config.config.store.path.as_deref().unwrap_or_else(|| Path::new("data")))
}

/// Checks for the epoch sync data reset marker and deletes the data directory if
/// found. Must be called before the store is opened. No-op when the marker is
/// absent. Archival nodes skip deletion to prevent accidental data loss.
pub fn clear_data_dir_if_reset_marker_present(hot_store_path: &Path, is_archival: bool) {
    let marker_path = hot_store_path.join(EPOCH_SYNC_DATA_RESET_MARKER_FILE_NAME);
    if !marker_path.exists() {
        return;
    }
    if is_archival {
        tracing::warn!(
            target: "epoch_sync_reset",
            "epoch sync data reset marker found but node is archival, ignoring"
        );
    } else {
        tracing::info!(
            target: "epoch_sync_reset",
            ?hot_store_path,
            "epoch sync data reset marker found, deleting data directory"
        );
        std::fs::remove_dir_all(hot_store_path)
            .expect("failed to delete data directory for epoch sync reset");
    }
}

/// Writes the epoch sync data reset marker file inside the hot store directory.
/// On next startup, this marker signals that the data directory should be wiped.
pub fn write_epoch_sync_data_reset_marker(hot_store_path: &Path) {
    let marker_path = hot_store_path.join(EPOCH_SYNC_DATA_RESET_MARKER_FILE_NAME);
    // Ensure the data directory exists (it should, since we're running).
    std::fs::create_dir_all(hot_store_path)
        .expect("failed to create data directory for reset marker");
    std::fs::write(&marker_path, b"").expect("failed to write epoch sync reset marker");
    // Fsync the marker file and the parent directory to ensure the directory
    // entry is durably persisted (fsync on the file alone is not sufficient
    // on all filesystems).
    std::fs::File::open(&marker_path)
        .and_then(|f| f.sync_all())
        .expect("failed to fsync reset marker file");
    std::fs::File::open(hot_store_path)
        .and_then(|d| d.sync_all())
        .expect("failed to fsync hot store directory");
    tracing::info!(target: "epoch_sync_reset", ?marker_path, "epoch sync data reset marker written");
}

/// Creates a shutdown signal channel and spawns a task that, on
/// [`ShutdownReason::EpochSyncDataReset`], writes the reset marker and restarts
/// the process so the data directory is wiped on the next startup. Returns the
/// sender to hand to the client.
///
/// This is what lets an embedder recover without implementing any reset logic
/// itself: it only has to opt in via config.
pub fn spawn_automatic_reset_handler(hot_store_path: PathBuf) -> broadcast::Sender<ShutdownReason> {
    let (tx, mut rx) = broadcast::channel::<ShutdownReason>(16);
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(ShutdownReason::EpochSyncDataReset) => {
                    tracing::warn!(
                        target: "epoch_sync_reset",
                        "node is stale; writing epoch sync data reset marker and restarting to wipe the data directory"
                    );
                    write_epoch_sync_data_reset_marker(&hot_store_path);
                    // Replaces the process image; the marker triggers the wipe on
                    // the next startup. Returns only on non-unix or on failure.
                    exec_restart();
                    return;
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Closed) => return,
                Err(broadcast::error::RecvError::Lagged(_)) => {}
            }
        }
    });
    tx
}

/// On non-unix platforms, exec is not available.
#[cfg(not(unix))]
pub fn exec_restart() {
    tracing::warn!(
        target: "epoch_sync_reset",
        "automatic restart after epoch sync data reset is not supported on this platform, \
         please restart manually"
    );
}

/// Re-execs the current process with the same arguments.
/// On success, this function never returns (the process image is replaced).
#[cfg(unix)]
pub fn exec_restart() {
    use std::env::{args_os, current_exe};
    use std::os::unix::process::CommandExt;
    use std::process::Command;

    let binary = current_exe().expect("failed to determine current executable path");
    let args: Vec<_> = args_os().skip(1).collect();

    tracing::info!(target: "epoch_sync_reset", ?binary, ?args, "restarting process after epoch sync data reset");

    // exec() replaces the process image. If it returns, it failed.
    let err = Command::new(&binary).args(&args).exec();
    panic!("failed to exec {:?}: {}", binary, err);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_data_dir_wipes_when_marker_present() {
        // Given a data dir containing a payload file and the reset marker.
        let tmp = tempfile::tempdir().unwrap();
        let hot_store_path = tmp.path().join("data");
        std::fs::create_dir_all(&hot_store_path).unwrap();
        std::fs::write(hot_store_path.join("SOME_DB_FILE"), b"payload").unwrap();
        write_epoch_sync_data_reset_marker(&hot_store_path);

        // When clearing on a non-archival node.
        clear_data_dir_if_reset_marker_present(&hot_store_path, false);

        // Then the whole data dir is gone.
        assert!(!hot_store_path.exists());
    }

    #[test]
    fn clear_data_dir_noop_without_marker() {
        // Given a data dir with no marker.
        let tmp = tempfile::tempdir().unwrap();
        let hot_store_path = tmp.path().join("data");
        std::fs::create_dir_all(&hot_store_path).unwrap();
        std::fs::write(hot_store_path.join("SOME_DB_FILE"), b"payload").unwrap();

        // When clearing.
        clear_data_dir_if_reset_marker_present(&hot_store_path, false);

        // Then nothing is deleted.
        assert!(hot_store_path.join("SOME_DB_FILE").exists());
    }

    #[test]
    fn clear_data_dir_skips_for_archival() {
        // Given an archival node whose data dir carries the marker.
        let tmp = tempfile::tempdir().unwrap();
        let hot_store_path = tmp.path().join("data");
        std::fs::create_dir_all(&hot_store_path).unwrap();
        std::fs::write(hot_store_path.join("SOME_DB_FILE"), b"payload").unwrap();
        write_epoch_sync_data_reset_marker(&hot_store_path);

        // When clearing with is_archival = true.
        clear_data_dir_if_reset_marker_present(&hot_store_path, true);

        // Then the data dir is preserved (archival nodes never auto-wipe).
        assert!(hot_store_path.join("SOME_DB_FILE").exists());
    }
}
