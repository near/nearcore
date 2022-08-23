use std::sync::{Condvar, Mutex};
use once_cell::sync::Lazy;
use tracing::info;

/// Describes number of RocksDB instances and sum of their max_open_files.
///
/// This is modified by [`InstanceTracker`] which uses RAII to automatically
/// update the trackers when new RocksDB instances are created and dropped.
#[derive(Default)]
struct Inner {
    /// Number of RocksDB instances open.
    count: usize,

    /// Sum of max_open_files configuration option of all the RocksDB instances.
    ///
    /// This is used when opening multiple instances to guarantee that process
    /// limit is high enough to allow all of the instances to open the maximum
    /// number of files allowed in their respective configurations.
    ///
    /// For example, if NOFILE limit is set to 1024 and we try to open two
    /// RocksDB instances with max_open_files set to 512 the second open should
    /// fail since the limit is too low to accommodate both databases and other
    /// file descriptor node is opening.
    max_open_files: u64,
}

/// Synchronisation wrapper for accessing [`Inner`] singleton.
#[derive(Default)]
struct State {
    mutex: Mutex<Inner>,
    zero_cvar: Condvar,
}

impl State {
    /// Registers a new RocksDB instance and checks if limits are enough for
    /// given max_open_files configuration option.
    ///
    /// Returns an error if process’ resource limits are too low to allow
    /// max_open_files open file descriptors for the new database instance.
    fn try_new_instance(&self, max_open_files: u32) -> Result<(), String> {
        let num_instances = {
            let mut inner = self.mutex.lock().unwrap();
            let max_open_files = inner.max_open_files + u64::from(max_open_files);
            ensure_max_open_files_limit(max_open_files)?;
            inner.max_open_files = max_open_files;
            inner.count += 1;
            inner.count
        };
        info!(target: "db", num_instances, "Created a new RocksDB instance.");
        Ok(())
    }

    fn drop_instance(&self, max_open_files: u32) {
        let num_instances = {
            let mut inner = self.mutex.lock().unwrap();
            inner.max_open_files = inner.max_open_files.saturating_sub(max_open_files.into());
            inner.count = inner.count.saturating_sub(1);
            if inner.count == 0 {
                self.zero_cvar.notify_all();
            }
            inner.count
        };
        info!(target: "db", num_instances, "Dropped a RocksDB instance.");
    }

    /// Blocks until all RocksDB instances (usually 0 or 1) shut down.
    fn block_until_all_instances_are_dropped(&self) {
        let mut inner = self.mutex.lock().unwrap();
        while inner.count != 0 {
            info!(target: "db", num_instances=inner.count,
                  "Waiting for remaining RocksDB instances to shut down");
            inner = self.zero_cvar.wait(inner).unwrap();
        }
        info!(target: "db", "All RocksDB instances shut down");
    }
}

/// The [`State`] singleton tracking all opened RocksDB instances.
static STATE: Lazy<State> = Lazy::new(Default::default);

/// Blocks until all RocksDB instances (usually 0 or 1) shut down.
pub(super) fn block_until_all_instances_are_dropped() {
    STATE.block_until_all_instances_are_dropped();
}

/// RAII style object which updates the instance tracker stats.
///
/// We’ve seen problems with RocksDB corruptions.  InstanceTracker lets us
/// gracefully shutdown the process letting RocksDB to finish all operations and
/// leaving the instances in a valid non-corrupted state.
pub(super) struct InstanceTracker {
    /// max_open_files configuration of given RocksDB instance.
    max_open_files: u32,
}

impl InstanceTracker {
    /// Registers a new RocksDB instance and checks if limits are enough for
    /// given max_open_files configuration option.
    ///
    /// Returns an error if process’ resource limits are too low to allow
    /// max_open_files open file descriptors for the new database instance.  On
    /// success max_open_files descriptors are ‘reserved’ for this instance so
    /// that creating more instances will take into account the sum of all the
    /// max_open_files options.
    ///
    /// The instance is unregistered once this object is dropped.
    pub(super) fn try_new(max_open_files: u32) -> Result<Self, String> {
        STATE.try_new_instance(max_open_files)?;
        Ok(Self { max_open_files })
    }
}

impl Drop for InstanceTracker {
    /// Deregisters a RocksDB instance and frees its reserved NOFILE limit.
    ///
    /// Returns an error if process’ resource limits are too low to allow
    /// max_open_files open file descriptors for the new database instance.
    ///
    /// The instance is unregistered once this object is dropped.
    fn drop(&mut self) {
        STATE.drop_instance(self.max_open_files);
    }
}

/// Ensures that NOFILE limit can accommodate `max_open_files` plus some small
/// margin of file descriptors.
///
/// A RocksDB instance can keep up to the configured `max_open_files` number of
/// file descriptors.  In addition, we need handful more for other processing
/// (such as network sockets to name just one example).  If NOFILE is too small
/// opening files may start failing which would prevent us from operating
/// correctly.
///
/// To avoid such failures, this method ensures that NOFILE limit is large
/// enough to accommodate `max_open_file` plus another 1000 file descriptors.
/// If current limit is too low, it will attempt to set it to a higher value.
///
/// Returns error if NOFILE limit could not be read or set.  In practice the
/// only thing that can happen is hard limit being too low such that soft limit
/// cannot be increased to required value.
fn ensure_max_open_files_limit(max_open_files: u64) -> Result<(), String> {
    let required = max_open_files.saturating_add(1000);
    if required > i64::MAX as u64 {
        return Err(format!(
            "Unable to change limit for the number of open files (NOFILE): \
             Required limit of {required} is too high"
        ));
    };
    let (soft, hard) = rlimit::Resource::NOFILE.get().map_err(|err| {
        format!("Unable to get limit for the number of open files (NOFILE): {err}")
    })?;
    if required <= soft {
        Ok(())
    } else if required <= hard {
        rlimit::Resource::NOFILE.set(required, hard).map_err(|err| {
            format!(
                "Unable to change limit for the number of open files (NOFILE) \
                 from ({soft}, {hard}) to ({required}, {hard}) (for configured \
                 max_open_files={max_open_files}): {err}"
            )
        })
    } else {
        Err(format!(
            "Hard limit for the number of open files (NOFILE) is too low \
             ({hard}).  At least {required} is required (for configured \
             max_open_files={max_open_files}).  Set ‘ulimit -Hn’ accordingly \
             and restart the node."
        ))
    }
}
