use std::sync::{Condvar, Mutex};
use tracing::info;

/// Describes number of RocksDB instances and sum of their max_open_files.
///
/// This is modified by [`InstanceTracker`] which uses RAII to automatically
/// update the trackers when new RocksDB instances are opened and closed.
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
    ///
    /// Note that there is a bit of shenanigans around types.  We’re using `u64`
    /// here but `u32` in [`crate::config::StoreConfig`].  The latter uses `u32`
    /// because we don’t want to deal with `-1` having special meaning in
    /// RocksDB so we opted to for unsigned type but then needed to limit it
    /// since RocksDB doesn’t accept full unsigned range.  Here, we’re using
    /// `u64` because that’s what ends up being passed to setrlimit so we would
    /// need to cast anyway and it allows us not to worry about summing multiple
    /// limits from StoreConfig.
    max_open_files: u64,
}

/// Synchronisation wrapper for accessing [`Inner`] singleton.
struct State {
    inner: Mutex<Inner>,
    zero_cvar: Condvar,
}

impl State {
    /// Creates a new instance with counts initialised to zero.
    const fn new() -> Self {
        let inner = Inner { count: 0, max_open_files: 0 };
        Self { inner: Mutex::new(inner), zero_cvar: Condvar::new() }
    }

    /// Registers a new RocksDB instance and checks if limits are enough for
    /// given max_open_files configuration option.
    ///
    /// Returns an error if process’ resource limits are too low to allow
    /// max_open_files open file descriptors for the new database instance.
    fn try_new_instance(&self, max_open_files: u32) -> Result<(), String> {
        let num_instances = {
            let mut inner = self.inner.lock().unwrap();
            let max_open_files = inner.max_open_files + u64::from(max_open_files);
            ensure_max_open_files_limit(RealNoFile, max_open_files)?;
            inner.max_open_files = max_open_files;
            inner.count += 1;
            inner.count
        };
        info!(target: "db", num_instances, "Opened a new RocksDB instance.");
        Ok(())
    }

    fn close_instance(&self, max_open_files: u32) {
        let num_instances = {
            let mut inner = self.inner.lock().unwrap();
            inner.max_open_files = inner.max_open_files.saturating_sub(max_open_files.into());
            inner.count = inner.count.saturating_sub(1);
            if inner.count == 0 {
                self.zero_cvar.notify_all();
            }
            inner.count
        };
        info!(target: "db", num_instances, "Closed a RocksDB instance.");
    }

    /// Blocks until all RocksDB instances (usually 0 or 1) shut down.
    fn block_until_all_instances_are_closed(&self) {
        let mut inner = self.inner.lock().unwrap();
        while inner.count != 0 {
            info!(target: "db", num_instances=inner.count,
                  "Waiting for remaining RocksDB instances to close");
            inner = self.zero_cvar.wait(inner).unwrap();
        }
        info!(target: "db", "All RocksDB instances closed.");
    }
}

/// The [`State`] singleton tracking all opened RocksDB instances.
static STATE: State = State::new();

/// Blocks until all RocksDB instances (usually 0 or 1) shut down.
pub(super) fn block_until_all_instances_are_closed() {
    STATE.block_until_all_instances_are_closed();
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
        STATE.close_instance(self.max_open_files);
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
fn ensure_max_open_files_limit(mut nofile: impl NoFile, max_open_files: u64) -> Result<(), String> {
    let required = max_open_files.saturating_add(1000);
    if required > i64::MAX as u64 {
        return Err(format!(
            "Unable to change limit for the number of open files (NOFILE): \
             Required limit of {required} is too high"
        ));
    };
    let (soft, hard) = nofile.get().map_err(|err| {
        format!("Unable to get limit for the number of open files (NOFILE): {err}")
    })?;
    if required <= soft {
        Ok(())
    } else if required <= hard {
        nofile.set(required, hard).map_err(|err| {
            format!(
                "Unable to change limit for the number of open files (NOFILE) \
                 from ({soft}, {hard}) to ({required}, {hard}): {err}"
            )
        })
    } else {
        Err(format!(
            "Hard limit for the number of open files (NOFILE) is too low \
             ({hard}).  At least {required} is required.  Set ‘ulimit -Hn’ \
             accordingly and restart the node."
        ))
    }
}

/// Interface for accessing the NOFILE resource limit.
///
/// The essentially trait exists for testing only.  It allows
/// [`ensure_max_open_files_limit`] to be parameterised such
/// that it access mock limits rather than real ones.
trait NoFile {
    fn get(&self) -> std::io::Result<(u64, u64)>;
    fn set(&mut self, soft: u64, hard: u64) -> std::io::Result<()>;
}

/// Access to the real process NOFILE resource limit.
struct RealNoFile;

impl NoFile for RealNoFile {
    fn get(&self) -> std::io::Result<(u64, u64)> {
        rlimit::Resource::NOFILE.get()
    }

    fn set(&mut self, soft: u64, hard: u64) -> std::io::Result<()> {
        rlimit::Resource::NOFILE.set(soft, hard)
    }
}

#[test]
fn test_ensure_max_open_files_limit() {
    fn other_error(msg: &str) -> std::io::Error {
        super::other_error(msg.to_string())
    }

    /// Mock implementation of NoFile interface.
    struct MockNoFile<'a>(&'a mut (u64, u64));

    impl<'a> NoFile for MockNoFile<'a> {
        fn get(&self) -> std::io::Result<(u64, u64)> {
            if self.0 .0 == 666 {
                Err(other_error("error"))
            } else {
                Ok(*self.0)
            }
        }

        fn set(&mut self, soft: u64, hard: u64) -> std::io::Result<()> {
            let (old_soft, old_hard) = self.get().unwrap();
            if old_hard == 666000 {
                Err(other_error("error"))
            } else {
                assert!(soft != old_soft, "Pointless call to set");
                *self.0 = (soft, hard);
                Ok(())
            }
        }
    }

    // We impose limit at i64::MAX and don’t even talk to the system if
    // number above that is requested.
    let msg = ensure_max_open_files_limit(MockNoFile(&mut (666, 666)), u64::MAX).unwrap_err();
    assert!(msg.contains("is too high"), "{msg}");

    // Error on get
    let msg = ensure_max_open_files_limit(MockNoFile(&mut (666, 666)), 1024).unwrap_err();
    assert!(msg.starts_with("Unable to get"), "{msg}");

    // Everything’s fine, soft limit should stay as it is.
    let mut state = (2048, 666000);
    ensure_max_open_files_limit(MockNoFile(&mut state), 1024).unwrap();
    assert_eq!((2048, 666000), state);

    // Should rise the soft limit.
    let mut state = (1024, 10000);
    ensure_max_open_files_limit(MockNoFile(&mut state), 1024).unwrap();
    assert_eq!((2024, 10000), state);

    // Should recognise trying to rise is futile because hard limit is too low.
    let mut state = (1024, 2000);
    let msg = ensure_max_open_files_limit(MockNoFile(&mut state), 1024).unwrap_err();
    assert!(msg.starts_with("Hard limit"), "{msg}");
    assert_eq!((1024, 2000), state);

    // Error trying to change the limit.
    let mut state = (1024, 666000);
    let msg = ensure_max_open_files_limit(MockNoFile(&mut state), 1024).unwrap_err();
    assert!(msg.starts_with("Unable to change"), "{msg}");
    assert_eq!((1024, 666000), state);
}
