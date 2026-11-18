// cspell:ignore NOENT, RDONLY, RGRP, RUSR, TRUNC, WGRP, WRONLY, WUSR
// cspell:ignore fstat, openat, renameat, unlinkat

use crate::{BackgroundJobSpawner, LruWeightedCache};
use parking_lot::Mutex;
use rand::Rng as _;
use rustix::fd::OwnedFd;
use rustix::fs::{
    Access, AtFlags, Dir, Mode, OFlags, Timespec, Timestamps, UTIME_NOW, UTIME_OMIT, accessat,
    fstat, open, openat, renameat, statat, unlinkat, utimensat,
};
use rustix::io::Errno;
use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A directory of files, one per key, with a size-bounded, best-effort eviction policy.
///
/// Files are tracked by key and on-disk byte size; LRU victims are unlinked on `commit_file`.
///
/// The key is the name of the file: it has to round-trip through [`Display`] and [`FromStr`].
pub struct LruFileCache<K> {
    /// file descriptor to the cache directory
    dir: OwnedFd,
    /// Tracks files present in `dir`, keyed by the same `CryptoHash` as the
    /// on-disk filename, weighted by on-disk byte size. The value is the
    /// instant the entry's on-disk atime was last refreshed/created.
    disk_index: Mutex<LruWeightedCache<CryptoHash, Instant>>,
    /// Minimum age of an entry's last atime on-disk refresh before `touch` enqueues another.
    /// Mirrors `relatime` behavior.
    access_time_refresh_throttle: Duration,
    /// Off-loads the on-disk atime refresh; see [`BackgroundJobSpawner`].
    bg_spawner: BackgroundJobSpawner,
}

/// A file of a [`LruFileCache`] that is being written.
///
/// Created with [`LruFileCache::create_file`]. The file is stored in the cache only once the
/// writer is passed to [`LruFileCache::commit_file`]. A writer that is dropped instead leaves
/// its temporary file behind.
#[must_use = "the file is not stored until the writer is passed to `LruFileCache::commit_file`"]
pub struct LruFileWriter<K> {
    key: K,
    temp_file_name: String,
    temp_file: std::fs::File,
}

impl<K> Write for LruFileWriter<K> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.temp_file.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.temp_file.write_all(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.temp_file.flush()
    }
}

/// Default minimum age of a tracked entry's last atime refresh before [`touch`] will enqueue another one.
///
/// [`touch`]: FilesystemContractRuntimeCache::touch
const ACCESS_TIME_REFRESH_THROTTLE: Duration = Duration::from_mins(10);

impl<K: Copy + Eq + Hash + Display + FromStr + Send + 'static> LruFileCache<K> {
    /// Largest accepted value for the size limit
    pub const MAX_BYTES: u64 = LruWeightedCache::<K, ()>::MAX_WEIGHT;

    /// Opens the directory at `path`, creating it if needed, and indexes the files in it.
    ///
    /// `max_bytes` is the maximum total size of the files kept in the directory. Panics if it
    /// is larger than [`Self::MAX_BYTES`].
    pub fn open(
        path: &Path,
        max_bytes: u64,
        bg_spawner: BackgroundJobSpawner,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(&path)?;
        let dir = open(&path, OFlags::DIRECTORY, Mode::empty())?;
        let disk_index = Mutex::new(build_disk_index(&dir, max_disk_cache_bytes)?);
        Ok(Self {
            dir,
            disk_index,
            access_time_refresh_throttle: ACCESS_TIME_REFRESH_THROTTLE,
            bg_spawner,
        })
    }

    /// TEST ONLY: override the atime-refresh throttle window. Setting it to
    /// [`Duration::ZERO`] makes every tracked key eligible on the next
    /// [`Self::touch`] — deterministic, with no `Instant` arithmetic that could
    /// underflow. Must be called before the cache is shared.
    #[cfg(test)]
    fn test_set_access_time_refresh_throttle(&mut self, throttle: Duration) {
        Arc::get_mut(&mut self.state)
            .expect("throttle must be set before the cache is shared")
            .access_time_refresh_throttle = throttle;
    }

    /// Stamp `key`'s on-disk file with the current access time, leaving its
    /// modification time untouched. Best-effort: failures are logged and dropped.
    fn refresh_disk_atime(&self, key: &CryptoHash) {
        let filename = key.to_string();
        let times = Timestamps {
            last_access: Timespec { tv_sec: 0, tv_nsec: UTIME_NOW },
            last_modification: Timespec { tv_sec: 0, tv_nsec: UTIME_OMIT },
        };
        match utimensat(&self.state.dir, &filename, &times, AtFlags::empty()) {
            // Success, or the file was evicted/removed out from under us.
            Ok(()) | Err(Errno::NOENT) => {}
            Err(err) => tracing::debug!(
                target: "vm",
                key = %key,
                err = &err as &dyn std::error::Error,
                "failed to refresh contract cache file atime",
            ),
        }
    }

    /// Creates a temporary file for `key` that can be written to.
    ///
    /// Returns a writer where the caller can write the contents of the file.
    ///
    /// Nothing is stored under `key` until the returned writer is committed via
    /// [`Self::commit_file`].
    pub fn create_file(&self, key: &K) -> std::io::Result<LruFileWriter<K>> {
        const MAX_ATTEMPTS: u32 = 5;
        let final_filename = key.to_string();
        let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
        let flags = OFlags::CREATE | OFlags::TRUNC | OFlags::WRONLY;
        let mut attempt = 0;
        let (temp_filename, mut file) = loop {
            attempt += 1;
            let mut temporary_filename = final_filename.clone();
            temporary_filename.push('.');
            for b in rand::thread_rng().sample_iter(rand::distributions::Alphanumeric).take(8) {
                temporary_filename.push(b as char);
            }
            temporary_filename.push_str(".temp");
            match openat(&self.state.dir, &temporary_filename, flags, mode) {
                Ok(f) => break (temporary_filename, std::fs::File::from(f)),
                Err(e) if attempt > MAX_ATTEMPTS => return Err(e.into()),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            }
        };
        Ok(LruFileWriter { key: *key, temp_file_name: temp_filename, temp_file: file })
    }

    /// Takes the temporary file returned by [`Self::create_file`] and commits
    /// it to the cache.
    ///
    /// The file is committed by renaming it into its final location and
    /// updating the on-disk LRU index.
    ///
    /// `weight` is the total number of bytes written to `writer`. Used to
    /// weight the on-disk LRU index.
    pub fn commit_file(&self, writer: LruFileWriter<K>, weight: u64) -> std::io::Result<()> {
        let key = &writer.key;
        let final_filename = key.to_string();
        let temp_filename = writer.temp_file_name;
        let file = writer.temp_file;
        file.sync_data()?;
        drop(file);
        // Rename, index update, and victim unlinks share one lock, so a
        // concurrent `put` of a victim key can't rename a fresh file into place
        // before our unlink and have us delete it. The write above is outside
        // the lock.
        {
            let mut index = self.state.disk_index.lock();
            renameat(&self.state.dir, temp_filename, &self.state.dir, final_filename)?;
            // We just wrote the file, so its atime is current as of now.
            let evicted = index.insert(*key, weight, Instant::now());
            // `insert` returns `key` for a same-key replacement (keep our file)
            // and for an oversized reject (remove it); `contains` tells them apart.
            let key_stored = index.contains(key);
            for (victim, _) in evicted {
                if &victim == key && key_stored {
                    continue;
                }
                match unlinkat(&self.state.dir, victim.to_string(), AtFlags::empty()) {
                    Ok(()) | Err(Errno::NOENT) => {}
                    Err(err) => tracing::debug!(
                        target: "vm",
                        victim = %victim,
                        err = &err as &dyn std::error::Error,
                        "failed to unlink evicted compiled-contract cache file; on-disk cache may exceed its size limit"
                    ),
                }
            }
        }

        // NOTE: we do not remove the temporary file in case of failure in many of the
        // intermediate steps above. This is not considered to be a significant risk: any failure
        // here will result in the node terminating anyway, so the operator will have to fix the
        // issue(s) before too many temporary files gather up in the cache.
        //
        // (Operators are also somewhat encouraged to occasionally clear up their cache.)
        Ok(())
    }

    /// Reads the whole file for `key`. Returns `None` if there is no such file.
    pub fn read(&self, key: &K) -> std::io::Result<Option<Vec<u8>>> {
        let filename = key.to_string();
        let mode = Mode::empty();
        let flags = OFlags::RDONLY;
        let file = openat(&self.state.dir, &filename, flags, mode);
        let file = match file {
            Err(Errno::NOENT) => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(file) => file,
        };
        let stat = fstat(&file)?;
        // TODO: explore mmap-ing the file and lending the map to the caller via a closure callback.
        // This would require some additional refactor work, but would likely help us to reduce the
        // system call overhead in this area.
        let mut buffer = Vec::with_capacity(stat.st_size.try_into().unwrap());
        let mut file = std::fs::File::from(file);
        file.read_to_end(&mut buffer)?;
    }

    /// Whether the file for `key` exists.
    pub fn has(&self, key: &K) -> std::io::Result<bool> {
        let filename = key.to_string();
        match accessat(&self.state.dir, &filename, Access::EXISTS, AtFlags::empty()) {
            Ok(()) => Ok(true),
            Err(Errno::NOENT) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Marks `key` as recently used.
    pub fn touch(&self, key: &K) {
        // Promote in the in-memory LRU and check if on-disk atime refresh is needed.
        let needs_refresh = {
            let mut index = self.state.disk_index.lock();
            match index.get_mut(key) {
                Some((_weight, last_refresh)) => {
                    let now = Instant::now();
                    if now >= *last_refresh + self.state.access_time_refresh_throttle {
                        *last_refresh = now;
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        };
        if needs_refresh {
            let cache = self.clone();
            let key = *key;
            (self.state.bg_spawner)(Box::new(move || cache.refresh_disk_atime(&key)));
        }
    }

    /// Clears the on-disk LRU index and the files in the directory.
    pub fn clear(&self) {
        // Drop the index along with the files it mirrors.
        self.state.disk_index.lock().clear();
        for entry in Dir::read_from(&self.state.dir).unwrap() {
            if let Ok(entry) = entry {
                let filename_bytes = entry.file_name().to_bytes();
                if filename_bytes == b"." || filename_bytes == b".." {
                    continue;
                } else if !entry.file_type().is_file() {
                    debug_assert!(
                        false,
                        "contract code cache should only contain file items, but found {:?}",
                        entry.file_name()
                    );
                } else {
                    if let Err(err) = unlinkat(&self.state.dir, entry.file_name(), AtFlags::empty())
                    {
                        tracing::error!(
                            file_name = ?entry.file_name(),
                            err = &err as &dyn std::error::Error,
                            "failed to remove contract cache file",
                        );
                    }
                }
            }
        }
    }
}

/// Scan `dir` and build an [`LruWeightedCache`] tracking each on-disk
/// cache file by its byte size, ordered from least- to most-recently-
/// accessed by `st_atime`.
///
/// Files whose names don't parse as a [`CryptoHash`] are skipped.
/// If the scanned valid total already exceeds `max_bytes`, the oldest
/// tracked entries are unlinked until the index fits.
fn build_disk_index(
    dir: &rustix::fd::OwnedFd,
    max_bytes: u64,
) -> std::io::Result<LruWeightedCache<CryptoHash, Instant>> {
    let mut index = LruWeightedCache::<CryptoHash, Instant>::without_item_cap(max_bytes);
    let mut entries: Vec<(CryptoHash, u64, i64, i64)> = Vec::new();

    let read_dir = Dir::read_from(dir)?;
    for entry in read_dir {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let filename = entry.file_name();
        if !entry.file_type().is_file() {
            continue;
        }
        let Some(key) = filename.to_str().ok().and_then(|s| CryptoHash::from_str(s).ok()) else {
            continue;
        };
        let stat = match statat(dir, filename, AtFlags::empty()) {
            Ok(s) => s,
            // Missing/unreadable: leave it alone for now; `get` will treat
            // it as a miss and the next compile will overwrite it.
            Err(_) => continue,
        };
        let size = u64::try_from(stat.st_size).unwrap_or(0);
        entries.push((key, size, stat.st_atime as i64, stat.st_atime_nsec as i64));
    }

    // Ascending by atime: most-recently-accessed ends up MRU.
    entries.sort_unstable_by_key(|(_, _, sec, nsec)| (*sec, *nsec));

    // Sizes by key, so we can total the bytes of any entries we trim below
    // (`insert` only hands back the evicted key, not its weight).
    let sizes: HashMap<CryptoHash, u64> =
        entries.iter().map(|&(key, size, _, _)| (key, size)).collect();

    let refreshed_at = Instant::now();
    let mut over_limit_trimmed: usize = 0;
    let mut trimmed_bytes: u64 = 0;
    for (key, size, _, _) in entries {
        for (victim, _) in index.insert(key, size, refreshed_at) {
            trimmed_bytes += sizes.get(&victim).copied().unwrap_or(0);
            match unlinkat(dir, victim.to_string(), AtFlags::empty()) {
                Ok(()) | Err(Errno::NOENT) => {}
                Err(err) => tracing::debug!(
                    target: "vm",
                    victim = %victim,
                    err = &err as &dyn std::error::Error,
                    "failed to unlink evicted compiled-contract cache file; on-disk cache may exceed its size limit"
                ),
            }
            over_limit_trimmed += 1;
        }
    }

    if 0 < over_limit_trimmed {
        tracing::info!(
            target: "vm",
            trimmed = over_limit_trimmed,
            trimmed_bytes,
            max_bytes,
            "compiled-contract cache exceeded its on-disk size limit at startup; trimmed oldest entries"
        );
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_disk_atime_advances_atime_and_preserves_mtime() {
        use std::time::{Duration, UNIX_EPOCH};
        // An explicit `utimensat` must bump atime regardless of the mount's
        // atime policy, while leaving mtime (which records compile time for the
        // protocol-version sweep) untouched.
        let cache =
            FilesystemContractRuntimeCache::test_with_disk_cache_bytes(1 << 20).unwrap();
        let path = cache_dir_path(&cache).join(CryptoHash::hash_bytes(b"refresh").to_string());
        let k = CryptoHash::hash_bytes(b"refresh");
        cache.put(&k, make_test_entry(0x11)).unwrap();

        // Backdate both timestamps to a fixed, clearly-old instant.
        let old = UNIX_EPOCH + Duration::from_secs(1_000);
        let times = std::fs::FileTimes::new().set_accessed(old).set_modified(old);
        std::fs::File::open(&path).unwrap().set_times(times).unwrap();

        cache.refresh_disk_atime(&k);

        let meta = std::fs::metadata(&path).unwrap();
        assert!(
            meta.accessed().unwrap() > old + Duration::from_secs(60),
            "atime should have been refreshed to ~now"
        );
        assert_eq!(
            meta.modified().unwrap(),
            old,
            "mtime must be preserved by the atime refresh"
        );
    }

    #[test]
    fn touch_throttles_and_enqueues_atime_refresh() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // A spawner that runs the job inline and counts how many it received.
        let count = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        let mut cache =
            FilesystemContractRuntimeCache::test_with_disk_cache_bytes(1 << 20).unwrap();
        cache.test_set_background_job_spawner(Arc::new(move |task| {
            observed.fetch_add(1, Ordering::SeqCst);
            task();
        }));

        let k = CryptoHash::hash_bytes(b"hot");
        cache.put(&k, make_test_entry(0x22)).unwrap();

        // Right after `put` the entry is inside its (default) throttle window.
        cache.touch(&k);
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "touch within the throttle must not refresh"
        );

        // With a zero window the entry is eligible: one touch enqueues exactly
        // one refresh and records "refreshed now".
        cache.test_set_access_time_refresh_throttle(Duration::ZERO);
        cache.touch(&k);
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "an eligible touch must enqueue one refresh"
        );

        // Restore the window; the just-recorded refresh time now suppresses.
        cache.test_set_access_time_refresh_throttle(ACCESS_TIME_REFRESH_THROTTLE);
        cache.touch(&k);
        assert_eq!(count.load(Ordering::SeqCst), 1, "the next touch must be throttled again");

        // An untracked key never enqueues anything.
        cache.touch(&CryptoHash::hash_bytes(b"never-put"));
        assert_eq!(count.load(Ordering::SeqCst), 1, "untracked key must not refresh");
    }
}
