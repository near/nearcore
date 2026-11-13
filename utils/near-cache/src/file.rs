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
use std::io::Read as _;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Default minimum age of an entry's last access-time refresh before [`LruFileCache::touch`]
/// refreshes it again.
const ACCESS_TIME_REFRESH_THROTTLE: Duration = Duration::from_mins(10);

/// A directory of files, one per key, kept under a byte limit by evicting the least recently
/// used ones.
///
/// The files are tracked in an in-memory index weighted by their size on disk. [`touch`] marks a
/// key as used: it promotes the entry in the index and, throttled, refreshes the access time of
/// the file, so the order survives a restart, when the index is rebuilt from the directory in
/// access-time order. A file is written under a temporary name and renamed into place once
/// complete, so a reader never sees a partial file. The rename, the index update and the
/// unlinking of the files it evicts happen under one lock, so a concurrent write of an evicted
/// key cannot lose its file.
///
/// Keys are the file names: a key has to round-trip through [`Display`] and [`FromStr`].
///
/// [`touch`]: LruFileCache::touch
pub struct LruFileCache<K> {
    /// File descriptor of the directory. Shared with the background access-time refresh jobs.
    dir: Arc<OwnedFd>,
    /// The files in `dir`, weighted by their size on disk. The value is the instant the access
    /// time of the file was last refreshed, or the file was written.
    index: Mutex<LruWeightedCache<K, Instant>>,
    /// Minimum age of an entry's last access-time refresh before `touch` enqueues another.
    /// Mirrors `relatime` behavior.
    access_time_refresh_throttle: Duration,
    /// Off-loads the on-disk access-time refresh; see [`BackgroundJobSpawner`].
    bg_spawner: BackgroundJobSpawner,
}

impl<K: Copy + Eq + Hash + Display + FromStr + Send + 'static> LruFileCache<K> {
    /// Largest accepted size limit.
    pub const MAX_BYTES: u64 = LruWeightedCache::<K, Instant>::MAX_WEIGHT;

    /// Opens the cache at `path`, creating the directory if needed. The files already in it are
    /// indexed in access-time order, and the least recently accessed ones are unlinked while
    /// they exceed `max_bytes`. Files whose name is not a key are left alone.
    pub fn open(
        path: &Path,
        max_bytes: u64,
        bg_spawner: BackgroundJobSpawner,
    ) -> std::io::Result<Self> {
        if max_bytes > Self::MAX_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("the size limit must be at most {} bytes", Self::MAX_BYTES),
            ));
        }
        std::fs::create_dir_all(path)?;
        let dir = open(path, OFlags::DIRECTORY, Mode::empty())?;
        let index = build_index(&dir, max_bytes)?;
        Ok(Self {
            dir: Arc::new(dir),
            index: Mutex::new(index),
            access_time_refresh_throttle: ACCESS_TIME_REFRESH_THROTTLE,
            bg_spawner,
        })
    }

    /// TEST ONLY: overrides the access-time refresh throttle. [`Duration::ZERO`] makes every
    /// tracked key eligible on the next [`Self::touch`].
    #[cfg(test)]
    fn set_access_time_refresh_throttle(&mut self, throttle: Duration) {
        self.access_time_refresh_throttle = throttle;
    }

    /// Number of files tracked.
    pub fn len(&self) -> usize {
        self.index.lock().len()
    }

    /// Total size of the files tracked.
    pub fn total_bytes(&self) -> u64 {
        self.index.lock().current_weight()
    }

    /// Whether the file for `key` exists.
    pub fn exists(&self, key: &K) -> std::io::Result<bool> {
        match accessat(&*self.dir, key.to_string(), Access::EXISTS, AtFlags::empty()) {
            Ok(()) => Ok(true),
            Err(Errno::NOENT) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// The contents of the file for `key`, `None` if there is none. Does not count as a use:
    /// call [`Self::touch`] once the contents turn out to be usable.
    pub fn read(&self, key: &K) -> std::io::Result<Option<Vec<u8>>> {
        let file = match openat(&*self.dir, key.to_string(), OFlags::RDONLY, Mode::empty()) {
            Ok(file) => file,
            Err(Errno::NOENT) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        // TODO: explore mmap-ing the file and lending the map to the caller via a closure
        // callback. This would require some additional refactor work, but would likely help us to
        // reduce the system call overhead in this area.
        let size = fstat(&file)?.st_size;
        let mut buffer = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        std::fs::File::from(file).read_to_end(&mut buffer)?;
        Ok(Some(buffer))
    }

    /// Writes the file for `key` with `write`, replacing the one there, and unlinks the files
    /// evicted to stay under the size limit. A file larger than the limit is not kept. The
    /// data is synced to disk before the file is renamed into place.
    pub fn write(
        &self,
        key: &K,
        write: impl FnOnce(&mut std::fs::File) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        const MAX_ATTEMPTS: u32 = 5;
        let file_name = key.to_string();
        let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
        let flags = OFlags::CREATE | OFlags::TRUNC | OFlags::WRONLY;
        let mut attempt = 0;
        let (temp_name, mut file) = loop {
            attempt += 1;
            let mut temp_name = file_name.clone();
            temp_name.push('.');
            for b in rand::thread_rng().sample_iter(rand::distributions::Alphanumeric).take(8) {
                temp_name.push(b as char);
            }
            temp_name.push_str(".temp");
            match openat(&*self.dir, &temp_name, flags, mode) {
                Ok(fd) => break (temp_name, std::fs::File::from(fd)),
                Err(err) if attempt > MAX_ATTEMPTS => return Err(err.into()),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(err.into()),
            }
        };
        let written = write(&mut file)
            .and_then(|()| file.sync_data())
            .and_then(|()| file.metadata().map(|metadata| metadata.len()));
        drop(file);
        let size = match written {
            Ok(size) => size,
            Err(err) => {
                let _ = unlinkat(&*self.dir, &temp_name, AtFlags::empty());
                return Err(err);
            }
        };
        // The rename, the index update and the unlinking of victims share one lock, so a
        // concurrent `write` of a victim cannot rename a fresh file into place before the unlink
        // and have it deleted here. The write above is outside the lock.
        let mut index = self.index.lock();
        if let Err(err) = renameat(&*self.dir, &temp_name, &*self.dir, &file_name) {
            let _ = unlinkat(&*self.dir, &temp_name, AtFlags::empty());
            return Err(err.into());
        }
        // The file was just written, so its access time is current as of now.
        let evicted = index.insert(*key, size, Instant::now());
        // `insert` returns `key` for a same-key replacement (keep the file) and for an oversized
        // reject (unlink it); `contains` tells them apart.
        let key_stored = index.contains(key);
        for (victim, _) in evicted {
            if &victim == key && key_stored {
                continue;
            }
            unlink(&self.dir, &victim);
        }
        Ok(())
    }

    /// Marks `key` as used: promotes it in the index and, at most once per throttle window,
    /// refreshes the access time of its file in the background. A no-op for untracked keys.
    pub fn touch(&self, key: &K) {
        let needs_refresh = {
            let mut index = self.index.lock();
            match index.get_mut(key) {
                Some((_size, last_refresh)) => {
                    let now = Instant::now();
                    if now >= *last_refresh + self.access_time_refresh_throttle {
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
            let dir = Arc::clone(&self.dir);
            let key = *key;
            (self.bg_spawner)(Box::new(move || refresh_access_time(&dir, &key)));
        }
    }

    /// Unlinks the file for `key` and drops it from the index.
    pub fn remove(&self, key: &K) {
        let mut index = self.index.lock();
        index.remove(key);
        unlink(&self.dir, key);
    }

    /// Unlinks every file in the directory and empties the index.
    pub fn clear(&self) -> std::io::Result<()> {
        let mut index = self.index.lock();
        index.clear();
        for entry in Dir::read_from(&*self.dir)? {
            let Ok(entry) = entry else {
                continue;
            };
            let file_name = entry.file_name();
            if file_name.to_bytes() == b"." || file_name.to_bytes() == b".." {
                continue;
            }
            if !entry.file_type().is_file() {
                debug_assert!(
                    false,
                    "the cache directory should only contain files, but found {:?}",
                    file_name
                );
                continue;
            }
            if let Err(err) = unlinkat(&*self.dir, file_name, AtFlags::empty()) {
                tracing::error!(
                    target: "cache",
                    ?file_name,
                    err = &err as &dyn std::error::Error,
                    "failed to remove a cache file",
                );
            }
        }
        Ok(())
    }
}

/// Indexes the files in `dir` from least to most recently accessed, unlinking the least
/// recently accessed ones while the total exceeds `max_bytes`.
fn build_index<K: Copy + Eq + Hash + Display + FromStr>(
    dir: &OwnedFd,
    max_bytes: u64,
) -> std::io::Result<LruWeightedCache<K, Instant>> {
    let mut index = LruWeightedCache::<K, Instant>::without_item_cap(max_bytes);
    let mut entries: Vec<(K, u64, i64, i64)> = Vec::new();
    for entry in Dir::read_from(dir)? {
        let Ok(entry) = entry else {
            continue;
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let Some(key) = file_name.to_str().ok().and_then(|name| K::from_str(name).ok()) else {
            continue;
        };
        // Missing or unreadable: left alone; `read` treats it as a miss and a later `write`
        // replaces it.
        let Ok(stat) = statat(dir, file_name, AtFlags::empty()) else {
            continue;
        };
        let size = u64::try_from(stat.st_size).unwrap_or(0);
        entries.push((key, size, stat.st_atime as i64, stat.st_atime_nsec as i64));
    }
    // Ascending by access time: the most recently accessed file ends up most recently used.
    entries.sort_unstable_by_key(|(_, _, sec, nsec)| (*sec, *nsec));

    // Sizes by key, to total the bytes of the files trimmed below (`insert` only hands back the
    // evicted key, not its weight).
    let sizes: HashMap<K, u64> = entries.iter().map(|&(key, size, _, _)| (key, size)).collect();
    let refreshed_at = Instant::now();
    let mut trimmed: usize = 0;
    let mut trimmed_bytes: u64 = 0;
    for (key, size, _, _) in entries {
        for (victim, _) in index.insert(key, size, refreshed_at) {
            trimmed_bytes += sizes.get(&victim).copied().unwrap_or(0);
            unlink(dir, &victim);
            trimmed += 1;
        }
    }
    if trimmed > 0 {
        tracing::info!(
            target: "cache",
            trimmed,
            trimmed_bytes,
            max_bytes,
            "the cache directory exceeded its size limit at startup; trimmed the least recently accessed files"
        );
    }
    Ok(index)
}

/// Unlinks the file for `key`. Best-effort: a failure is logged, unless the file is gone already.
fn unlink<K: Display>(dir: &OwnedFd, key: &K) {
    match unlinkat(dir, key.to_string(), AtFlags::empty()) {
        Ok(()) | Err(Errno::NOENT) => {}
        Err(err) => tracing::debug!(
            target: "cache",
            %key,
            err = &err as &dyn std::error::Error,
            "failed to unlink an evicted cache file; the directory may exceed its size limit"
        ),
    }
}

/// Stamps the file for `key` with the current access time, leaving its modification time
/// untouched. Best-effort: a failure is logged, unless the file is gone already.
fn refresh_access_time<K: Display>(dir: &OwnedFd, key: &K) {
    let times = Timestamps {
        last_access: Timespec { tv_sec: 0, tv_nsec: UTIME_NOW },
        last_modification: Timespec { tv_sec: 0, tv_nsec: UTIME_OMIT },
    };
    match utimensat(dir, key.to_string(), &times, AtFlags::empty()) {
        // Success, or the file was evicted or removed out from under us.
        Ok(()) | Err(Errno::NOENT) => {}
        Err(err) => tracing::debug!(
            target: "cache",
            %key,
            err = &err as &dyn std::error::Error,
            "failed to refresh the access time of a cache file",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::noop_background_spawner;
    use std::io::Write as _;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::UNIX_EPOCH;

    const FILE_LEN: usize = 100;

    fn open_in(dir: &Path, max_bytes: u64) -> LruFileCache<u64> {
        LruFileCache::open(&dir.join("cache"), max_bytes, noop_background_spawner()).unwrap()
    }

    fn write(cache: &LruFileCache<u64>, key: u64, filler: u8) {
        cache.write(&key, |file| file.write_all(&[filler; FILE_LEN])).unwrap();
    }

    fn path(dir: &Path, key: u64) -> PathBuf {
        dir.join("cache").join(key.to_string())
    }

    fn set_times(path: &Path, time: std::time::SystemTime) {
        let times = std::fs::FileTimes::new().set_accessed(time).set_modified(time);
        std::fs::File::open(path).unwrap().set_times(times).unwrap();
    }

    #[test]
    fn write_read_exists_remove() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = open_in(temp_dir.path(), 1 << 20);
        assert!(!cache.exists(&1).unwrap());
        assert_eq!(cache.read(&1).unwrap(), None);

        write(&cache, 1, 0xAA);
        assert!(cache.exists(&1).unwrap());
        assert_eq!(cache.read(&1).unwrap(), Some(vec![0xAA; FILE_LEN]));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.total_bytes(), FILE_LEN as u64);

        // A rewrite replaces the contents and keeps a single entry.
        write(&cache, 1, 0xBB);
        assert_eq!(cache.read(&1).unwrap(), Some(vec![0xBB; FILE_LEN]));
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.total_bytes(), FILE_LEN as u64);

        cache.remove(&1);
        assert!(!cache.exists(&1).unwrap());
        assert!(!path(temp_dir.path(), 1).exists());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_bytes(), 0);
        // Removing again is a no-op.
        cache.remove(&1);
    }

    #[test]
    fn eviction_unlinks_the_least_recently_used_file() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        // Room for exactly two files.
        let cache = open_in(temp_dir.path(), 2 * FILE_LEN as u64);
        write(&cache, 1, 0x11);
        write(&cache, 2, 0x22);
        // Touching 1 makes 2 the least recently used.
        cache.touch(&1);
        write(&cache, 3, 0x33);

        assert!(path(temp_dir.path(), 1).exists());
        assert!(!path(temp_dir.path(), 2).exists(), "the least recently used file must go");
        assert!(path(temp_dir.path(), 3).exists());
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.total_bytes(), 2 * FILE_LEN as u64);
    }

    #[test]
    fn without_touch_the_oldest_file_is_evicted() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = open_in(temp_dir.path(), 2 * FILE_LEN as u64);
        write(&cache, 1, 0x11);
        write(&cache, 2, 0x22);
        write(&cache, 3, 0x33);

        assert!(!path(temp_dir.path(), 1).exists(), "the oldest file must go");
        assert!(path(temp_dir.path(), 2).exists());
        assert!(path(temp_dir.path(), 3).exists());
    }

    #[test]
    fn oversized_file_is_not_kept() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = open_in(temp_dir.path(), FILE_LEN as u64 - 1);
        write(&cache, 1, 0x11);

        assert!(!path(temp_dir.path(), 1).exists(), "an oversized file must be unlinked");
        assert!(!cache.exists(&1).unwrap());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_bytes(), 0);
    }

    #[test]
    fn oversized_replacement_drops_the_entry() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = open_in(temp_dir.path(), FILE_LEN as u64);
        write(&cache, 1, 0x11);
        assert!(cache.exists(&1).unwrap());

        cache.write(&1, |file| file.write_all(&[0x22; 2 * FILE_LEN])).unwrap();
        assert!(!path(temp_dir.path(), 1).exists(), "an oversized replacement must be unlinked");
        assert!(!cache.exists(&1).unwrap());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_bytes(), 0);
    }

    #[test]
    fn limit_above_the_cap_is_a_clean_error() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let result = LruFileCache::<u64>::open(
            &temp_dir.path().join("cache"),
            LruFileCache::<u64>::MAX_BYTES + 1,
            noop_background_spawner(),
        );
        assert_eq!(result.err().map(|err| err.kind()), Some(std::io::ErrorKind::InvalidInput));
    }

    #[test]
    fn failed_write_leaves_nothing_behind() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = open_in(temp_dir.path(), 1 << 20);
        let result = cache.write(&1, |_file| Err(std::io::Error::other("boom")));
        assert!(result.is_err());
        assert!(!cache.exists(&1).unwrap());
        assert_eq!(cache.len(), 0);
        let leftovers = std::fs::read_dir(temp_dir.path().join("cache")).unwrap().count();
        assert_eq!(leftovers, 0, "the temporary file must be unlinked");
    }

    #[test]
    fn open_indexes_the_files_by_access_time_and_trims_the_oldest() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let dir = temp_dir.path().join("cache");
        std::fs::create_dir_all(&dir).unwrap();
        // Explicit access times make the test deterministic regardless of the mount's atime
        // mode. The file names decide nothing: the newest access time gets the lowest key.
        for (key, secs) in [(3, 1_000), (2, 2_000), (1, 3_000)] {
            std::fs::write(path(temp_dir.path(), key), [0u8; FILE_LEN]).unwrap();
            set_times(&path(temp_dir.path(), key), UNIX_EPOCH + Duration::from_secs(secs));
        }
        // A file that is not a key is left alone.
        std::fs::write(dir.join("not-a-key"), b"x").unwrap();

        // Room for two of the three files: the least recently accessed one is trimmed.
        let cache = open_in(temp_dir.path(), 2 * FILE_LEN as u64);
        assert!(!path(temp_dir.path(), 3).exists(), "the least recently accessed file must go");
        assert!(path(temp_dir.path(), 2).exists());
        assert!(path(temp_dir.path(), 1).exists());
        assert!(dir.join("not-a-key").exists());
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.total_bytes(), 2 * FILE_LEN as u64);

        // The order carries over: 2 is now the least recently used.
        write(&cache, 4, 0x44);
        assert!(!path(temp_dir.path(), 2).exists());
        assert!(path(temp_dir.path(), 1).exists());
        assert!(path(temp_dir.path(), 4).exists());
    }

    #[test]
    fn refresh_access_time_advances_atime_and_preserves_mtime() {
        // An explicit `utimensat` must bump the access time regardless of the mount's atime
        // policy, while leaving the modification time untouched.
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = open_in(temp_dir.path(), 1 << 20);
        write(&cache, 1, 0x11);
        let file = path(temp_dir.path(), 1);

        // Backdate both timestamps to a fixed, clearly old instant.
        let old = UNIX_EPOCH + Duration::from_secs(1_000);
        set_times(&file, old);

        refresh_access_time(&cache.dir, &1u64);

        let meta = std::fs::metadata(&file).unwrap();
        assert!(
            meta.accessed().unwrap() > old + Duration::from_secs(60),
            "the access time should have been refreshed to about now"
        );
        assert_eq!(meta.modified().unwrap(), old, "the modification time must be preserved");
    }

    #[test]
    fn touch_throttles_and_enqueues_the_access_time_refresh() {
        // A spawner that runs the job inline and counts how many it received.
        let count = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut cache = LruFileCache::<u64>::open(
            &temp_dir.path().join("cache"),
            1 << 20,
            Arc::new(move |job| {
                observed.fetch_add(1, Ordering::SeqCst);
                job();
            }),
        )
        .unwrap();
        write(&cache, 1, 0x11);

        // Right after a write the entry is inside its (default) throttle window.
        cache.touch(&1);
        assert_eq!(count.load(Ordering::SeqCst), 0, "a touch within the throttle must not refresh");

        // With a zero window the entry is eligible: one touch enqueues exactly one refresh and
        // records "refreshed now".
        cache.set_access_time_refresh_throttle(Duration::ZERO);
        cache.touch(&1);
        assert_eq!(count.load(Ordering::SeqCst), 1, "an eligible touch must enqueue one refresh");

        // Restore the window; the just-recorded refresh now suppresses.
        cache.set_access_time_refresh_throttle(ACCESS_TIME_REFRESH_THROTTLE);
        cache.touch(&1);
        assert_eq!(count.load(Ordering::SeqCst), 1, "the next touch must be throttled again");

        // An untracked key never enqueues anything.
        cache.touch(&2);
        assert_eq!(count.load(Ordering::SeqCst), 1, "an untracked key must not refresh");
    }

    #[test]
    fn clear_unlinks_every_file() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = open_in(temp_dir.path(), 1 << 20);
        write(&cache, 1, 0x11);
        write(&cache, 2, 0x22);

        cache.clear().unwrap();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_bytes(), 0);
        let left = std::fs::read_dir(temp_dir.path().join("cache")).unwrap().count();
        assert_eq!(left, 0, "every file must be unlinked");

        // The cache is usable after a clear.
        write(&cache, 1, 0x33);
        assert_eq!(cache.read(&1).unwrap(), Some(vec![0x33; FILE_LEN]));
    }
}
