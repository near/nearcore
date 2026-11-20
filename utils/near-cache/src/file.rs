// cspell:ignore EXCL, NOENT, RDONLY, RGRP, RUSR, WGRP, WRONLY, WUSR
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
use std::ffi::CStr;
use std::fmt::Display;
use std::hash::Hash;
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// A directory of files, one per key, with a size-bounded, best-effort eviction policy.
///
/// Files are tracked by key and on-disk byte size; LRU victims are unlinked on `commit_file`.
///
/// The key is the name of the file: it has to round-trip through [`Display`] and [`FromStr`].
/// The name must not be empty and must not contain `/` or `.`: the dot is
/// reserved for the names of the temporary files that are still being
/// written. A key that renders differently is rejected with an
/// [`InvalidInput`] error.
///
/// Files in the directory that are not named after a key are left alone,
/// except for temporary files that a crashed writer left behind: those are
/// removed when the cache is opened, once they are old enough.
///
/// [`read`] and [`has`] go straight to the file system. They do not consult
/// the index and do not mark the key as recently used, so the caller has to
/// call [`touch`] for every access that should count for the eviction order.
///
/// [`read`]: LruFileCache::read
/// [`has`]: LruFileCache::has
/// [`touch`]: LruFileCache::touch
/// [`InvalidInput`]: std::io::ErrorKind::InvalidInput
pub struct LruFileCache<K> {
    /// file descriptor to the cache directory
    dir: Arc<OwnedFd>,
    /// Name of the cache. Only used to tell the caches apart in the logs.
    name: &'static str,
    /// Tracks files present in `dir`, keyed by the same key as the
    /// on-disk filename, weighted by on-disk byte size. The value is the
    /// instant the entry's on-disk atime was last refreshed/created.
    disk_index: Mutex<LruWeightedCache<K, Instant>>,
    /// Minimum age of an entry's last atime on-disk refresh before `touch` enqueues another.
    /// Mirrors `relatime` behavior.
    access_time_refresh_throttle: Duration,
    /// Off-loads the on-disk atime refresh; see [`BackgroundJobSpawner`].
    bg_spawner: BackgroundJobSpawner,
}

/// A file of a [`LruFileCache`] that is being written.
///
/// Created with [`LruFileCache::create_file`]. The file is stored in the cache
/// only once the writer is passed to [`LruFileCache::commit_file`]. A writer
/// that is dropped instead removes its temporary file.
#[must_use = "the file is not stored until the writer is passed to `LruFileCache::commit_file`"]
pub struct LruFileWriter<K> {
    key: K,
    /// file descriptor to the cache directory
    dir: Arc<OwnedFd>,
    /// Name of the cache, for the logs.
    name: &'static str,
    temp_file_name: String,
    temp_file: std::fs::File,
    /// Whether the temporary file was renamed to its final name. Until then
    /// the writer owns the temporary file and removes it when dropped.
    committed: bool,
}

impl<K> Drop for LruFileWriter<K> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        match unlinkat(&self.dir, &self.temp_file_name, AtFlags::empty()) {
            Ok(()) | Err(Errno::NOENT) => {}
            Err(err) => tracing::debug!(
                target: "cache",
                cache = self.name,
                file_name = %self.temp_file_name,
                err = &err as &dyn std::error::Error,
                "failed to remove the temporary file of an abandoned cache file writer",
            ),
        }
    }
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
/// [`touch`]: LruFileCache::touch
const ACCESS_TIME_REFRESH_THROTTLE: Duration = Duration::from_mins(10);

/// Suffix of the temporary files that [`LruFileCache::create_file`] writes to.
///
/// The full name is `<key>.<random>.temp`, where the random part is
/// [`TEMP_FILE_RANDOM_LEN`] alphanumeric characters.
const TEMP_FILE_SUFFIX: &str = ".temp";

/// Length of the random part of a temporary file name.
const TEMP_FILE_RANDOM_LEN: usize = 8;

/// Minimum age of a temporary file, by its modification time, before the
/// startup scan removes it.
///
/// A writer removes its own temporary file, so a leftover one comes from a
/// process that crashed or was killed while writing. The age gate protects
/// the files of a live writer in another process that has the same directory
/// open.
const STALE_TEMP_FILE_AGE: Duration = Duration::from_hours(1);

/// The name of the file that stores `key`.
///
/// Returns an `InvalidInput` error if the key does not render as a valid file
/// name, see [`is_valid_file_name`].
fn file_name<K: Display>(key: &K) -> std::io::Result<String> {
    let name = key.to_string();
    if !is_valid_file_name(&name) {
        let message = format!("the key {name:?} is not a valid cache file name");
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, message));
    }
    Ok(name)
}

/// Whether `name` can be the name of the file of a key: a single path
/// component without a dot. That rules out `.` and `..`, and a temporary file
/// name, which always has dots.
fn is_valid_file_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }
    !name.contains(['/', '\0', '.'])
}

/// Whether `name` has the shape of a temporary file name:
/// `<key>.<random>.temp`.
fn is_temp_file_name(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(TEMP_FILE_SUFFIX) else {
        return false;
    };
    let Some((key_name, random)) = stem.rsplit_once('.') else {
        return false;
    };
    if random.len() != TEMP_FILE_RANDOM_LEN {
        return false;
    }
    if !random.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return false;
    }
    is_valid_file_name(key_name)
}

impl<K: Copy + Eq + Hash + Display + FromStr + Send + 'static> LruFileCache<K> {
    /// Largest accepted value for the size limit
    pub const MAX_BYTES: u64 = LruWeightedCache::<K, ()>::MAX_WEIGHT;

    /// Opens the directory at `path`, creating it if needed, and indexes the files in it.
    ///
    /// `name` identifies the cache in the logs, e.g. `"compiled_contracts"`.
    ///
    /// `max_bytes` is the maximum total size of the files kept in the directory. Returns an
    /// [`InvalidInput`] error if it is larger than [`Self::MAX_BYTES`].
    ///
    /// [`InvalidInput`]: std::io::ErrorKind::InvalidInput
    pub fn open(
        name: &'static str,
        path: &Path,
        max_bytes: u64,
        bg_spawner: BackgroundJobSpawner,
    ) -> std::io::Result<Self> {
        if max_bytes > Self::MAX_BYTES {
            let message = format!("max_bytes must be at most {} bytes", Self::MAX_BYTES);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, message));
        }
        std::fs::create_dir_all(path)?;
        let dir = open(path, OFlags::DIRECTORY, Mode::empty())?;
        let disk_index = Mutex::new(build_disk_index(&dir, name, max_bytes)?);
        Ok(Self {
            dir: Arc::new(dir),
            name,
            disk_index,
            access_time_refresh_throttle: ACCESS_TIME_REFRESH_THROTTLE,
            bg_spawner,
        })
    }

    /// TEST ONLY: override the atime-refresh throttle window. Setting it to
    /// [`Duration::ZERO`] makes every tracked key eligible on the next
    /// [`Self::touch`] — deterministic, with no `Instant` arithmetic that could
    /// underflow.
    #[cfg(test)]
    fn test_set_access_time_refresh_throttle(&mut self, throttle: Duration) {
        self.access_time_refresh_throttle = throttle;
    }

    /// Stamp `key`'s on-disk file with the current access time, leaving its
    /// modification time untouched. Best-effort: failures are logged and dropped.
    fn refresh_disk_atime(dir: &OwnedFd, name: &str, key: &K) {
        let filename = key.to_string();
        let times = Timestamps {
            last_access: Timespec { tv_sec: 0, tv_nsec: UTIME_NOW },
            last_modification: Timespec { tv_sec: 0, tv_nsec: UTIME_OMIT },
        };
        match utimensat(dir, &filename, &times, AtFlags::empty()) {
            // Success, or the file was evicted/removed out from under us.
            Ok(()) | Err(Errno::NOENT) => {}
            Err(err) => tracing::debug!(
                target: "cache",
                cache = name,
                key = %key,
                err = &err as &dyn std::error::Error,
                "failed to refresh cache file atime",
            ),
        }
    }

    /// Creates a temporary file for `key` that can be written to.
    ///
    /// Returns a writer where the caller can write the contents of the file.
    ///
    /// Nothing is stored under `key` until the returned writer is committed via
    /// [`Self::commit_file`]. A writer that is dropped without being committed
    /// removes the temporary file.
    pub fn create_file(&self, key: &K) -> std::io::Result<LruFileWriter<K>> {
        const MAX_ATTEMPTS: u32 = 5;
        let final_filename = file_name(key)?;
        let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
        // `EXCL`: never write into the temporary file of another writer.
        let flags = OFlags::CREATE | OFlags::EXCL | OFlags::WRONLY;
        let mut attempt = 0;
        let (temp_filename, file) = loop {
            attempt += 1;
            let mut temporary_filename = final_filename.clone();
            temporary_filename.push('.');
            let random = rand::thread_rng().sample_iter(rand::distributions::Alphanumeric);
            for b in random.take(TEMP_FILE_RANDOM_LEN) {
                temporary_filename.push(b as char);
            }
            temporary_filename.push_str(TEMP_FILE_SUFFIX);
            match openat(&self.dir, &temporary_filename, flags, mode) {
                Ok(f) => break (temporary_filename, std::fs::File::from(f)),
                // The name is taken by another writer: try a new one.
                Err(Errno::EXIST) if attempt < MAX_ATTEMPTS => continue,
                Err(e) => return Err(e.into()),
            }
        };
        Ok(LruFileWriter {
            key: *key,
            dir: Arc::clone(&self.dir),
            name: self.name,
            temp_file_name: temp_filename,
            temp_file: file,
            committed: false,
        })
    }

    /// Takes the temporary file returned by [`Self::create_file`] and commits
    /// it to the cache.
    ///
    /// The file is committed by renaming it into its final location and
    /// updating the on-disk LRU index.
    ///
    /// `weight` is the total number of bytes written to `writer`. Used to
    /// weight the on-disk LRU index.
    ///
    /// If the commit fails, the temporary file is removed.
    pub fn commit_file(&self, mut writer: LruFileWriter<K>, weight: u64) -> std::io::Result<()> {
        let key = &writer.key;
        let final_filename = key.to_string();
        writer.temp_file.sync_data()?;
        // Rename, index update, and victim unlinks share one lock, so a
        // concurrent `commit_file` of a victim key can't rename a fresh file into place
        // before our unlink and have us delete it. The write to the file is outside
        // the lock.
        {
            let mut index = self.disk_index.lock();
            renameat(&self.dir, &writer.temp_file_name, &self.dir, final_filename)?;
            // The temporary name is gone: nothing is left for the writer to
            // remove when it is dropped.
            writer.committed = true;
            // We just wrote the file, so its atime is current as of now.
            let evicted = index.insert(*key, weight, Instant::now());
            // `insert` returns `key` for a same-key replacement (keep our file)
            // and for an oversized reject (remove it); `contains` tells them apart.
            let key_stored = index.contains(key);
            for (victim, _) in evicted {
                if &victim == key && key_stored {
                    continue;
                }
                match unlinkat(&self.dir, victim.to_string(), AtFlags::empty()) {
                    Ok(()) | Err(Errno::NOENT) => {}
                    Err(err) => tracing::debug!(
                        target: "cache",
                        cache = self.name,
                        victim = %victim,
                        err = &err as &dyn std::error::Error,
                        "failed to unlink evicted cache file; file cache may exceed its size limit"
                    ),
                }
            }
        }
        Ok(())
    }

    /// Reads the whole file for `key`. Returns `None` if there is no such file.
    ///
    /// Does not mark `key` as recently used: call [`Self::touch`] for that.
    /// The file is looked up in the directory, not in the index, so a file
    /// that the index does not track is returned as well.
    pub fn read(&self, key: &K) -> std::io::Result<Option<Vec<u8>>> {
        let filename = file_name(key)?;
        let mode = Mode::empty();
        let flags = OFlags::RDONLY;
        let file = openat(&self.dir, &filename, flags, mode);
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
        Ok(Some(buffer))
    }

    /// Whether the file for `key` exists.
    ///
    /// Like [`Self::read`], this checks the directory, not the index, and
    /// does not mark `key` as recently used.
    pub fn has(&self, key: &K) -> std::io::Result<bool> {
        let filename = file_name(key)?;
        match accessat(&self.dir, &filename, Access::EXISTS, AtFlags::empty()) {
            Ok(()) => Ok(true),
            Err(Errno::NOENT) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Marks `key` as recently used.
    ///
    /// This is the only way to protect a file from eviction: reading it is
    /// not enough. Does nothing if the index does not track `key`.
    pub fn touch(&self, key: &K) {
        // Promote in the in-memory LRU and check if on-disk atime refresh is needed.
        let needs_refresh = {
            let mut index = self.disk_index.lock();
            match index.get_mut(key) {
                Some(last_refresh) => {
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
            let name = self.name;
            let key = *key;
            (self.bg_spawner)(Box::new(move || Self::refresh_disk_atime(&dir, name, &key)));
        }
    }

    /// TESTING ONLY: Clears the on-disk LRU index and the files in the
    /// directory.
    ///
    /// The index lock is not held while the files are removed, so this must
    /// not run concurrently with other accesses to the cache.
    #[cfg(feature = "test_features")]
    pub fn clear(&self) {
        // Drop the index along with the files it mirrors.
        self.disk_index.lock().clear();
        for entry in Dir::read_from(&self.dir).unwrap() {
            if let Ok(entry) = entry {
                let filename_bytes = entry.file_name().to_bytes();
                if filename_bytes == b"." || filename_bytes == b".." {
                    continue;
                } else if !entry.file_type().is_file() {
                    debug_assert!(
                        false,
                        "the cache directory should only contain files, but found {:?}",
                        entry.file_name()
                    );
                } else {
                    if let Err(err) = unlinkat(&self.dir, entry.file_name(), AtFlags::empty()) {
                        tracing::error!(
                            target: "cache",
                            cache = self.name,
                            file_name = ?entry.file_name(),
                            err = &err as &dyn std::error::Error,
                            "failed to remove cache file",
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
/// Files whose names are not valid key file names, don't parse as a key, or
/// are not the name that the key renders as, are skipped. Files with the name
/// of a temporary file that are older than [`STALE_TEMP_FILE_AGE`] are
/// unlinked.
/// If the scanned valid total already exceeds `max_bytes`, the oldest
/// tracked entries are unlinked until the index fits.
fn build_disk_index<K: Copy + Eq + Hash + Display + FromStr>(
    dir: &OwnedFd,
    name: &str,
    max_bytes: u64,
) -> std::io::Result<LruWeightedCache<K, Instant>> {
    let mut index = LruWeightedCache::<K, Instant>::without_item_cap(max_bytes);
    let mut entries: Vec<(K, u64, i64, i64)> = Vec::new();
    let now = SystemTime::now();

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
        let Ok(entry_name) = filename.to_str() else {
            continue;
        };
        if is_temp_file_name(entry_name) {
            remove_stale_temp_file(dir, name, filename, now);
            continue;
        }
        if !is_valid_file_name(entry_name) {
            continue;
        }
        let Ok(key) = K::from_str(entry_name) else {
            continue;
        };
        // Every operation derives the file name from the key, so a file under
        // any other name (e.g. `05` for the integer key 5) can't be reached.
        // Tracking it would also make the index unlink the wrong file.
        if key.to_string() != entry_name {
            continue;
        }
        let stat = match statat(dir, filename, AtFlags::empty()) {
            Ok(s) => s,
            // Missing/unreadable: leave it alone for now; `read` will treat
            // it as a miss and the next `commit_file` will overwrite it.
            Err(_) => continue,
        };
        let size = u64::try_from(stat.st_size).unwrap_or(0);
        entries.push((key, size, stat.st_atime as i64, stat.st_atime_nsec as i64));
    }

    // Ascending by atime: most-recently-accessed ends up MRU.
    entries.sort_unstable_by_key(|(_, _, sec, nsec)| (*sec, *nsec));

    // Sizes by key, so we can total the bytes of any entries we trim below
    // (`insert` only hands back the evicted key, not its weight).
    let sizes: HashMap<K, u64> = entries.iter().map(|&(key, size, _, _)| (key, size)).collect();

    let refreshed_at = Instant::now();
    let mut over_limit_trimmed: usize = 0;
    let mut trimmed_bytes: u64 = 0;
    for (key, size, _, _) in entries {
        for (victim, _) in index.insert(key, size, refreshed_at) {
            trimmed_bytes += sizes.get(&victim).copied().unwrap_or(0);
            match unlinkat(dir, victim.to_string(), AtFlags::empty()) {
                Ok(()) | Err(Errno::NOENT) => {}
                Err(err) => tracing::debug!(
                    target: "cache",
                    cache = name,
                    victim = %victim,
                    err = &err as &dyn std::error::Error,
                    "failed to unlink evicted cache file; file cache may exceed its size limit"
                ),
            }
            over_limit_trimmed += 1;
        }
    }

    if 0 < over_limit_trimmed {
        tracing::info!(
            target: "cache",
            cache = name,
            trimmed = over_limit_trimmed,
            trimmed_bytes,
            max_bytes,
            "file cache exceeded its size limit at startup; trimmed oldest entries"
        );
    }

    Ok(index)
}

/// Unlinks the temporary file `filename` if it was last modified at least
/// [`STALE_TEMP_FILE_AGE`] before `now`.
///
/// Best-effort: a file that can't be inspected or removed is left alone.
fn remove_stale_temp_file(dir: &OwnedFd, name: &str, filename: &CStr, now: SystemTime) {
    let Ok(stat) = statat(dir, filename, AtFlags::empty()) else {
        return;
    };
    let Ok(modified_secs) = u64::try_from(stat.st_mtime as i64) else {
        return;
    };
    let modified = UNIX_EPOCH + Duration::from_secs(modified_secs);
    // A modification time in the future is not stale.
    let Ok(age) = now.duration_since(modified) else {
        return;
    };
    if age < STALE_TEMP_FILE_AGE {
        return;
    }
    match unlinkat(dir, filename, AtFlags::empty()) {
        Ok(()) => tracing::debug!(
            target: "cache",
            cache = name,
            file_name = ?filename,
            size = stat.st_size,
            ?age,
            "removed a stale temporary cache file",
        ),
        Err(Errno::NOENT) => {}
        Err(err) => tracing::debug!(
            target: "cache",
            cache = name,
            file_name = ?filename,
            err = &err as &dyn std::error::Error,
            "failed to remove a stale temporary cache file",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PAYLOAD_LEN: usize = 100;

    fn test_cache(
        temp_dir: &tempfile::TempDir,
        bg_spawner: BackgroundJobSpawner,
    ) -> LruFileCache<u64> {
        LruFileCache::open("test", temp_dir.path(), 1 << 20, bg_spawner).unwrap()
    }

    fn put(cache: &LruFileCache<u64>, key: u64, filler: u8) {
        let payload = [filler; TEST_PAYLOAD_LEN];
        let mut file = cache.create_file(&key).unwrap();
        file.write_all(&payload).unwrap();
        cache.commit_file(file, TEST_PAYLOAD_LEN as u64).unwrap();
    }

    /// Names of the entries in the cache directory, sorted.
    fn file_names(temp_dir: &tempfile::TempDir) -> Vec<String> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(temp_dir.path()).unwrap() {
            let name = entry.unwrap().file_name();
            names.push(name.into_string().unwrap());
        }
        names.sort();
        names
    }

    /// Sets the modification time of `path` to `age` ago.
    fn backdate(path: &Path, age: Duration) {
        let modified = SystemTime::now() - age;
        let times = std::fs::FileTimes::new().set_modified(modified);
        std::fs::File::open(path).unwrap().set_times(times).unwrap();
    }

    /// A key whose file name is whatever the test wants it to be.
    #[derive(Clone, Copy, PartialEq, Eq, Hash)]
    struct NameKey(&'static str);

    impl Display for NameKey {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl FromStr for NameKey {
        type Err = ();

        fn from_str(_: &str) -> Result<Self, Self::Err> {
            Err(())
        }
    }

    #[test]
    fn scan_skips_directories_and_unparsable_names() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        // A directory is not a cache file, even if its name parses as a key.
        std::fs::create_dir(temp_dir.path().join("7")).unwrap();
        std::fs::write(temp_dir.path().join("not-a-key"), b"foreign").unwrap();
        std::fs::write(temp_dir.path().join("3"), b"tracked").unwrap();

        let cache = test_cache(&temp_dir, crate::noop_background_spawner());

        let index = cache.disk_index.lock();
        assert_eq!(index.len(), 1);
        assert!(index.contains(&3));
        assert_eq!(index.current_weight(), 7);
        // The skipped entries are left alone.
        assert_eq!(file_names(&temp_dir), ["3", "7", "not-a-key"]);
    }

    #[test]
    fn scan_skips_names_that_do_not_round_trip() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        // Both names parse as the key 5, but only `5` is the file that the
        // cache reads and unlinks for that key.
        std::fs::write(temp_dir.path().join("05"), b"foreign").unwrap();
        std::fs::write(temp_dir.path().join("5"), b"ok").unwrap();

        let cache = test_cache(&temp_dir, crate::noop_background_spawner());

        let index = cache.disk_index.lock();
        assert_eq!(index.len(), 1);
        assert!(index.contains(&5));
        assert_eq!(index.current_weight(), 2, "only the file `5` is tracked");
        assert_eq!(file_names(&temp_dir), ["05", "5"]);
    }

    #[test]
    fn scan_removes_only_stale_temp_files() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let stale = temp_dir.path().join("1.aaaaaaaa.temp");
        let fresh = temp_dir.path().join("2.bbbbbbbb.temp");
        std::fs::write(&stale, b"stale").unwrap();
        std::fs::write(&fresh, b"fresh").unwrap();
        let stale_age = STALE_TEMP_FILE_AGE + Duration::from_mins(1);
        backdate(&stale, stale_age);
        // Old files that only resemble a temporary file: a dot in the key
        // part, a random part of the wrong length, no random part.
        let foreign_names = ["1.a.b.aaaaaaaa.temp", "1.short.temp", "foreign.temp"];
        for name in foreign_names {
            let foreign = temp_dir.path().join(name);
            std::fs::write(&foreign, b"foreign").unwrap();
            backdate(&foreign, stale_age);
        }

        let cache = test_cache(&temp_dir, crate::noop_background_spawner());

        // The fresh one may belong to a live writer and has to stay.
        let mut expected = vec!["2.bbbbbbbb.temp"];
        expected.extend(foreign_names);
        expected.sort();
        assert_eq!(file_names(&temp_dir), expected);
        assert_eq!(cache.disk_index.lock().len(), 0, "temp files are never indexed");
    }

    #[test]
    fn dropped_writer_removes_its_temp_file() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = test_cache(&temp_dir, crate::noop_background_spawner());

        let mut writer = cache.create_file(&1).unwrap();
        writer.write_all(b"abandoned").unwrap();
        assert_eq!(file_names(&temp_dir).len(), 1, "the temp file exists while writing");
        drop(writer);
        assert!(file_names(&temp_dir).is_empty());
        assert_eq!(cache.read(&1).unwrap(), None);

        // A committed writer leaves the final file and nothing else.
        put(&cache, 1, 0x33);
        assert_eq!(file_names(&temp_dir), ["1"]);
    }

    #[test]
    fn keys_that_are_not_a_valid_file_name_are_rejected() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache_path = temp_dir.path().join("cache");
        let bg_spawner = crate::noop_background_spawner();
        let cache =
            LruFileCache::<NameKey>::open("test", &cache_path, 1 << 20, bg_spawner).unwrap();
        // A file outside of the cache directory that `../outside` points at.
        std::fs::write(temp_dir.path().join("outside"), b"secret").unwrap();

        let names = ["", ".", "..", "../outside", "a/b", "nul\0", "key.temp", ".hidden", "a.b"];
        for name in names {
            let key = NameKey(name);
            let kind = std::io::ErrorKind::InvalidInput;
            assert_eq!(cache.create_file(&key).err().unwrap().kind(), kind, "create {name:?}");
            assert_eq!(cache.read(&key).unwrap_err().kind(), kind, "read {name:?}");
            assert_eq!(cache.has(&key).unwrap_err().kind(), kind, "has {name:?}");
        }
        assert!(std::fs::read_dir(&cache_path).unwrap().next().is_none());

        // An ordinary name works.
        let key = NameKey("key");
        let mut writer = cache.create_file(&key).unwrap();
        writer.write_all(b"value").unwrap();
        cache.commit_file(writer, 5).unwrap();
        assert_eq!(cache.read(&key).unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn open_rejects_max_bytes_above_the_cap() {
        // A limit the index can't represent must surface as an `Err`, not a
        // panic, and must not create the directory.
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().join("cache");
        let too_big = LruFileCache::<u64>::MAX_BYTES + 1;
        let bg_spawner = crate::noop_background_spawner();
        match LruFileCache::<u64>::open("test", &path, too_big, bg_spawner) {
            Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput),
            Ok(_) => panic!("limit above the cap must be rejected"),
        }
        assert!(!path.exists(), "a rejected open must not create the directory");
    }

    #[test]
    fn refresh_disk_atime_advances_atime_and_preserves_mtime() {
        use std::time::{Duration, UNIX_EPOCH};
        // An explicit `utimensat` must bump atime regardless of the mount's
        // atime policy, while leaving mtime (which callers may rely on as the
        // time the file was written) untouched.
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = test_cache(&temp_dir, crate::noop_background_spawner());
        let k = 1u64;
        let path = temp_dir.path().join(k.to_string());
        put(&cache, k, 0x11);

        // Backdate both timestamps to a fixed, clearly-old instant.
        let old = UNIX_EPOCH + Duration::from_secs(1_000);
        let times = std::fs::FileTimes::new().set_accessed(old).set_modified(old);
        std::fs::File::open(&path).unwrap().set_times(times).unwrap();

        LruFileCache::refresh_disk_atime(&cache.dir, cache.name, &k);

        let meta = std::fs::metadata(&path).unwrap();
        assert!(
            meta.accessed().unwrap() > old + Duration::from_secs(60),
            "atime should have been refreshed to ~now"
        );
        assert_eq!(meta.modified().unwrap(), old, "mtime must be preserved by the atime refresh");
    }

    #[test]
    fn touch_throttles_and_enqueues_atime_refresh() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // A spawner that runs the job inline and counts how many it received.
        let count = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&count);
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut cache = test_cache(
            &temp_dir,
            Arc::new(move |task| {
                observed.fetch_add(1, Ordering::SeqCst);
                task();
            }),
        );

        let k = 1u64;
        put(&cache, k, 0x22);

        // Right after `commit_file` the entry is inside its (default) throttle window.
        cache.touch(&k);
        assert_eq!(count.load(Ordering::SeqCst), 0, "touch within the throttle must not refresh");

        // With a zero window the entry is eligible: one touch enqueues exactly
        // one refresh and records "refreshed now".
        cache.test_set_access_time_refresh_throttle(Duration::ZERO);
        cache.touch(&k);
        assert_eq!(count.load(Ordering::SeqCst), 1, "an eligible touch must enqueue one refresh");

        // Restore the window; the just-recorded refresh time now suppresses.
        cache.test_set_access_time_refresh_throttle(ACCESS_TIME_REFRESH_THROTTLE);
        cache.touch(&k);
        assert_eq!(count.load(Ordering::SeqCst), 1, "the next touch must be throttled again");

        // An untracked key never enqueues anything.
        cache.touch(&2u64);
        assert_eq!(count.load(Ordering::SeqCst), 1, "untracked key must not refresh");
    }
}
