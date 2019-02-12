use tempfile::TempDir;
use std::sync::Mutex;
use std::path::{Path, PathBuf};

lazy_static! {
    static ref CACHE_TEMP_DIR: Mutex<TempDir> =
        Mutex::new(TempDir::new()
            .expect("create wasm cache temp directory"));
}

pub(crate) fn get_cached_path<P: AsRef<Path>>(filename: P) -> PathBuf {
    CACHE_TEMP_DIR.lock().unwrap().path().join(filename)
}
