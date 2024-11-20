use std::collections::HashMap;
use strum::IntoEnumIterator;

use crate::DBCol;
use std::io;

use super::{cold_column_dirname, ArchivalStorage};
use std::io::Read;
use std::io::Write;

pub(crate) struct FilesystemArchiver {
    base_dir: rustix::fd::OwnedFd,
    col_to_dir: HashMap<DBCol, std::path::PathBuf>,
}

impl FilesystemArchiver {
    pub(crate) fn open(path: &std::path::Path) -> io::Result<Self> {
        let col_to_dir = Self::setup_dirs(path).unwrap();
        let dir = rustix::fs::open(path, rustix::fs::OFlags::DIRECTORY, rustix::fs::Mode::empty())
            .unwrap();
        tracing::debug!(
            target: "archiver",
            path = %path.display(),
            message = "opened archive directory"
        );
        Ok(Self { base_dir: dir, col_to_dir })
    }

    fn setup_dirs(base_path: &std::path::Path) -> io::Result<HashMap<DBCol, std::path::PathBuf>> {
        std::fs::create_dir_all(base_path).unwrap();
        let mut col_to_dir = HashMap::new();
        for col in DBCol::iter() {
            if col.is_cold() {
                let path: std::path::PathBuf =
                    [base_path, cold_column_dirname(col).as_ref()].into_iter().collect();
                std::fs::create_dir(&path).unwrap();
                col_to_dir.insert(col, path);
            }
        }
        Ok(col_to_dir)
    }

    #[inline]
    fn get_path(&self, col: DBCol, key: &[u8]) -> std::path::PathBuf {
        let dirname = self.col_to_dir.get(&col);
        let filename = bs58::encode(key).with_alphabet(bs58::Alphabet::BITCOIN).into_string();
        [dirname.as_ref().unwrap(), std::path::Path::new(&filename)].into_iter().collect()
    }
}

impl ArchivalStorage for FilesystemArchiver {
    fn put(&self, col: DBCol, key: &[u8], value: &[u8]) -> io::Result<()> {
        use rustix::fs::{Mode, OFlags};
        let mut temp_file = tempfile::Builder::new()
            .make_in("", |filename| {
                let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
                let flags = OFlags::CREATE | OFlags::TRUNC | OFlags::WRONLY;
                Ok(std::fs::File::from(rustix::fs::openat(&self.base_dir, filename, flags, mode)?))
            })
            .unwrap();

        temp_file.write_all(value).unwrap();

        let temp_path = temp_file.into_temp_path();
        let file_path = self.get_path(col, key);
        rustix::fs::renameat(&self.base_dir, &*temp_path, &self.base_dir, file_path).unwrap();
        std::mem::forget(temp_path);
        Ok(())
    }

    fn get(&self, col: DBCol, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        use rustix::fs::{Mode, OFlags};
        let mode = Mode::empty();
        let flags = OFlags::RDONLY;
        let file_path = self.get_path(col, key);
        let file = rustix::fs::openat(&self.base_dir, &file_path, flags, mode);
        let file = match file {
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(file) => file,
        };
        let stat = rustix::fs::fstat(&file).unwrap();
        let mut buffer: Vec<u8> = Vec::with_capacity(stat.st_size.try_into().unwrap());
        let mut file = std::fs::File::from(file);
        file.read_to_end(&mut buffer).unwrap();
        Ok(Some(buffer))
    }

    fn delete(&self, col: DBCol, key: &[u8]) -> io::Result<()> {
        let file_path = self.get_path(col, key);
        Ok(rustix::fs::unlinkat(&self.base_dir, &file_path, rustix::fs::AtFlags::empty())?)
    }
}
