use super::ArchivalStorage;
use std::io;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Write;

pub(crate) struct FilesystemArchiver {
    base_dir: rustix::fd::OwnedFd,
}

impl FilesystemArchiver {
    pub(crate) fn open(
        base_path: &std::path::Path,
        sub_paths: Vec<&std::path::Path>,
    ) -> io::Result<Self> {
        Self::setup_dirs(base_path, sub_paths).unwrap();
        let dir =
            rustix::fs::open(base_path, rustix::fs::OFlags::DIRECTORY, rustix::fs::Mode::empty())
                .unwrap();
        tracing::debug!(
            target: "archiver",
            path = %base_path.display(),
            message = "opened archive directory"
        );
        Ok(Self { base_dir: dir })
    }

    fn setup_dirs(base_path: &std::path::Path, sub_paths: Vec<&std::path::Path>) -> io::Result<()> {
        ignore_if_exists(std::fs::create_dir_all(base_path))?;
        for sub_path in sub_paths.into_iter() {
            ignore_if_exists(std::fs::create_dir(&base_path.join(sub_path)))?;
        }
        Ok(())
    }
}

impl ArchivalStorage for FilesystemArchiver {
    fn put(&self, path: &std::path::Path, value: &[u8]) -> io::Result<()> {
        use rustix::fs::{Mode, OFlags};
        let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
        let flags = OFlags::CREATE | OFlags::TRUNC | OFlags::WRONLY;
        let file_path = path;
        let fd = rustix::fs::openat(&self.base_dir, file_path, flags, mode)?;
        let mut file = std::fs::File::from(fd);
        file.write_all(value)
    }

    fn get(&self, path: &std::path::Path) -> io::Result<Option<Vec<u8>>> {
        use rustix::fs::{Mode, OFlags};
        let mode = Mode::empty();
        let flags = OFlags::RDONLY;
        let file_path = path;
        let file = rustix::fs::openat(&self.base_dir, file_path, flags, mode);
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
}

fn ignore_if_exists(result: io::Result<()>) -> io::Result<()> {
    match result {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.kind() == ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(e)
            }
        }
    }
}