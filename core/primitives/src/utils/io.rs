use std::io::{self, Read, Write};

/// Wrapper for Write that counts number of bytes written.
/// It also allow setting an optional limit to stop writing.
pub struct CountingWrite<W: Write> {
    inner: W,
    /// Total number of bytes written.
    written: u64,
    /// If set, the number of bytes allowed to be written.
    /// If this limit is reached, any additional write will return an error.
    limit: Option<u64>,
}

impl<W: Write> CountingWrite<W> {
    pub fn new_with_limit(inner: W, limit: bytesize::ByteSize) -> Self {
        Self { inner, written: 0, limit: Some(limit.as_u64()) }
    }

    pub fn new(inner: W) -> Self {
        Self { inner, written: 0, limit: None }
    }

    pub fn bytes_written(&self) -> bytesize::ByteSize {
        bytesize::ByteSize::b(self.written)
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for CountingWrite<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if let Some(limit) = self.limit {
            if self.written.saturating_add(buffer.len() as u64) > limit {
                return Err(io::Error::other(format!("Exceeded the limit of {} bytes", limit)));
            }
        }
        let last_written = self.inner.write(buffer)?;
        self.written = self.written.saturating_add(last_written as u64);
        Ok(last_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Wrapper for Write that counts number of bytes written.
/// It also allow setting an optional limit to stop writing.
pub struct CountingRead<R: Read> {
    inner: R,
    /// Total number of bytes read.
    read: u64,
    /// If set, the number of bytes allowed to be written.
    /// If this limit is reached, any additional write will return an error.
    limit: Option<u64>,
}

impl<R: Read> CountingRead<R> {
    pub fn new_with_limit(inner: R, limit: bytesize::ByteSize) -> Self {
        Self { inner, read: 0, limit: Some(limit.as_u64()) }
    }

    pub fn new(inner: R) -> Self {
        Self { inner, read: 0, limit: None }
    }

    pub fn bytes_read(&self) -> bytesize::ByteSize {
        bytesize::ByteSize::b(self.read)
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> Read for CountingRead<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let buffer_size = buffer.len();
        if let Some(limit) = self.limit {
            if self.read.saturating_add(buffer_size as u64) > limit {
                return Err(io::Error::other(format!("Exceeded the limit of {} bytes", limit)));
            }
        }
        let last_read = self.inner.read(buffer)?;
        self.read = self.read.saturating_add(last_read as u64);
        Ok(last_read)
    }
}
