use std::io::{self, Read, Write};

/// Wrapper for Write that counts number of bytes written.
/// It also accepts an optional limit for the number of bytes written;
/// if this limit is exceeded, write operation raises an error.
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

/// Wrapper for Read that counts number of bytes read.
/// It also accepts an optional limit for the number of bytes written;
/// if this limit is exceeded, write operation raises an error.
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

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut};
    use std::io::{self};

    #[test]
    fn counting_writer_without_limit() {
        let mut counting_write = super::CountingWrite::new(Vec::new().writer());

        let source: Vec<u8> = (1..=42).collect();
        let bytes_written = io::copy(&mut source.reader(), &mut counting_write).unwrap();

        assert_eq!(bytes_written, 42);
        assert_eq!(bytes_written, counting_write.bytes_written().as_u64());

        let target = counting_write.into_inner().into_inner();
        assert_eq!(target, source);
    }

    #[test]
    fn counting_writer_with_limit_success() {
        let mut counting_write =
            super::CountingWrite::new_with_limit(Vec::new().writer(), bytesize::ByteSize::b(42));

        let source: Vec<u8> = (1..=42).collect();
        let bytes_written = io::copy(&mut source.reader(), &mut counting_write).unwrap();

        assert_eq!(bytes_written, 42);
        assert_eq!(bytes_written, counting_write.bytes_written().as_u64());

        let target = counting_write.into_inner().into_inner();
        assert_eq!(target, source);
    }

    #[test]
    fn counting_writer_with_limit_fail() {
        let mut counting_write =
            super::CountingWrite::new_with_limit(Vec::new().writer(), bytesize::ByteSize::b(41));

        let source: Vec<u8> = (1..=42).collect();
        let error = io::copy(&mut source.reader(), &mut counting_write).unwrap_err();
        assert_eq!("Exceeded the limit of 41 bytes", error.to_string());
    }

    #[test]
    fn counting_read_without_limit() {
        let source: Vec<u8> = (1..=42).collect();
        let mut counting_read = super::CountingRead::new(source.reader());

        let mut target = Vec::new().writer();
        let bytes_written = io::copy(&mut counting_read, &mut target).unwrap();

        assert_eq!(bytes_written, 42);
        assert_eq!(bytes_written, counting_read.bytes_read().as_u64());
        assert_eq!(target.into_inner(), source);
    }

    #[test]
    fn counting_read_with_limit_success() {
        let source: Vec<u8> = (1..=42).collect();
        let mut counting_read =
            super::CountingRead::new_with_limit(source.reader(), bytesize::ByteSize::b(42));

        let mut target = Vec::new().writer();
        let bytes_written = io::copy(&mut counting_read, &mut target).unwrap();

        assert_eq!(bytes_written, 42);
        assert_eq!(bytes_written, counting_read.bytes_read().as_u64());
        assert_eq!(target.into_inner(), source);
    }

    #[test]
    fn counting_read_with_limit_fail() {
        let source: Vec<u8> = (1..=42).collect();
        let mut counting_read =
            super::CountingRead::new_with_limit(source.reader(), bytesize::ByteSize::b(41));

        let mut target = Vec::new().writer();
        let error = io::copy(&mut counting_read, &mut target).unwrap_err();
        assert_eq!("Exceeded the limit of 41 bytes", error.to_string());
    }
}
