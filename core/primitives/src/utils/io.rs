use std::io::{self, Write};

/// Wrapper for Write that counts number of bytes written.
/// It also accepts an optional limit for the number of bytes written;
/// if this limit is exceeded, write operation raises an error.
pub struct CountingWrite<W: Write> {
    inner: W,
    /// Total number of bytes written.
    written: u64,
}

impl<W: Write> CountingWrite<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, written: 0 }
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
        let last_written = self.inner.write(buffer)?;
        self.written = self.written.saturating_add(last_written as u64);
        Ok(last_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut};
    use std::io::{self};

    #[test]
    fn counting_write() {
        let mut counting_write = super::CountingWrite::new(Vec::new().writer());

        let source: Vec<u8> = (1..=42).collect();
        let bytes_written = io::copy(&mut source.reader(), &mut counting_write).unwrap();

        assert_eq!(bytes_written, 42);
        assert_eq!(bytes_written, counting_write.bytes_written().as_u64());

        let target = counting_write.into_inner().into_inner();
        assert_eq!(target, source);
    }
}
