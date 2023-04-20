use hyper::body::HttpBody;
use indicatif::{ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

#[derive(thiserror::Error, Debug)]
pub enum FileDownloadError {
    #[error("{0}")]
    HttpError(hyper::Error),
    #[error("Failed to open temporary file")]
    OpenError(#[source] std::io::Error),
    #[error("Failed to write to temporary file at {0:?}")]
    WriteError(PathBuf, #[source] std::io::Error),
    #[error("Failed to decompress XZ stream: {0}")]
    XzDecodeError(#[from] xz2::stream::Error),
    #[error("Failed to decompress XZ stream: internal error: unexpected status {0:?}")]
    XzStatusError(String),
    #[error("Failed to rename temporary file {0:?} to {1:?}")]
    RenameError(PathBuf, PathBuf, #[source] std::io::Error),
    #[error("Invalid URI")]
    UriError(#[from] hyper::http::uri::InvalidUri),
    #[error("Failed to remove temporary file: {0}. Download previously failed")]
    RemoveTemporaryFileError(std::io::Error, #[source] Box<FileDownloadError>),
}

pub(crate) fn run_download_file(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { download_file(url, path).await })
}

/// Downloads resource at given `uri` and saves it to `file`.  On failure,
/// `file` may be left in inconsistent state (i.e. may contain partial data).
///
/// If the downloaded file is an XZ stream (i.e. starts with the XZ 6-byte magic
/// number), transparently decompresses the file as it’s being downloaded.
async fn download_file_impl(
    uri: hyper::Uri,
    path: &std::path::Path,
    file: tokio::fs::File,
) -> Result<(), FileDownloadError> {
    let mut out = AutoXzDecoder::new(path, file);
    let https_connector = hyper_tls::HttpsConnector::new();
    let client = hyper::Client::builder().build::<_, hyper::Body>(https_connector);
    let mut resp = client.get(uri).await.map_err(FileDownloadError::HttpError)?;
    let bar = if let Some(file_size) = resp.size_hint().upper() {
        let bar = ProgressBar::new(file_size);
        bar.set_style(
            ProgressStyle::default_bar().template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} [{bytes_per_sec}] ({eta})"
            ).progress_chars("#>-")
        );
        bar
    } else {
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] {bytes} [{bytes_per_sec}]"),
        );
        bar
    };

    #[cfg(test)]
    let bar = {
        drop(bar);
        ProgressBar::hidden()
    };

    while let Some(next_chunk_result) = resp.data().await {
        let next_chunk = next_chunk_result.map_err(FileDownloadError::HttpError)?;
        out.write_all(next_chunk.as_ref()).await?;
        bar.inc(next_chunk.len() as u64);
    }
    out.finish().await?;
    bar.finish();
    Ok(())
}

/// Downloads a resource at given `url` and saves it to `path`.  On success, if
/// file at `path` exists it will be overwritten.  On failure, file at `path` is
/// left unchanged (if it exists).
async fn download_file(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    let uri = url.parse()?;

    let (tmp_file, tmp_path) = {
        let tmp_dir = path.parent().unwrap_or(Path::new("."));
        tempfile::NamedTempFile::new_in(tmp_dir).map_err(FileDownloadError::OpenError)?.into_parts()
    };

    let result = match download_file_impl(uri, &tmp_path, tokio::fs::File::from_std(tmp_file)).await
    {
        Err(err) => Err((tmp_path, err)),
        Ok(()) => tmp_path.persist(path).map_err(|e| {
            let from = e.path.to_path_buf();
            let to = path.to_path_buf();
            (e.path, FileDownloadError::RenameError(from, to, e.error))
        }),
    };

    result.map_err(|(tmp_path, err)| match tmp_path.close() {
        Ok(()) => err,
        Err(close_err) => FileDownloadError::RemoveTemporaryFileError(close_err, Box::new(err)),
    })
}

/// Object which allows transparent XZ decoding when saving data to a file.
/// It automatically detects whether the data being read is compressed by
/// looking at the magic at the beginning of the file.
struct AutoXzDecoder<'a> {
    path: &'a std::path::Path,
    file: tokio::fs::File,
    state: AutoXzState,
}

/// State in which of the AutoXzDecoder
enum AutoXzState {
    /// Given number of bytes have been read so far and all of them match bytes
    /// in [`XZ_HEADER_MAGIC`].  The object starts in `Probing(0)` state and the
    /// number never reaches the length of the [`XZ_HEADER_MAGIC`] buffer.
    Probing(usize),

    /// The header did not match XZ stream header and thus the data is passed
    /// through.
    PlainText,

    /// The header did match XZ stream header and thus the data is being
    /// decompressed.
    Compressed(xz2::stream::Stream, Box<[u8]>),
}

/// Header that every XZ streams starts with.  See
/// <https://tukaani.org/xz/xz-file-format-1.0.4.txt> § 2.1.1.1.
static XZ_HEADER_MAGIC: [u8; 6] = [0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00];

impl<'a> AutoXzDecoder<'a> {
    fn new(path: &'a std::path::Path, file: tokio::fs::File) -> Self {
        Self { path, file, state: AutoXzState::Probing(0) }
    }

    /// Writes data from the chunk to the output file automatically
    /// decompressing it if the stream is XZ-compressed.  Note that once all the
    /// data has been written [`Self::finish`] function must be called to flush
    /// internal buffers.
    async fn write_all(&mut self, chunk: &[u8]) -> Result<(), FileDownloadError> {
        if let Some(len) = self.probe(chunk) {
            if len != 0 {
                self.write_all_impl(&XZ_HEADER_MAGIC[..len]).await?;
            }
            self.write_all_impl(chunk).await?;
        }
        Ok(())
    }

    /// Flushes all internal buffers and closes the output file.
    async fn finish(mut self) -> Result<(), FileDownloadError> {
        match self.state {
            AutoXzState::Probing(pos) => self.write_all_raw(&XZ_HEADER_MAGIC[..pos]).await?,
            AutoXzState::PlainText => (),
            AutoXzState::Compressed(ref mut stream, ref mut buffer) => {
                Self::decompress(self.path, &mut self.file, stream, buffer, b"").await?
            }
        }
        self.file
            .flush()
            .await
            .map_err(|e| FileDownloadError::WriteError(self.path.to_path_buf(), e))
    }

    /// If object is still in `Probing` state, read more data from the input to
    /// determine whether it’s XZ stream or not.  Updates `state` accordingly.
    /// If probing succeeded, returns number of bytes from XZ header magic that
    /// need to be processed before `chunk` is processed.  If the entire data
    /// from `chunk` has been processed and it should be discarded by the
    /// caller, returns `None`.
    fn probe(&mut self, chunk: &[u8]) -> Option<usize> {
        if chunk.is_empty() {
            None
        } else if let AutoXzState::Probing(pos) = self.state {
            let len = std::cmp::min(XZ_HEADER_MAGIC.len() - pos, chunk.len());
            if XZ_HEADER_MAGIC[pos..(pos + len)] != chunk[..len] {
                self.state = AutoXzState::PlainText;
                Some(pos)
            } else if pos + len == XZ_HEADER_MAGIC.len() {
                let stream = xz2::stream::Stream::new_stream_decoder(u64::max_value(), 0).unwrap();
                // TODO(mina86): Once ‘new_uninit’ feature gets stabilised
                // replaced buffer initialisation by:
                //     let buffer = Box::new_uninit_slice(64 << 10);
                //     let buffer = unsafe { buffer.assume_init() };
                let buffer = vec![0u8; 64 << 10].into_boxed_slice();
                self.state = AutoXzState::Compressed(stream, buffer);
                Some(pos)
            } else {
                self.state = AutoXzState::Probing(pos + len);
                None
            }
        } else {
            Some(0)
        }
    }

    /// Writes data to the output file.  Panics if the object is still in
    /// probing stage.
    async fn write_all_impl(&mut self, chunk: &[u8]) -> Result<(), FileDownloadError> {
        match self.state {
            AutoXzState::Probing(_) => unreachable!(),
            AutoXzState::PlainText => self.write_all_raw(chunk).await,
            AutoXzState::Compressed(ref mut stream, ref mut buffer) => {
                Self::decompress(self.path, &mut self.file, stream, buffer, chunk).await
            }
        }
    }

    /// Writes data to output file directly.
    async fn write_all_raw(&mut self, chunk: &[u8]) -> Result<(), FileDownloadError> {
        self.file
            .write_all(chunk)
            .await
            .map_err(|e| FileDownloadError::WriteError(self.path.to_path_buf(), e))
    }

    /// Internal implementation for [`Self::write_all`] and [`Self::finish`]
    /// methods used when performing decompression.  Calling it with an empty
    /// `chunk` indicates the end of the compressed data.
    async fn decompress(
        path: &std::path::Path,
        file: &mut tokio::fs::File,
        stream: &mut xz2::stream::Stream,
        buffer: &mut [u8],
        mut chunk: &[u8],
    ) -> Result<(), FileDownloadError> {
        let action =
            if chunk.is_empty() { xz2::stream::Action::Finish } else { xz2::stream::Action::Run };
        loop {
            let total_in = stream.total_in();
            let total_out = stream.total_out();
            let status = stream.process(chunk, buffer, action)?;
            match status {
                xz2::stream::Status::Ok => (),
                xz2::stream::Status::StreamEnd => (),
                status => {
                    let status = format!("{:?}", status);
                    tracing::error!(target: "near", "Got unexpected status ‘{}’ when decompressing downloaded file.", status);
                    return Err(FileDownloadError::XzStatusError(status));
                }
            };
            let read = (stream.total_in() - total_in).try_into().unwrap();
            chunk = &chunk[read..];
            let out = (stream.total_out() - total_out).try_into().unwrap();
            file.write_all(&buffer[..out])
                .await
                .map_err(|e| FileDownloadError::WriteError(path.to_path_buf(), e))?;
            if chunk.is_empty() {
                break Ok(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Request, Response, Server};
    use std::convert::Infallible;
    use std::sync::Arc;

    async fn check_file_download(payload: &[u8], expected: Result<&[u8], &str>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();

        let payload = Arc::new(payload.to_vec());
        tokio::task::spawn(async move {
            let make_svc = make_service_fn(move |_conn| {
                let payload = Arc::clone(&payload);
                let handle_request = move |_: Request<Body>| {
                    let payload = Arc::clone(&payload);
                    async move { Ok::<_, Infallible>(Response::new(Body::from(payload.to_vec()))) }
                };
                async move { Ok::<_, Infallible>(service_fn(handle_request)) }
            });
            let server = Server::from_tcp(listener).unwrap().serve(make_svc);
            if let Err(e) = server.await {
                eprintln!("server error: {}", e);
            }
        });

        let tmp_file = tempfile::NamedTempFile::new().unwrap();

        let res = download_file(&format!("http://localhost:{}", port), tmp_file.path())
            .await
            .map(|()| std::fs::read(tmp_file.path()).unwrap());

        match (res, expected) {
            (Ok(res), Ok(expected)) => assert_eq!(&res, expected),
            (Ok(_), Err(_)) => panic!("expected an error"),
            (Err(res), Ok(_)) => panic!("unexpected error: {res}"),
            (Err(res), Err(expected)) => assert_eq!(res.to_string(), expected),
        }
    }

    #[tokio::test]
    async fn test_file_download_plaintext() {
        let data = &[42; 1024];
        check_file_download(data, Ok(data)).await;

        let data = b"A quick brown fox jumps over a lazy dog";
        check_file_download(data, Ok(data)).await;

        let payload = b"\xfd\x37\x7a\x58\x5a\x00\x00\x04\xe6\xd6\xb4\x46\
                        \x02\x00\x21\x01\x1c\x00\x00\x00\x10\xcf\x58\xcc\
                        \x01\x00\x19\x5a\x61\xc5\xbc\xc3\xb3\xc5\x82\xc4\
                        \x87\x20\x67\xc4\x99\xc5\x9b\x6c\xc4\x85\x20\x6a\
                        \x61\xc5\xba\xc5\x84\x00\x00\x00\x89\x4e\xdf\x72\
                        \x66\xbe\xa9\x51\x00\x01\x32\x1a\x20\x18\x94\x30\
                        \x1f\xb6\xf3\x7d\x01\x00\x00\x00\x00\x04\x59\x5a";
        check_file_download(payload, Ok("Zażółć gęślą jaźń".as_bytes())).await;

        let payload = b"\xfd\x37\x7a\x58\x5a\x00A quick brown fox";
        check_file_download(payload, Err("Failed to decompress XZ stream: lzma data error")).await;
    }

    fn auto_xz_test_write_file(
        buffer: &[u8],
        chunk_size: usize,
    ) -> Result<Vec<u8>, FileDownloadError> {
        let (file, path) = tempfile::NamedTempFile::new().unwrap().into_parts();
        let mut out = AutoXzDecoder::new(&path, tokio::fs::File::from_std(file));
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(
            async move {
                for chunk in buffer.chunks(chunk_size) {
                    out.write_all(chunk).await?;
                }
                out.finish().await
            },
        )?;
        Ok(std::fs::read(path).unwrap())
    }

    /// Tests writing plain text of varying lengths through [`AutoXzDecoder`].
    /// Includes test cases where prefix of a XZ header is present at the beginning
    /// of the stream being written.  That tests the object not being fooled by
    /// partial prefix.
    #[test]
    fn test_auto_xz_decode_plain() {
        let mut data: [u8; 39] = *b"A quick brown fox jumps over a lazy dog";
        // On first iteration we’re testing just a plain text data.  On subsequent
        // iterations, we’re testing uncompressed data whose first few bytes match
        // the XZ header.
        for (pos, &ch) in XZ_HEADER_MAGIC.iter().enumerate() {
            for len in [0, 1, 2, 3, 4, 5, 6, 10, 20, data.len()] {
                let buffer = &data[0..len];
                for chunk_size in 1..11 {
                    let got = auto_xz_test_write_file(&buffer, chunk_size).unwrap();
                    assert_eq!(got, buffer, "pos={}, len={}, chunk_size={}", pos, len, chunk_size);
                }
            }
            data[pos] = ch;
        }
    }

    /// Tests writing XZ stream through [`AutoXzDecoder`].  The stream should be
    /// properly decompressed.
    #[test]
    fn test_auto_xz_decode_compressed() {
        let buffer = b"\xfd\x37\x7a\x58\x5a\x00\x00\x04\xe6\xd6\xb4\x46\
                       \x02\x00\x21\x01\x1c\x00\x00\x00\x10\xcf\x58\xcc\
                       \x01\x00\x19\x5a\x61\xc5\xbc\xc3\xb3\xc5\x82\xc4\
                       \x87\x20\x67\xc4\x99\xc5\x9b\x6c\xc4\x85\x20\x6a\
                       \x61\xc5\xba\xc5\x84\x00\x00\x00\x89\x4e\xdf\x72\
                       \x66\xbe\xa9\x51\x00\x01\x32\x1a\x20\x18\x94\x30\
                       \x1f\xb6\xf3\x7d\x01\x00\x00\x00\x00\x04\x59\x5a";
        for chunk_size in 1..11 {
            let got = auto_xz_test_write_file(buffer, chunk_size).unwrap();
            assert_eq!(got, "Zażółć gęślą jaźń".as_bytes());
        }
    }

    /// Tests [`AutoXzDecoder`]’s handling of corrupt XZ streams.  The data being
    /// processed starts with a proper XZ header but what follows is an invalid XZ
    /// data.  This should result in [`FileDownloadError::XzDecodeError`].
    #[test]
    fn test_auto_xz_decode_corrupted() {
        let buffer = b"\xfd\x37\x7a\x58\x5a\x00A quick brown fox";
        for chunk_size in 1..11 {
            let got = auto_xz_test_write_file(buffer, chunk_size);
            assert!(
                matches!(got, Err(FileDownloadError::XzDecodeError(xz2::stream::Error::Data))),
                "got {:?}",
                got
            );
        }
    }
}
