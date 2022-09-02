use bytes::BytesMut;
use futures_core::{ready, Stream};
use pin_project_lite::pin_project;
use std::pin::Pin;
use bytes::Buf;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;
use tokio_util::io::poll_read_buf;
use near_network_primitives::types::ReasonForBan;
use std::io;
use crate::peer::codec::NETWORK_MESSAGE_MAX_SIZE_BYTES;

struct State {
    buffer: BytesMut,
}

pin_project! {
    pub(crate) struct FramedRead<T> {
        #[pin]
        inner: T,
        state: State,
    }
}

impl<T:AsyncRead> FramedRead<T> {
    pub fn new(inner: T) -> FramedRead<T> {
        FramedRead {
            inner,
            state: Default::default(),
        }
    }
}

impl Default for State {
    fn default() -> Self {
        Self { buffer: BytesMut::new() }
    }
}

impl State {
    fn decode(&mut self) -> Result<Option<Result<Vec<u8>, ReasonForBan>>,io::Error> {
        let buf = &mut self.buffer;
        let len_buf = match buf.get(..4).and_then(|s| <[u8; 4]>::try_from(s).ok()) {
            // not enough bytes to start decoding
            None => return Ok(None),
            Some(res) => res,
        };

        let len = u32::from_le_bytes(len_buf) as usize;
        if len > NETWORK_MESSAGE_MAX_SIZE_BYTES {
            // If this point is reached, abusive peer is banned.
            return Ok(Some(Err(ReasonForBan::Abusive)));
        }

        if let Some(data_buf) = buf.get(4..4 + len) {
            let res = Some(Ok(data_buf.to_vec()));
            buf.advance(4 + len);
            if buf.is_empty() && buf.capacity() > 0 {
                *buf = BytesMut::new();
            }
            Ok(res)
        } else {
            // not enough bytes, keep waiting
            Ok(None)
        }
    }
}

impl<T:AsyncRead> Stream for FramedRead<T> {
    type Item = Result<Result<Vec<u8>, ReasonForBan>,io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut pinned = self.project();
        loop {
            if let Some(frame) = pinned.state.decode()? {
                return Poll::Ready(Some(Ok(frame)));
            }
            pinned.state.buffer.reserve(1);
            if ready!(poll_read_buf(pinned.inner.as_mut(), cx, &mut pinned.state.buffer)?)==0 {
                // TODO: check that this is equivalent to reporting EOF
                return Poll::Ready(None);
            }
        }
    }
}
/*
#[cfg(test)]
mod tests {
    use super::{ThrottleController, ThrottleFramedRead};
    use bytes::{Buf, BytesMut};
    use futures_core::Stream;
    use std::error::Error;
    use std::pin::Pin;
    use std::ptr::null;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::Instant;
    use tokio_stream::StreamExt;
    use tokio_util::codec::Decoder;

    #[derive(Default)]
    pub struct Codec {}

    impl Decoder for Codec {
        type Item = u8;
        type Error = std::io::Error;

        fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, std::io::Error> {
            if buf.is_empty() {
                // not enough bytes to start decoding
                return Ok(None);
            }
            let res = buf[0];
            buf.advance(1);
            Ok(Some(res))
        }
    }

    unsafe fn noop_clone(_data: *const ()) -> RawWaker {
        noop_raw_waker()
    }

    unsafe fn noop(_data: *const ()) {}

    const NOOP_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(noop_clone, noop, noop, noop);

    const fn noop_raw_waker() -> RawWaker {
        RawWaker::new(null(), &NOOP_WAKER_VTABLE)
    }
    pub fn noop_waker() -> Waker {
        // FIXME: Since 1.46.0 we can use transmute in consts, allowing this function to be const.
        unsafe { Waker::from_raw(noop_raw_waker()) }
    }

    #[tokio::test]
    async fn test_throttle_framed_read() -> Result<(), Box<dyn Error>> {
        let rate_limiter = ThrottleController::new(1, usize::MAX);

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("TcpListener::bind");
        let addr = listener.local_addr().expect("addr");

        let client_stream = TcpStream::connect(addr).await?;

        let (mut server_tcp_stream, _socket) = listener.accept().await.unwrap();

        // Write some data.
        server_tcp_stream.write_all(b"hello world!").await?;

        let (read, _write) = tokio::io::split(client_stream);
        let mut read = ThrottleFramedRead::new(read, rate_limiter.clone());

        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        let start = Instant::now();
        // Unfortunately, the test times out for some reason.
        loop {
            let msg = read.next().await;
            if let Some(msg) = msg {
                tracing::info!(message = "GOT", ?msg);
            }

            assert!(start.elapsed() < Duration::from_secs(10), "timed out {:?}", start.elapsed());

            match Pin::new(&mut read).poll_next(&mut context) {
                Poll::Pending => {
                    // this expected
                }
                Poll::Ready(Some(Ok(101))) => {
                    // got 'h' as expected
                    break;
                }
                Poll::Ready(val) => {
                    panic!("unexpected got {:?}", val);
                }
            }
        }

        rate_limiter.add_msg(1);
        rate_limiter.add_msg(1);

        match Pin::new(&mut read).poll_next(&mut context) {
            Poll::Pending => {
                // this expected
            }
            Poll::Ready(x) => {
                panic!("got {:?}", x);
            }
        }

        Ok(())
    }
}
*/
