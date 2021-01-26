use bytes::Buf;
use bytes::BufMut;
use futures::task::Context;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;
use strum::AsStaticRef;
use tokio::io;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

pub fn measure_performance<F, Message, Result>(
    _class_name: &'static str,
    msg: Message,
    f: F,
) -> Result
where
    F: FnOnce(Message) -> Result,
{
    f(msg)
}

pub fn measure_performance_with_debug<F, Message, Result>(
    _class_name: &'static str,
    msg: Message,
    f: F,
) -> Result
where
    F: FnOnce(Message) -> Result,
    Message: AsStaticRef<str>,
{
    f(msg)
}

pub fn print_performance_stats(_sleep_time: Duration) {}

pub struct NearReadHalf<T> {
    pub inner: ReadHalf<T>,
}

pub struct NearWriteHalf<T> {
    pub inner: WriteHalf<T>,
}

impl<T: AsyncRead> AsyncRead for NearReadHalf<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<tokio::io::Result<usize>> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll_read(cx, buf)
    }

    fn poll_read_buf<B: BufMut>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut B,
    ) -> Poll<tokio::io::Result<usize>> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll_read_buf(cx, buf)
    }
}

impl<T: AsyncWrite> AsyncWrite for NearWriteHalf<T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll_shutdown(cx)
    }

    fn poll_write_buf<B: Buf>(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut B,
    ) -> Poll<Result<usize, io::Error>> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.inner) }.poll_write_buf(cx, buf)
    }
}

pub struct MeasurePerf {}

impl MeasurePerf {
    pub fn new(_class_name: &'static str, _msg: &'static str) -> Self {
        Self {}
    }

    pub fn done(self) {}
}
