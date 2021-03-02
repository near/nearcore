// LICENCE: https://github.com/actix/actix/blob/master/LICENSE-APACHE
// LICENCE: https://github.com/actix/actix/blob/master/LICENSE-MIT


use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::{io, task};

use actix::{Actor, ActorContext, ActorFuture, AsyncContext, Running, SpawnHandle};
use bitflags::bitflags;
use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::codec::Encoder;

// This file was copied from actix-0.11.0-beta.1/src/io.rs
// The only change was to add EncoderCallBack

bitflags! {
    struct Flags: u8 {
        const CLOSING = 0b0000_0001;
        const CLOSED = 0b0000_0010;
    }
}

const LOW_WATERMARK: usize = 4 * 1024;
const HIGH_WATERMARK: usize = 4 * LOW_WATERMARK;

#[allow(unused_variables)]
pub trait WriteHandler<E>
where
    Self: Actor,
    Self::Context: ActorContext,
{
    /// Called when the writer emits error.
    ///
    /// If this method returns `ErrorAction::Continue` writer processing
    /// continues otherwise stream processing stops.
    fn error(&mut self, err: E, ctx: &mut Self::Context) -> Running {
        Running::Stop
    }

    /// Called when the writer finishes.
    ///
    /// By default this method stops actor's `Context`.
    fn finished(&mut self, ctx: &mut Self::Context) {
        ctx.stop()
    }
}

struct InnerWriter<E: From<io::Error>, K: EncoderCallBack> {
    flags: Flags,
    buffer: BytesMut,
    error: Option<E>,
    low: usize,
    high: usize,
    handle: SpawnHandle,
    task: Option<task::Waker>,
    callback: K,
}

pub struct EmptyCallBack {}

impl EncoderCallBack for EmptyCallBack {
    fn drained(&mut self, _bytes: usize, _buf_len: usize, _buf_capacity: usize) {}
}

struct UnsafeWriter<T: AsyncWrite, E: From<io::Error>, K: EncoderCallBack>(
    Rc<RefCell<InnerWriter<E, K>>>,
    Rc<RefCell<T>>,
);

impl<T: AsyncWrite, E: From<io::Error>, K: EncoderCallBack> Clone for UnsafeWriter<T, E, K> {
    fn clone(&self) -> Self {
        UnsafeWriter(self.0.clone(), self.1.clone())
    }
}

pub trait EncoderCallBack {
    fn drained(&mut self, bytes: usize, buf_len: usize, buf_capacity: usize);
}

/// A wrapper for the `AsyncWrite` and `Encoder` types. The AsyncWrite will be flushed when this
/// struct is dropped.
pub struct FramedWrite<I, T: AsyncWrite + Unpin, U: Encoder<I>, K: EncoderCallBack = EmptyCallBack>
{
    enc: U,
    inner: UnsafeWriter<T, U::Error, K>,
}

impl<I, T: AsyncWrite + Unpin, U: Encoder<I>, K: 'static + EncoderCallBack>
    FramedWrite<I, T, U, K>
{
    pub fn new<A, C>(io: T, enc: U, callback: K, ctx: &mut C) -> Self
    where
        A: Actor<Context = C> + WriteHandler<U::Error>,
        C: AsyncContext<A>,
        U::Error: 'static,
        T: Unpin + 'static,
    {
        let inner = UnsafeWriter(
            Rc::new(RefCell::new(InnerWriter {
                flags: Flags::empty(),
                buffer: BytesMut::new(),
                error: None,
                low: LOW_WATERMARK,
                high: HIGH_WATERMARK,
                handle: SpawnHandle::default(),
                task: None,
                callback,
            })),
            Rc::new(RefCell::new(io)),
        );
        let h = ctx.spawn(WriterFut { inner: inner.clone(), act: PhantomData });

        let writer = Self { enc, inner };
        writer.inner.0.borrow_mut().handle = h;
        writer
    }

    pub fn from_buffer<A, C>(io: T, enc: U, buffer: BytesMut, callback: K, ctx: &mut C) -> Self
    where
        A: Actor<Context = C> + WriteHandler<U::Error>,
        C: AsyncContext<A>,
        U::Error: 'static,
        T: Unpin + 'static,
    {
        let inner = UnsafeWriter(
            Rc::new(RefCell::new(InnerWriter {
                buffer,
                flags: Flags::empty(),
                error: None,
                low: LOW_WATERMARK,
                high: HIGH_WATERMARK,
                handle: SpawnHandle::default(),
                task: None,
                callback: callback,
            })),
            Rc::new(RefCell::new(io)),
        );
        let h = ctx.spawn(WriterFut { inner: inner.clone(), act: PhantomData });

        let writer = Self { enc, inner };
        writer.inner.0.borrow_mut().handle = h;
        writer
    }

    /// Gracefully closes the sink.
    ///
    /// The closing happens asynchronously.
    pub fn close(&mut self) {
        self.inner.0.borrow_mut().flags.insert(Flags::CLOSING);
    }

    /// Checks if the sink is closed.
    pub fn closed(&self) -> bool {
        self.inner.0.borrow().flags.contains(Flags::CLOSED)
    }

    /// Sets the write buffer capacity.
    pub fn set_buffer_capacity(&mut self, low: usize, high: usize) {
        let mut inner = self.inner.0.borrow_mut();
        inner.low = low;
        inner.high = high;
    }

    /// Writes an item to the sink.
    pub fn write(&mut self, item: I) -> bool {
        let mut inner = self.inner.0.borrow_mut();
        let mut success = true;
        let _ = self.enc.encode(item, &mut inner.buffer).map_err(|e| {
            inner.error = Some(e);
            success = false;
        });
        if let Some(task) = inner.task.take() {
            task.wake_by_ref();
        }
        success
    }

    /// Returns the `SpawnHandle` for this writer.
    pub fn handle(&self) -> SpawnHandle {
        self.inner.0.borrow().handle
    }
}

impl<I, T: AsyncWrite + Unpin, U: Encoder<I>, K: EncoderCallBack> Drop for FramedWrite<I, T, U, K> {
    fn drop(&mut self) {
        // Attempts to write any remaining bytes to the stream and flush it
        let mut async_writer = self.inner.1.borrow_mut();
        let inner = self.inner.0.borrow_mut();
        if !inner.buffer.is_empty() {
            // Results must be ignored during drop, as the errors cannot be handled meaningfully
            let _ = async_writer.write(&inner.buffer);
            let _ = async_writer.flush();
        }
    }
}

struct WriterFut<T, E, A, K>
where
    T: AsyncWrite + Unpin,
    E: From<io::Error>,
    K: EncoderCallBack,
{
    act: PhantomData<A>,
    inner: UnsafeWriter<T, E, K>,
}

impl<T: 'static, E: 'static, A, K: 'static + EncoderCallBack> ActorFuture for WriterFut<T, E, A, K>
where
    T: AsyncWrite + Unpin,
    E: From<io::Error>,
    A: Actor + WriteHandler<E>,
    A::Context: AsyncContext<A>,
{
    type Output = ();
    type Actor = A;

    fn poll(
        self: Pin<&mut Self>,
        act: &mut A,
        ctx: &mut A::Context,
        task: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut inner = this.inner.0.borrow_mut();
        if let Some(err) = inner.error.take() {
            if act.error(err, ctx) == Running::Stop {
                act.finished(ctx);
                return Poll::Ready(());
            }
        }

        let mut io = this.inner.1.borrow_mut();
        inner.task = None;
        while !inner.buffer.is_empty() {
            match Pin::new(io.deref_mut()).poll_write(task, &inner.buffer) {
                Poll::Ready(Ok(n)) => {
                    if n == 0
                        && act.error(
                            io::Error::new(
                                io::ErrorKind::WriteZero,
                                "failed to write frame to transport",
                            )
                            .into(),
                            ctx,
                        ) == Running::Stop
                    {
                        act.finished(ctx);
                        return Poll::Ready(());
                    }
                    let _ = inner.buffer.split_to(n);
                    let len = inner.buffer.len();
                    let capacity = inner.buffer.capacity();
                    inner.callback.drained(n, len, capacity);
                }
                Poll::Ready(Err(ref e)) if e.kind() == io::ErrorKind::WouldBlock => {
                    if inner.buffer.len() > inner.high {
                        ctx.wait(WriterDrain { inner: this.inner.clone(), act: PhantomData });
                    }
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => {
                    if act.error(e.into(), ctx) == Running::Stop {
                        act.finished(ctx);
                        return Poll::Ready(());
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // Try flushing the underlying IO
        match Pin::new(io.deref_mut()).poll_flush(task) {
            Poll::Ready(Ok(_)) => (),
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(ref e)) if e.kind() == io::ErrorKind::WouldBlock => {
                return Poll::Pending;
            }
            Poll::Ready(Err(e)) => {
                if act.error(e.into(), ctx) == Running::Stop {
                    act.finished(ctx);
                    return Poll::Ready(());
                }
            }
        }

        // close if closing and we don't need to flush any data
        if inner.flags.contains(Flags::CLOSING) {
            inner.flags |= Flags::CLOSED;
            act.finished(ctx);
            Poll::Ready(())
        } else {
            inner.task = Some(task.waker().clone());
            Poll::Pending
        }
    }
}

struct WriterDrain<T, E, A, K>
where
    T: AsyncWrite + Unpin,
    E: From<io::Error>,
    K: EncoderCallBack,
{
    act: PhantomData<A>,
    inner: UnsafeWriter<T, E, K>,
}

impl<T, E, A, K> ActorFuture for WriterDrain<T, E, A, K>
where
    T: AsyncWrite + Unpin,
    E: From<io::Error>,
    A: Actor,
    A::Context: AsyncContext<A>,
    K: EncoderCallBack,
{
    type Output = ();
    type Actor = A;

    fn poll(
        self: Pin<&mut Self>,
        _: &mut A,
        _: &mut A::Context,
        task: &mut Context<'_>,
    ) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut inner = this.inner.0.borrow_mut();
        if inner.error.is_some() {
            return Poll::Ready(());
        }
        let mut io = this.inner.1.borrow_mut();
        while !inner.buffer.is_empty() {
            match Pin::new(io.deref_mut()).poll_write(task, &inner.buffer) {
                Poll::Ready(Ok(n)) => {
                    if n == 0 {
                        inner.error = Some(
                            io::Error::new(
                                io::ErrorKind::WriteZero,
                                "failed to write frame to transport",
                            )
                            .into(),
                        );
                        return Poll::Ready(());
                    }

                    let _ = inner.buffer.split_to(n);

                    let len = inner.buffer.len();
                    let capacity = inner.buffer.capacity();
                    inner.callback.drained(n, len, capacity);
                }
                Poll::Ready(Err(ref e)) if e.kind() == io::ErrorKind::WouldBlock => {
                    return if inner.buffer.len() < inner.low {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    };
                }
                Poll::Ready(Err(e)) => {
                    inner.error = Some(e.into());
                    return Poll::Ready(());
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(())
    }
}
