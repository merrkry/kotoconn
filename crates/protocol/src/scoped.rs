use crate::{BoxStream, ChunkBuffer, Scope, Stream, WorkGuard};
use bytes::Bytes;
use futures_util::task::AtomicWaker;
use std::{
    future::Future,
    io,
    pin::Pin,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr, Ordering},
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct Value {
    stream: BoxStream,
    _work: WorkGuard,
}

struct Shared {
    value: AtomicPtr<Value>,
    closed: AtomicBool,
    reader: AtomicWaker,
    writer: AtomicWaker,
}

impl Shared {
    fn take(&self) -> Option<Box<Value>> {
        let pointer = self.value.swap(ptr::null_mut(), Ordering::AcqRel);
        if pointer.is_null() {
            return None;
        }
        // SAFETY: swap transfers the slot's sole Box to this caller. Only put
        // publishes pointers, and neither cancellation nor I/O can take it twice.
        Some(unsafe { Box::from_raw(pointer) })
    }

    fn put(&self, value: Box<Value>) {
        if self.closed.load(Ordering::Acquire) {
            return;
        }
        debug_assert!(self.value.load(Ordering::Relaxed).is_null());
        self.value.store(Box::into_raw(value), Ordering::Release);
        // Cancellation may have observed an empty slot while I/O held it.
        // Recheck after publication so either cancellation or this owner drops it.
        if self.closed.load(Ordering::Acquire) {
            drop(self.take());
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        drop(self.take());
        self.reader.wake();
        self.writer.wake();
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        drop(self.take());
    }
}

struct Owner(Arc<Shared>);

impl Drop for Owner {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// Setup runs eagerly and cancellation revokes idle I/O. The owner task carries
/// no payload: established calls operate directly on the concrete stream.
pub fn stream_task(
    scope: Scope,
    connect: impl Future<Output = anyhow::Result<BoxStream>> + Send + 'static,
) -> anyhow::Result<BoxStream> {
    let shared = Arc::new(Shared {
        value: AtomicPtr::new(ptr::null_mut()),
        closed: AtomicBool::new(false),
        reader: AtomicWaker::new(),
        writer: AtomicWaker::new(),
    });
    let owner = Owner(shared.clone());
    let tracking = scope.clone();
    scope.spawn(async move {
        let owner = owner;
        let stream = connect.await?;
        let work = tracking.track()?;
        owner.0.put(Box::new(Value {
            stream,
            _work: work,
        }));
        owner.0.reader.wake();
        owner.0.writer.wake();
        std::future::pending::<()>().await;
        Ok(())
    })?;
    Ok(Box::pin(Scoped { shared, scope }))
}

struct Scoped {
    shared: Arc<Shared>,
    scope: Scope,
}

impl Scoped {
    fn with<T>(&mut self, operation: impl FnOnce(Pin<&mut dyn Stream>) -> T) -> Option<T> {
        let mut value = self.shared.take()?;
        let result = operation(value.stream.as_mut());
        self.shared.put(value);
        Some(result)
    }

    fn unavailable<T>(&self) -> Poll<io::Result<T>> {
        if self.shared.closed.load(Ordering::Acquire) {
            Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Scoped {
    fn drop(&mut self) {
        self.shared.close();
        self.scope.close();
    }
}

impl AsyncRead for Scoped {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.shared.reader.register(cx.waker());
        self.with(|stream| stream.poll_read(cx, out))
            .unwrap_or_else(|| {
                if self.shared.closed.load(Ordering::Acquire) {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                }
            })
    }
}

impl AsyncWrite for Scoped {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.shared.writer.register(cx.waker());
        self.with(|stream| stream.poll_write(cx, bytes))
            .unwrap_or_else(|| self.unavailable())
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shared.writer.register(cx.waker());
        self.with(|stream| stream.poll_flush(cx))
            .unwrap_or_else(|| self.unavailable())
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.shared.writer.register(cx.waker());
        self.with(|stream| stream.poll_shutdown(cx))
            .unwrap_or_else(|| self.unavailable())
    }
}

impl Stream for Scoped {
    fn poll_read_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ChunkBuffer,
    ) -> Poll<io::Result<Bytes>> {
        self.shared.reader.register(cx.waker());
        self.with(|stream| stream.poll_read_chunk(cx, buffer))
            .unwrap_or_else(|| {
                if self.shared.closed.load(Ordering::Acquire) {
                    Poll::Ready(Ok(Bytes::new()))
                } else {
                    Poll::Pending
                }
            })
    }
    fn poll_write_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut Bytes,
    ) -> Poll<io::Result<usize>> {
        self.shared.writer.register(cx.waker());
        self.with(|stream| stream.poll_write_chunk(cx, bytes))
            .unwrap_or_else(|| self.unavailable())
    }
    fn consume_chunk(mut self: Pin<&mut Self>, bytes: usize) {
        self.with(|stream| stream.consume_chunk(bytes));
    }
}
