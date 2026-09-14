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
        atomic::{AtomicPtr, Ordering},
    },
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct Value<T> {
    io: T,
    _work: WorkGuard,
}

pub(crate) struct Shared<T: Send> {
    value: AtomicPtr<Value<T>>,
    pub(crate) reader: AtomicWaker,
    pub(crate) writer: AtomicWaker,
}

impl<T: Send> Shared<T> {
    pub(crate) fn with<R>(&self, operation: impl FnOnce(&mut T) -> R) -> Option<R> {
        let mut value = self.take()?;
        let result = operation(&mut value.io);
        self.put(value);
        Some(result)
    }

    fn closed_pointer() -> *mut Value<T> {
        // SAFETY: Value contains a WorkGuard with pointer-aligned storage, so 1
        // can never equal a live allocation. This sentinel is never dereferenced.
        const {
            assert!(std::mem::align_of::<Value<T>>() >= 2);
        }
        ptr::without_provenance_mut(1)
    }

    fn is_closed(&self) -> bool {
        self.value.load(Ordering::Acquire) == Self::closed_pointer()
    }

    fn take(&self) -> Option<Box<Value<T>>> {
        let pointer = self
            .value
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |pointer| {
                (!pointer.is_null() && pointer != Self::closed_pointer()).then_some(ptr::null_mut())
            })
            .ok()?;
        // SAFETY: The successful CAS takes the slot's sole live Box. The closed
        // sentinel is excluded and remains installed permanently once observed.
        Some(unsafe { Box::from_raw(pointer) })
    }

    fn put(&self, value: Box<Value<T>>) {
        let pointer = Box::into_raw(value);
        if let Err(state) = self.value.compare_exchange(
            ptr::null_mut(),
            pointer,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            // SAFETY: The CAS did not publish this Box, so this caller still owns
            // it. Cancellation cannot take it and no future poll can acquire it.
            drop(unsafe { Box::from_raw(pointer) });
            // SAFETY: Only one I/O handle publishes; cancellation is the sole
            // competing writer. It replaces an empty/busy slot with the sentinel.
            assert_eq!(state, Self::closed_pointer(), "occupied I/O slot");
        }
    }

    pub(crate) fn close(&self) {
        let pointer = self.value.swap(Self::closed_pointer(), Ordering::AcqRel);
        if !pointer.is_null() && pointer != Self::closed_pointer() {
            // SAFETY: swap takes ownership of the published Box exactly once.
            // A concurrent poll either took it earlier or observes the sentinel.
            drop(unsafe { Box::from_raw(pointer) });
        }
        self.reader.wake();
        self.writer.wake();
    }
}

impl<T: Send> Drop for Shared<T> {
    fn drop(&mut self) {
        self.close();
    }
}

struct Owner<T: Send>(Arc<Shared<T>>);

impl<T: Send> Drop for Owner<T> {
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
            io: stream,
            _work: work,
        }));
        owner.0.reader.wake();
        owner.0.writer.wake();
        std::future::pending::<()>().await;
        Ok(())
    })?;
    Ok(Box::pin(Scoped { shared, scope }))
}

/// Revocation stays active even when an established owner is never polled again.
pub(crate) fn installed<T: Send + 'static>(scope: &Scope, io: T) -> anyhow::Result<Arc<Shared<T>>> {
    let shared = Arc::new(Shared {
        value: AtomicPtr::new(ptr::null_mut()),
        reader: AtomicWaker::new(),
        writer: AtomicWaker::new(),
    });
    shared.put(Box::new(Value {
        io,
        _work: scope.track()?,
    }));
    let owner = Owner(shared.clone());
    scope.spawn(async move {
        let _owner = owner;
        std::future::pending::<()>().await;
        Ok(())
    })?;
    Ok(shared)
}

struct Scoped {
    shared: Arc<Shared<BoxStream>>,
    scope: Scope,
}

impl Scoped {
    fn with<T>(&mut self, operation: impl FnOnce(Pin<&mut dyn Stream>) -> T) -> Option<T> {
        self.shared.with(|io| operation(io.as_mut()))
    }

    fn unavailable<T>(&self) -> Poll<io::Result<T>> {
        if self.shared.is_closed() {
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
                if self.shared.is_closed() {
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
    fn poll_direct(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<bool>> {
        self.shared.reader.register(cx.waker());
        self.with(|stream| stream.poll_direct(cx))
            .unwrap_or_else(|| self.unavailable())
    }
    fn poll_read_chunk(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ChunkBuffer,
    ) -> Poll<io::Result<Bytes>> {
        self.shared.reader.register(cx.waker());
        self.with(|stream| stream.poll_read_chunk(cx, buffer))
            .unwrap_or_else(|| {
                if self.shared.is_closed() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    struct Resource(Arc<AtomicBool>);
    impl Drop for Resource {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_waits_for_in_flight_io_and_prevents_republication() {
        let scope = Scope::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let shared = installed(&scope, Resource(dropped.clone())).unwrap();
        let io = shared.clone();
        let (entered, executing) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            io.with(|_| {
                entered.send(()).unwrap();
                released.recv().unwrap();
            })
        });
        executing.await.unwrap();
        scope.close();
        std::future::poll_fn(|cx| {
            shared.reader.register(cx.waker());
            if shared.is_closed() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        tokio::select! {
            biased;
            _ = scope.wait() => panic!("scope finished while I/O retained its resource"),
            _ = std::future::ready(()) => {},
        }
        assert!(!dropped.load(Ordering::Acquire));
        release.send(()).unwrap();
        assert_eq!(thread.join().unwrap(), Some(()));
        scope.wait().await;
        assert!(dropped.load(Ordering::Acquire));
        assert!(shared.is_closed());
        assert!(shared.with(|_| ()).is_none());
    }
}
