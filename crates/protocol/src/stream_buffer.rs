//! A duplex byte stream with independently adapting, chunked directions.
use crate::queue::{Capacity, INITIAL_BYTES};
use futures_util::task::AtomicWaker;
use std::{
    collections::VecDeque,
    io,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, ready},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    task::coop,
    time::Instant,
};

const CHUNK: usize = 16 * 1024;

struct State {
    chunks: VecDeque<Vec<u8>>,
    offset: usize,
    bytes: usize,
    capacity: Capacity,
    writer_closed: bool,
    reader_closed: bool,
}

struct Pipe {
    state: Mutex<State>,
    reader: AtomicWaker,
    writer: AtomicWaker,
}

impl Pipe {
    fn new(initial: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                chunks: VecDeque::new(),
                offset: 0,
                bytes: 0,
                capacity: Capacity::new(initial),
                writer_closed: false,
                reader_closed: false,
            }),
            reader: AtomicWaker::new(),
            writer: AtomicWaker::new(),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // SAFETY: Only byte copies and checked queue operations run under this
        // lock. Poisoning means accepted stream data can no longer be trusted.
        self.state.lock().expect("stream buffer poisoned")
    }
}

pub struct BufferedStream {
    read: Arc<Pipe>,
    write: Arc<Pipe>,
}

pub fn duplex() -> (BufferedStream, BufferedStream) {
    duplex_with_capacity(INITIAL_BYTES)
}

pub fn duplex_with_capacity(initial: usize) -> (BufferedStream, BufferedStream) {
    // SAFETY: A zero-capacity reliable queue could never make progress.
    assert!(initial > 0, "stream buffer needs positive initial capacity");
    let read = Pipe::new(initial);
    let write = Pipe::new(initial);
    (
        BufferedStream {
            read: read.clone(),
            write: write.clone(),
        },
        BufferedStream {
            read: write,
            write: read,
        },
    )
}

impl AsyncRead for BufferedStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let coop = ready!(coop::poll_proceed(cx));
        let result = self.read_inner(cx, out);
        if result.is_ready() {
            coop.made_progress();
        }
        result
    }
}

impl BufferedStream {
    fn read_inner(&self, cx: &mut Context<'_>, out: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        self.read.reader.register(cx.waker());
        let mut state = self.read.lock();
        let before = out.filled().len();
        while out.remaining() > 0 {
            let Some(chunk) = state.chunks.front() else {
                break;
            };
            let length = chunk.len();
            let count = out.remaining().min(length - state.offset);
            out.put_slice(&chunk[state.offset..state.offset + count]);
            state.offset += count;
            state.bytes -= count;
            if state.offset == length {
                state.chunks.pop_front();
                state.offset = 0;
            }
        }
        let copied = out.filled().len() - before;
        if copied > 0 {
            state.capacity.complete(copied, Instant::now());
            if state.chunks.is_empty() {
                state.chunks = VecDeque::new();
            }
            drop(state);
            self.read.writer.wake();
            Poll::Ready(Ok(()))
        } else if state.writer_closed {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

impl AsyncWrite for BufferedStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let coop = ready!(coop::poll_proceed(cx));
        let result = self.write_inner(cx, bytes);
        if result.is_ready() {
            coop.made_progress();
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Writes commit to the peer's receive queue, like Tokio's duplex stream.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.write.lock().writer_closed = true;
        self.write.reader.wake();
        Poll::Ready(Ok(()))
    }
}

impl BufferedStream {
    fn write_inner(&self, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        self.write.writer.register(cx.waker());
        let mut state = self.write.lock();
        if state.reader_closed || state.writer_closed {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        let space = state
            .capacity
            .target(Instant::now())
            .saturating_sub(state.bytes);
        if space == 0 {
            return Poll::Pending;
        }
        let count = space.min(bytes.len()).min(CHUNK);
        let room = state
            .chunks
            .back()
            .map_or(0, |chunk| chunk.capacity() - chunk.len());
        let count = if room > 0 {
            let count = count.min(room);
            // SAFETY: room was read from the last chunk under this same lock.
            debug_assert!(!state.chunks.is_empty());
            state
                .chunks
                .back_mut()
                .expect("last stream chunk")
                .extend_from_slice(&bytes[..count]);
            count
        } else {
            let mut chunk = Vec::with_capacity(CHUNK);
            chunk.extend_from_slice(&bytes[..count]);
            state.chunks.push_back(chunk);
            count
        };
        state.bytes += count;
        drop(state);
        self.write.reader.wake();
        Poll::Ready(Ok(count))
    }
}

impl Drop for BufferedStream {
    fn drop(&mut self) {
        self.write.lock().writer_closed = true;
        self.write.reader.wake();
        let mut state = self.read.lock();
        state.reader_closed = true;
        state.chunks = VecDeque::new();
        state.bytes = 0;
        drop(state);
        self.read.writer.wake();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn backpressure_preserves_bytes_and_half_close_preserves_reverse_traffic() {
        let (mut a, mut b) = duplex_with_capacity(32);
        let send = async {
            a.write_all(&vec![7; 100_000]).await.unwrap();
            a.shutdown().await.unwrap();
            let mut reply = Vec::new();
            a.read_to_end(&mut reply).await.unwrap();
            assert_eq!(reply, b"after EOF");
        };
        let receive = async {
            let mut received = Vec::new();
            b.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, vec![7; 100_000]);
            b.write_all(b"after EOF").await.unwrap();
            b.shutdown().await.unwrap();
        };
        tokio::join!(send, receive);
    }
}
