//! Established sockets execute on the flow's worker after daemon routing.
use super::*;
use bytes::Buf;
use kotoconn_protocol::{BoxStream, ChunkBuffer};
use std::{collections::VecDeque, io::IoSlice};

pub(super) struct Attachment {
    pub peer: BoxStream,
    pub done: oneshot::Sender<io::Result<(u64, u64)>>,
}

pub(super) struct Cancellation {
    pub shared: Arc<Shared>,
    pub armed: bool,
}

impl Cancellation {
    pub fn complete(mut self) {
        self.armed = false;
    }
}

impl Drop for Cancellation {
    fn drop(&mut self) {
        if self.armed {
            self.shared.abort.store(true, Ordering::Release);
            self.shared.wake();
        }
    }
}

pub(super) struct Direct {
    pub attachment: Attachment,
    buffer: ChunkBuffer,
    upload: VecDeque<Bytes>,
    download: Bytes,
    peer_eof: bool,
    upload_done: bool,
    sent: u64,
    received: u64,
}

impl Direct {
    pub fn new(attachment: Attachment) -> Self {
        Self {
            attachment,
            buffer: ChunkBuffer::default(),
            upload: VecDeque::new(),
            download: Bytes::new(),
            peer_eof: false,
            upload_done: false,
            sent: 0,
            received: 0,
        }
    }

    pub fn poll(
        &mut self,
        socket: &mut tcp::Socket<'static, Storage>,
        shared: &Shared,
        cx: &mut Context<'_>,
    ) -> io::Result<bool> {
        let upload = self.upload(socket, shared, cx)?;
        let download = self.download(socket, shared, cx)?;
        Ok(upload.is_ready() && download.is_ready())
    }

    pub fn finish(self, result: io::Result<()>) {
        let _ = self
            .attachment
            .done
            .send(result.map(|()| (self.sent, self.received)));
    }

    fn upload(
        &mut self,
        socket: &mut tcp::Socket<'static, Storage>,
        shared: &Shared,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        if self.upload_done {
            return Poll::Ready(Ok(()));
        }
        // Preserve data published before handoff, then append the socket's
        // contiguous receive prefix. One turn writes at most 32 blocks.
        while self.upload.len() < 32 {
            let Some(bytes) = shared.incoming.pop() else {
                break;
            };
            self.upload.push_back(bytes);
        }
        if self.upload.len() < 32 && socket.can_recv() {
            socket
                .recv_buffer(|storage| {
                    let mut count = 0;
                    while self.upload.len() < 32 {
                        let Some(bytes) = storage.take() else {
                            break;
                        };
                        count += bytes.len();
                        self.upload.push_back(bytes);
                    }
                    shared.rx_used.fetch_add(count, Ordering::Relaxed);
                    (count, ())
                })
                .map_err(io::Error::other)?;
        }

        if !self.upload.is_empty() {
            let mut vectors = [IoSlice::new(&[]); 32];
            for (vector, bytes) in vectors.iter_mut().zip(&self.upload) {
                debug_assert!(!bytes.is_empty());
                *vector = IoSlice::new(bytes);
            }
            let count = ready!(
                self.attachment
                    .peer
                    .as_mut()
                    .poll_write_vectored(cx, &vectors[..self.upload.len()],)
            )?;
            if count == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            debug_assert!(count <= self.upload.iter().map(Bytes::len).sum::<usize>());

            let mut remaining = count;
            while remaining > 0 {
                // SAFETY: AsyncWrite accepted a prefix of the offered vectors.
                // The queue retains that prefix until this accounting completes.
                let front = self.upload.front_mut().expect("accepted upload block");
                let consumed = remaining.min(front.len());
                front.advance(consumed);
                remaining -= consumed;
                if front.is_empty() {
                    self.upload.pop_front();
                }
            }
            let old = shared.rx_used.fetch_sub(count, Ordering::Relaxed);
            debug_assert!(old >= count);
            shared.rx_consumed.fetch_add(count, Ordering::Relaxed);
            self.sent += count as u64;
        }

        if !self.upload.is_empty() || socket.can_recv() || !shared.incoming.is_empty() {
            shared.wake();
        } else if !socket.may_recv() {
            ready!(self.attachment.peer.as_mut().poll_shutdown(cx))?;
            self.upload_done = true;
            return Poll::Ready(Ok(()));
        } else {
            ready!(self.attachment.peer.as_mut().poll_flush(cx))?;
        }
        Poll::Pending
    }

    fn download(
        &mut self,
        socket: &mut tcp::Socket<'static, Storage>,
        shared: &Shared,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        for _ in 0..32 {
            if self.peer_eof && self.download.is_empty() {
                return Poll::Ready(Ok(()));
            }
            let available = shared
                .tx_target
                .load(Ordering::Relaxed)
                .saturating_sub(shared.tx_used.load(Ordering::Relaxed));
            if available == 0 {
                return Poll::Pending;
            }
            if self.download.is_empty() {
                self.download = ready!(
                    self.attachment
                        .peer
                        .as_mut()
                        .poll_read_chunk(cx, &mut self.buffer)
                )?;
                if self.download.is_empty() {
                    self.peer_eof = true;
                    socket.close();
                    return Poll::Ready(Ok(()));
                }
            }
            let count = available.min(self.download.len());
            let bytes = self.download.split_to(count);
            shared.tx_used.fetch_add(count, Ordering::Relaxed);
            socket
                .send_buffer(|storage| (storage.push(bytes), ()))
                .map_err(io::Error::other)?;
            self.attachment.peer.as_mut().consume_chunk(count);
            self.received += count as u64;
        }
        shared.wake();
        Poll::Pending
    }
}
