//! Established sockets execute on the flow's worker after daemon routing.
use super::*;
use kotoconn_protocol::{BoxStream, ChunkBuffer};

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
    upload: Bytes,
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
            upload: Bytes::new(),
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
        for _ in 0..32 {
            if self.upload.is_empty() {
                if let Some(bytes) = shared.incoming.pop() {
                    // Admission may have published data before the handoff arrived.
                    self.upload = bytes;
                } else if socket.can_recv() {
                    self.upload = socket
                        .recv_buffer(|storage| {
                            let bytes = storage.take().unwrap_or_default();
                            let count = bytes.len();
                            shared.rx_used.fetch_add(count, Ordering::Relaxed);
                            (count, bytes)
                        })
                        .map_err(io::Error::other)?;
                }
            }
            if self.upload.is_empty() {
                if !socket.may_recv() {
                    ready!(self.attachment.peer.as_mut().poll_shutdown(cx))?;
                    self.upload_done = true;
                    return Poll::Ready(Ok(()));
                }
                ready!(self.attachment.peer.as_mut().poll_flush(cx))?;
                return Poll::Pending;
            }
            let before = self.upload.len();
            let count = ready!(
                self.attachment
                    .peer
                    .as_mut()
                    .poll_write_chunk(cx, &mut self.upload)
            )?;
            debug_assert_eq!(before - self.upload.len(), count);
            if count == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            let old = shared.rx_used.fetch_sub(count, Ordering::Relaxed);
            debug_assert!(old >= count);
            shared.rx_consumed.fetch_add(count, Ordering::Relaxed);
            self.sent += count as u64;
        }
        shared.wake();
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
