use crate::{
    frame::{self, Command, Frame, FrameReader, Sid},
    padding::{self, PaddingScheme},
    stream::{Closed, LogicalStream, Received, Status},
};
use anyhow::{Result, bail, ensure};
use bytes::{Bytes, BytesMut};
use kotoconn_protocol::{BoxStream, Scope, WorkGuard};
use std::{collections::HashMap, io::IoSlice, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
};
use tokio_util::sync::CancellationToken;

const PROTOCOL_VERSION: u32 = 2;

const MAX_STREAMS: usize = 128;

/// Received bytes queued for a session's streams, before their readers take them.
const RECEIVE_BYTES: usize = 4 * 1024 * 1024;

/// Frames queued for the TLS writer; each holds at most one stream chunk.
const WRITE_QUEUE: usize = 64;

/// One batch of unpadded frames coalesces into a single vectored TLS write.
const WRITE_BATCH: usize = 16;

/// Each write is one counted packet for padding. Streams enqueue their own
/// data, so a stream's frames keep their order relative to its SYN and FIN.
pub(crate) enum Write {
    Push {
        sid: Sid,
        data: Bytes,
    },
    Control(Bytes),
    /// Completes after every earlier write reached the transport.
    Flushed(oneshot::Sender<()>),
}

pub(crate) struct Open {
    address: Bytes,
    work: WorkGuard,
    complete: oneshot::Sender<Result<LogicalStream>>,
}

pub(crate) struct Handle {
    sender: mpsc::Sender<Open>,
    pub closed: CancellationToken,
    scope: Scope,
}

impl Handle {
    pub async fn open(&self, address: Bytes, work: WorkGuard) -> Result<BoxStream> {
        // Until the server acknowledges or rejects the stream, a cancelled open,
        // including a handshake timeout, retires this reserved session: one that
        // stopped answering SYN must not return to the pool.
        struct Opening(Option<Scope>);

        impl Drop for Opening {
            fn drop(&mut self) {
                if let Some(scope) = &self.0 {
                    scope.close();
                }
            }
        }

        let mut opening = Opening(Some(self.scope.clone()));
        let (complete, receive) = oneshot::channel();
        self.sender
            .send(Open {
                address,
                work,
                complete,
            })
            .await
            .map_err(|_| anyhow::anyhow!("AnyTLS session closed"))?;

        let mut stream = receive.await??;
        let handshake = stream.wait_handshake().await;
        opening.0 = None;
        handshake?;
        Ok(Box::pin(stream))
    }
}

struct Entry {
    incoming: mpsc::UnboundedSender<Received>,
    status: Arc<Status>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.status.close(None);
    }
}

pub(crate) type Incoming = Box<dyn Fn(BoxStream) -> Result<()> + Send + Sync>;

pub(crate) type Idle = Box<dyn Fn() + Send + Sync>;

enum Role {
    Client {
        /// Shared with later sessions of the same client, as the protocol requires.
        padding: watch::Sender<PaddingScheme>,
        idle: Idle,
        started: bool,
        opens: mpsc::Receiver<Open>,
    },
    Server {
        padding: PaddingScheme,
        incoming: Incoming,
        settings: bool,
        stopping: CancellationToken,
    },
}

/// A PSH that arrived while receive credit was exhausted. The protocol has no
/// per-stream flow control, so like the reference session, reading pauses
/// until a consumer frees credit; only that pause applies backpressure to the
/// peer. Local opens and closes keep running meanwhile.
struct Blocked {
    sid: Sid,
    data: Bytes,
}

impl Blocked {
    /// Message overhead counts too, so tiny PSH frames cannot turn the byte
    /// budget into an unbounded message count.
    fn cost(&self) -> u32 {
        let cost = self.data.len() + std::mem::size_of::<Received>();
        debug_assert!(cost <= RECEIVE_BYTES);
        // SAFETY: A frame payload is at most 65535 bytes, far below u32::MAX.
        u32::try_from(cost).expect("receive cost fits u32")
    }
}

pub(crate) struct Session {
    role: Role,
    entries: HashMap<Sid, Entry>,
    receive_credit: Arc<Semaphore>,
    blocked: Option<Blocked>,
    last_sid: u32,
    closed: mpsc::UnboundedReceiver<Closed>,
    close_sender: mpsc::UnboundedSender<Closed>,
    writes: mpsc::Sender<Write>,
}

impl Session {
    pub fn client<T>(
        io: T,
        scope: &Scope,
        padding: watch::Sender<PaddingScheme>,
        idle: Idle,
    ) -> Result<Arc<Handle>>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (sender, opens) = mpsc::channel(1);
        let shaping = padding.subscribe();
        let role = Role::Client {
            padding,
            idle,
            started: false,
            opens,
        };
        let closed = Self::start(io, scope, role, Some(shaping))?;
        Ok(Arc::new(Handle {
            sender,
            closed,
            scope: scope.clone(),
        }))
    }

    pub fn server<T>(
        io: T,
        scope: &Scope,
        padding: PaddingScheme,
        incoming: Incoming,
        stopping: CancellationToken,
    ) -> Result<CancellationToken>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let role = Role::Server {
            padding,
            incoming,
            settings: false,
            stopping,
        };
        Self::start(io, scope, role, None)
    }

    /// Only clients shape their writes; the reference server sends unpadded.
    fn start<T>(
        io: T,
        scope: &Scope,
        role: Role,
        shaping: Option<watch::Receiver<PaddingScheme>>,
    ) -> Result<CancellationToken>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (close_sender, closed) = mpsc::unbounded_channel();
        let (writes, pending) = mpsc::channel(WRITE_QUEUE);
        let mut session = Self {
            role,
            entries: HashMap::new(),
            receive_credit: Arc::new(Semaphore::new(RECEIVE_BYTES)),
            blocked: None,
            last_sid: 0,
            closed,
            close_sender,
            writes,
        };

        let (reader, writer) = tokio::io::split(io);
        let done = CancellationToken::new();
        let guard = done.clone().drop_guard();
        scope.spawn(async move {
            let _guard = guard;
            let writing = write(writer, pending, shaping);
            tokio::pin!(writing);
            tokio::select! {
                result = session.run(FrameReader::new(reader)) => result?,
                result = &mut writing => return result,
            }
            // A graceful end still sends every frame queued before it.
            tokio::select! {
                result = session.flushed() => result,
                result = &mut writing => result,
            }
        })?;
        Ok(done)
    }

    /// Ends gracefully once no stream remains after a server drain starts or a
    /// client's pool releases the session.
    async fn run<R: AsyncRead + Unpin>(&mut self, mut frames: FrameReader<R>) -> Result<()> {
        let stopping = match &self.role {
            Role::Server { stopping, .. } => stopping.clone(),
            Role::Client { .. } => CancellationToken::new(),
        };
        let mut ending = false;
        loop {
            if ending && self.entries.is_empty() {
                return Ok(());
            }

            tokio::select! {
                _ = stopping.cancelled(), if !ending => ending = true,
                frame = frames.next(), if self.blocked.is_none() => {
                    let frame = frame?.ok_or_else(|| anyhow::anyhow!("AnyTLS peer disconnected"))?;
                    self.received(frame).await?;
                }
                credit = blocked_credit(&self.receive_credit, &self.blocked) => self.deliver(credit),
                open = next_open(&mut self.role), if !ending => match open {
                    Some(open) => self.open(open).await?,
                    None => ending = true,
                },
                Some(closed) = self.closed.recv() => {
                    self.close(closed.sid, None, closed.complete).await?;
                }
            }
        }
    }

    fn stream(&mut self, sid: Sid, work: Option<WorkGuard>) -> LogicalStream {
        debug_assert!(!self.entries.contains_key(&sid));
        let (incoming, receive) = mpsc::unbounded_channel();
        let status = Arc::new(Status::default());
        let stream = LogicalStream::new(
            sid,
            receive,
            self.writes.clone(),
            self.close_sender.clone(),
            status.clone(),
            work,
        );
        self.entries.insert(sid, Entry { incoming, status });
        stream
    }

    /// Session frames wait for writer capacity like stream data, so while the
    /// peer does not read, this session stops reading as well, as the
    /// reference receive loop does. Clients open one stream per session at a
    /// time, so in practice this couples only the two directions of a stream.
    async fn send(&self, frames: &[Frame]) -> Result<()> {
        let mut bytes = BytesMut::new();
        for frame in frames {
            frame.encode(&mut bytes);
        }
        self.writes
            .send(Write::Control(bytes.freeze()))
            .await
            .map_err(|_| anyhow::anyhow!("AnyTLS writer closed"))
    }

    async fn alert(&self, message: &'static str) -> Result<()> {
        self.send(&[Frame::Alert(Bytes::from_static(message.as_bytes()))])
            .await?;
        // The writer ends with the session; a lost alert must not hide the cause.
        let _ = self.flushed().await;
        bail!("AnyTLS peer rejected: {message}");
    }

    async fn open(&mut self, open: Open) -> Result<()> {
        ensure!(self.entries.is_empty(), "AnyTLS session is busy");
        let Role::Client {
            padding, started, ..
        } = &mut self.role
        else {
            // SAFETY: Only client roles own an open channel; next_open never
            // yields for a server.
            unreachable!("servers do not open streams");
        };

        let mut frames = Vec::with_capacity(3);
        if !*started {
            let settings = format!(
                "v={PROTOCOL_VERSION}\nclient=kotoconn/{}\npadding-md5={}",
                env!("CARGO_PKG_VERSION"),
                padding.borrow().md5()
            );
            frames.push(Frame::Settings(settings.into()));
            *started = true;
        }
        self.last_sid = self
            .last_sid
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("AnyTLS stream IDs exhausted"))?;
        // SAFETY: checked_add above succeeded from a value of at least zero.
        let sid = Sid::new(self.last_sid).expect("nonzero stream ID");
        frames.push(Frame::Syn(sid));
        frames.push(Frame::Push(sid, open.address));

        // Settings, SYN and the target address form the first counted packet.
        self.send(&frames).await?;
        let stream = self.stream(sid, Some(open.work));
        // A cancelled opener drops its stream, queuing normal cleanup.
        let _ = open.complete.send(Ok(stream));
        Ok(())
    }

    fn idle(&self) {
        if let Role::Client { idle, .. } = &self.role
            && self.entries.is_empty()
        {
            idle();
        }
    }

    /// `flushed` completes once the FIN and every frame queued before it
    /// reached the transport, without holding up this session's other streams.
    async fn close(
        &mut self,
        sid: Sid,
        error: Option<String>,
        flushed: Option<oneshot::Sender<()>>,
    ) -> Result<()> {
        if self
            .blocked
            .as_ref()
            .is_some_and(|blocked| blocked.sid == sid)
        {
            self.blocked = None;
        }
        let Some(entry) = self.entries.remove(&sid) else {
            // The peer closed it first; nothing of this stream remains to send.
            if let Some(flushed) = flushed {
                let _ = flushed.send(());
            }
            return Ok(());
        };
        entry.status.close(error);
        drop(entry);
        self.send(&[Frame::Fin(sid)]).await?;
        if let Some(flushed) = flushed {
            self.writes
                .send(Write::Flushed(flushed))
                .await
                .map_err(|_| anyhow::anyhow!("AnyTLS writer closed"))?;
        }
        self.idle();
        Ok(())
    }

    async fn flushed(&self) -> Result<()> {
        let (flushed, complete) = oneshot::channel();
        let closed = || anyhow::anyhow!("AnyTLS writer closed");
        self.writes
            .send(Write::Flushed(flushed))
            .await
            .map_err(|_| closed())?;
        complete.await.map_err(|_| closed())
    }

    fn push(&mut self, sid: Sid, data: Bytes) {
        if data.is_empty() || !self.entries.contains_key(&sid) {
            return;
        }
        let blocked = Blocked { sid, data };
        match self
            .receive_credit
            .clone()
            .try_acquire_many_owned(blocked.cost())
        {
            Ok(credit) => self.queue(blocked, credit),
            Err(_) => self.blocked = Some(blocked),
        }
    }

    fn deliver(&mut self, credit: OwnedSemaphorePermit) {
        // SAFETY: Credit is acquired only while a frame is blocked, and only
        // the run loop, which polls that acquisition, clears it.
        let blocked = self.blocked.take().expect("blocked frame");
        self.queue(blocked, credit);
    }

    fn queue(&self, Blocked { sid, data }: Blocked, credit: OwnedSemaphorePermit) {
        // The stream may have closed while its frame waited for credit.
        if let Some(entry) = self.entries.get(&sid) {
            let _ = entry.incoming.send(Received {
                bytes: data,
                credit,
            });
        }
    }

    async fn received(&mut self, frame: Frame) -> Result<()> {
        match frame {
            Frame::Push(sid, data) => self.push(sid, data),
            Frame::Fin(sid) => {
                // FIN closes both directions. Retain already queued bytes for
                // the reader, suppress writes, and never echo the peer's FIN.
                if self.entries.remove(&sid).is_some() {
                    self.idle();
                }
            }
            Frame::Alert(message) => {
                bail!("AnyTLS alert: {}", String::from_utf8_lossy(&message));
            }
            Frame::HeartRequest => self.send(&[Frame::HeartResponse]).await?,
            Frame::Waste | Frame::HeartResponse | Frame::Unknown(_) => {}
            frame => match self.role {
                Role::Client { .. } => self.client_received(frame).await?,
                Role::Server { .. } => self.server_received(frame).await?,
            },
        }
        Ok(())
    }

    async fn client_received(&mut self, frame: Frame) -> Result<()> {
        let Role::Client { padding, .. } = &self.role else {
            // SAFETY: received dispatches here only for a client role, which
            // never changes during a session.
            unreachable!("client frame on a server session");
        };
        match frame {
            Frame::SynAck(sid, error) if error.is_empty() => {
                if let Some(entry) = self.entries.get(&sid) {
                    entry.status.accepted.cancel();
                }
            }
            Frame::SynAck(sid, error) => {
                let error = String::from_utf8_lossy(&error).into_owned();
                self.close(sid, Some(error), None).await?;
            }
            Frame::UpdatePaddingScheme(raw) => match PaddingScheme::parse(&raw) {
                // Later records of this session and every later session use it.
                Ok(scheme) => {
                    padding.send_replace(scheme);
                }
                Err(error) => tracing::warn!(%error, "ignoring invalid AnyTLS padding update"),
            },
            // Version 2 is required of servers, so their settings change nothing.
            Frame::ServerSettings(_) => {}
            frame => tracing::debug!(?frame, "ignoring AnyTLS server-bound frame"),
        }
        Ok(())
    }

    async fn server_received(&mut self, frame: Frame) -> Result<()> {
        let Role::Server {
            padding, settings, ..
        } = &self.role
        else {
            // SAFETY: received dispatches here only for a server role, which
            // never changes during a session.
            unreachable!("server frame on a client session");
        };
        let settled = *settings;
        match frame {
            Frame::Settings(_) if settled => self.alert("duplicate client settings").await,
            Frame::Settings(payload) => {
                let version = frame::setting(&payload, "v").and_then(|v| v.parse::<u32>().ok());
                if version.is_none_or(|version| version < PROTOCOL_VERSION) {
                    return self.alert("client protocol version 2 is required").await;
                }

                let mut replies = Vec::with_capacity(2);
                if frame::setting(&payload, "padding-md5") != Some(padding.md5()) {
                    replies.push(Frame::UpdatePaddingScheme(padding.raw().clone()));
                }
                replies.push(Frame::ServerSettings(Bytes::from_static(b"v=2")));
                if let Role::Server { settings, .. } = &mut self.role {
                    *settings = true;
                }
                self.send(&replies).await
            }
            Frame::Syn(_) if !settled => self.alert("client did not send its settings").await,
            Frame::Syn(sid) => {
                ensure!(sid.get() > self.last_sid, "AnyTLS stream IDs must increase");
                self.last_sid = sid.get();
                if self.entries.len() >= MAX_STREAMS {
                    let error = Bytes::from_static(b"stream limit reached");
                    return self
                        .send(&[Frame::SynAck(sid, error), Frame::Fin(sid)])
                        .await;
                }

                // Acknowledge on admission; routing failures close the stream.
                let stream = self.stream(sid, None);
                self.send(&[Frame::SynAck(sid, Bytes::new())]).await?;
                let Role::Server { incoming, .. } = &self.role else {
                    // SAFETY: This function starts by matching a server role,
                    // which never changes during a session.
                    unreachable!("server role changed");
                };
                incoming(Box::pin(stream))
            }
            frame => {
                tracing::debug!(?frame, "ignoring AnyTLS client-bound frame");
                Ok(())
            }
        }
    }
}

/// Pending unless a frame waits for credit.
async fn blocked_credit(
    semaphore: &Arc<Semaphore>,
    blocked: &Option<Blocked>,
) -> OwnedSemaphorePermit {
    let Some(blocked) = blocked else {
        return std::future::pending().await;
    };
    // SAFETY: The session never closes its own semaphore.
    semaphore
        .clone()
        .acquire_many_owned(blocked.cost())
        .await
        .expect("receive credit semaphore closed")
}

async fn next_open(role: &mut Role) -> Option<Open> {
    match role {
        Role::Client { opens, .. } => opens.recv().await,
        Role::Server { .. } => std::future::pending().await,
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        for entry in self.entries.values() {
            entry.status.close(Some("AnyTLS session closed".into()));
        }
    }
}

/// Like the reference, shaping ends for good at the first packet past `stop`,
/// even if a later padding update raises it.
async fn write<W: AsyncWrite + Unpin>(
    mut writer: W,
    mut pending: mpsc::Receiver<Write>,
    mut shaping: Option<watch::Receiver<PaddingScheme>>,
) -> Result<()> {
    let mut packet = 1u32;
    let mut batch = Vec::with_capacity(WRITE_BATCH);
    loop {
        if pending.recv_many(&mut batch, WRITE_BATCH).await == 0 {
            return Ok(());
        }

        let scheme = shaping.as_ref().map(|shaping| shaping.borrow().clone());
        let scheme = scheme.filter(|scheme| scheme.applies(packet));
        if scheme.is_none() {
            shaping = None;
        }
        let mut flushed = Vec::new();
        tokio::time::timeout(crate::WRITE_TIMEOUT, async {
            match &scheme {
                Some(scheme) => {
                    write_shaped(&mut writer, &mut batch, &mut flushed, scheme, &mut packet).await?
                }
                None => write_batch(&mut writer, &mut batch, &mut flushed).await?,
            }
            writer.flush().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        for complete in flushed {
            let _ = complete.send(());
        }
    }
}

/// Shaped packets are written one at a time, as the reference counts them.
async fn write_shaped<W: AsyncWrite + Unpin>(
    writer: &mut W,
    batch: &mut Vec<Write>,
    flushed: &mut Vec<oneshot::Sender<()>>,
    scheme: &PaddingScheme,
    packet: &mut u32,
) -> Result<()> {
    for write in batch.drain(..) {
        if let Write::Flushed(complete) = write {
            flushed.push(complete);
            continue;
        }
        let payload = encode(&write);
        if scheme.applies(*packet) {
            padding::write_packet(writer, &payload, scheme, *packet).await?;
        } else {
            writer.write_all(&payload).await?;
        }
        *packet = packet.saturating_add(1);
    }
    Ok(())
}

fn encode(write: &Write) -> Bytes {
    match write {
        Write::Push { sid, data } => {
            let mut bytes = BytesMut::with_capacity(frame::HEADER + data.len());
            bytes.extend_from_slice(&frame::header(Command::Push, sid.get(), data.len()));
            bytes.extend_from_slice(data);
            bytes.freeze()
        }
        Write::Control(bytes) => bytes.clone(),
        // SAFETY: Callers separate flush barriers before encoding.
        Write::Flushed(_) => unreachable!("flush barriers carry no frame"),
    }
}

/// TLS encrypts the vectored slices into full records without an extra copy.
async fn write_batch<W: AsyncWrite + Unpin>(
    writer: &mut W,
    batch: &mut Vec<Write>,
    flushed: &mut Vec<oneshot::Sender<()>>,
) -> Result<()> {
    let mut headers = Vec::with_capacity(batch.len());
    for write in batch.iter() {
        if let Write::Push { sid, data } = write {
            headers.push(frame::header(Command::Push, sid.get(), data.len()));
        }
    }

    let mut slices = Vec::with_capacity(batch.len() * 2);
    let mut headers = headers.iter();
    for write in batch.iter() {
        match write {
            Write::Push { data, .. } => {
                // SAFETY: The loop above stored one header per Push, in order.
                slices.push(IoSlice::new(headers.next().expect("PSH header")));
                slices.push(IoSlice::new(data));
            }
            Write::Control(bytes) => slices.push(IoSlice::new(bytes)),
            Write::Flushed(_) => {}
        }
    }

    let mut remaining = &mut slices[..];
    while !remaining.is_empty() {
        let written = writer.write_vectored(remaining).await?;
        ensure!(written > 0, "AnyTLS transport closed");
        IoSlice::advance_slices(&mut remaining, written);
    }
    drop(slices);

    for write in batch.drain(..) {
        if let Write::Flushed(complete) = write {
            flushed.push(complete);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, DuplexStream, ReadHalf, WriteHalf};

    const LIMIT: Duration = Duration::from_secs(10);

    struct Peer {
        done: CancellationToken,
        accepted: mpsc::UnboundedReceiver<BoxStream>,
        frames: FrameReader<ReadHalf<DuplexStream>>,
        writer: WriteHalf<DuplexStream>,
    }

    /// A server session on a 64-byte pipe whose peer sent settings and opened sid 1.
    async fn server_stream(stopping: CancellationToken) -> Result<(BoxStream, Peer)> {
        let (transport, peer) = tokio::io::duplex(64);
        let (admitted, mut accepted) = mpsc::unbounded_channel();
        let done = Session::server(
            transport,
            &Scope::new(),
            PaddingScheme::default(),
            Box::new(move |stream| {
                admitted
                    .send(stream)
                    .map_err(|_| anyhow::anyhow!("admission closed"))
            }),
            stopping,
        )?;
        let (reader, mut writer) = tokio::io::split(peer);
        let mut opening = BytesMut::new();
        let settings = format!("v=2\npadding-md5={}", PaddingScheme::default().md5());
        Frame::Settings(settings.into()).encode(&mut opening);
        Frame::Syn(Sid::new(1).unwrap()).encode(&mut opening);
        writer.write_all(&opening).await?;

        let mut frames = FrameReader::new(reader);
        assert!(matches!(
            frames.next().await?,
            Some(Frame::ServerSettings(_))
        ));
        assert!(matches!(frames.next().await?, Some(Frame::SynAck(_, _))));
        let stream = accepted.recv().await.unwrap();
        let peer = Peer {
            done,
            accepted,
            frames,
            writer,
        };
        Ok((stream, peer))
    }

    #[tokio::test]
    async fn draining_flushes_the_last_stream_before_ending_the_session() -> Result<()> {
        tokio::time::timeout(LIMIT, async {
            let stopping = CancellationToken::new();
            let (mut stream, peer) = server_stream(stopping.clone()).await?;
            let Peer {
                done, mut frames, ..
            } = peer;
            stopping.cancel();
            let payload = vec![b'x'; 4096];
            stream.write_all(&payload).await?;

            let (shutdown, (received, end)) = tokio::join!(stream.shutdown(), async {
                let mut received = Vec::new();
                loop {
                    match frames.next().await {
                        Ok(Some(Frame::Push(_, bytes))) => received.extend_from_slice(&bytes),
                        end => break (received, end),
                    }
                }
            });
            shutdown?;
            assert_eq!(received, payload);
            assert!(matches!(end, Ok(Some(Frame::Fin(_)))), "{end:?}");
            done.cancelled().await;
            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn exhausted_receive_credit_pauses_reading_but_not_local_close() -> Result<()> {
        tokio::time::timeout(LIMIT, async {
            let (mut stream, peer) = server_stream(CancellationToken::new()).await?;
            let Peer {
                mut frames,
                mut writer,
                ..
            } = peer;
            let chunk = Bytes::from(vec![7; u16::MAX as usize]);
            let fitting = RECEIVE_BYTES / (chunk.len() + std::mem::size_of::<Received>());
            let mut push = BytesMut::new();
            Frame::Push(Sid::new(1).unwrap(), chunk).encode(&mut push);
            for _ in 0..=fitting {
                writer.write_all(&push).await?;
            }
            // Filling the 64-byte pipe again requires the session to have read
            // the last PSH, which then waits for credit.
            let mut waste = Vec::from(frame::header(Command::Waste, 0, 64 - frame::HEADER));
            waste.resize(64, 0);
            writer.write_all(&waste).await?;

            stream.shutdown().await?;
            assert!(matches!(frames.next().await?, Some(Frame::Fin(_))));
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await?;
            assert_eq!(received.len(), fitting * u16::MAX as usize);
            assert!(received.iter().all(|&byte| byte == 7));
            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn closing_a_stream_does_not_hold_up_its_siblings() -> Result<()> {
        tokio::time::timeout(LIMIT, async {
            let (mut first, mut peer) = server_stream(CancellationToken::new()).await?;
            let second = Sid::new(2).unwrap();
            peer.writer
                .write_all(&frame::header(Command::Syn, second.get(), 0))
                .await?;
            assert_eq!(
                peer.frames.next().await?,
                Some(Frame::SynAck(second, Bytes::new()))
            );
            let mut sibling = peer.accepted.recv().await.unwrap();

            // The peer stops reading, so this tail and its FIN cannot leave.
            first.write_all(&[1; 4096]).await?;
            drop(first);
            let mut push = BytesMut::new();
            Frame::Push(second, Bytes::from_static(b"sibling")).encode(&mut push);
            peer.writer.write_all(&push).await?;
            let mut received = [0; 7];
            sibling.read_exact(&mut received).await?;
            assert_eq!(&received, b"sibling");
            Ok(())
        })
        .await?
    }

    #[tokio::test(start_paused = true)]
    async fn an_abandoned_synack_wait_retires_the_session() -> Result<()> {
        let (transport, _peer) = tokio::io::duplex(4096);
        let scope = Scope::new();
        let session_scope = scope.child();
        let (padding, _) = watch::channel(PaddingScheme::default());
        let session = Session::client(transport, &session_scope, padding, Box::new(|| {}))?;
        let address = Bytes::from_static(&[1, 127, 0, 0, 1, 0, 80]);
        let opening = session.open(address, scope.track()?);
        assert!(tokio::time::timeout(LIMIT, opening).await.is_err());
        assert!(session_scope.is_closed());
        Ok(())
    }

    /// A server session driven by direct calls instead of its run loop.
    fn unstarted() -> (Session, mpsc::Receiver<Write>) {
        let (writes, pending) = mpsc::channel(WRITE_QUEUE);
        let (close_sender, closed) = mpsc::unbounded_channel();
        let session = Session {
            role: Role::Server {
                padding: PaddingScheme::default(),
                incoming: Box::new(|_| Ok(())),
                settings: true,
                stopping: CancellationToken::new(),
            },
            entries: HashMap::new(),
            receive_credit: Arc::new(Semaphore::new(RECEIVE_BYTES)),
            blocked: None,
            last_sid: 0,
            closed,
            close_sender,
            writes,
        };
        (session, pending)
    }

    #[tokio::test]
    async fn shutdown_fails_when_the_session_ends_before_sending_fin() {
        let (mut session, pending) = unstarted();
        let mut stream = session.stream(Sid::new(1).unwrap(), None);
        let mut shutdown = Box::pin(stream.shutdown());
        assert!(futures_util::poll!(&mut shutdown).is_pending());

        let closed = session.closed.recv().await.unwrap();
        session
            .close(closed.sid, None, closed.complete)
            .await
            .unwrap();
        // The writer ends with the FIN still queued.
        drop(pending);
        assert!(shutdown.await.is_err());
    }

    #[tokio::test]
    async fn peer_fin_wakes_a_writer_waiting_for_queue_space() {
        struct Count(std::sync::atomic::AtomicUsize);
        impl std::task::Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        let (mut session, _pending) = unstarted();
        let sid = Sid::new(1).unwrap();
        let mut stream = session.stream(sid, None);
        for _ in 0..WRITE_QUEUE {
            stream.write_all(b"queued").await.unwrap();
        }
        let reply = Bytes::from_static(b"reply");
        session.received(Frame::Push(sid, reply)).await.unwrap();

        let wakes = Arc::new(Count(Default::default()));
        let waker = std::task::Waker::from(wakes.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        let mut waiting = Box::pin(stream.write_all(b"after the queue filled"));
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        session.received(Frame::Fin(sid)).await.unwrap();
        assert!(wakes.0.load(std::sync::atomic::Ordering::Relaxed) > 0);
        // Dropped like any write after FIN while the reply is unread.
        waiting.await.unwrap();

        let mut received = Vec::new();
        stream.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, b"reply");
        assert!(stream.write_all(b"after the reply").await.is_err());
    }

    #[tokio::test]
    async fn exhausted_receive_credit_delays_data_instead_of_dropping_it() {
        let (mut session, _pending) = unstarted();
        let sid = Sid::new(1).unwrap();
        let mut stream = session.stream(sid, None);
        let chunk = Bytes::from(vec![7; u16::MAX as usize]);
        let fitting = RECEIVE_BYTES / (chunk.len() + std::mem::size_of::<Received>());
        for _ in 0..=fitting {
            session
                .received(Frame::Push(sid, chunk.clone()))
                .await
                .unwrap();
        }
        assert!(session.blocked.is_some());

        let mut received = vec![0; chunk.len()];
        stream.read_exact(&mut received).await.unwrap();
        let credit = blocked_credit(&session.receive_credit, &session.blocked).await;
        session.deliver(credit);

        let mut rest = vec![0; fitting * chunk.len()];
        stream.read_exact(&mut rest).await.unwrap();
        assert!(rest.iter().all(|&byte| byte == 7));
    }

    #[tokio::test]
    async fn relay_forwards_data_received_before_peer_fin_while_still_uploading() -> Result<()> {
        tokio::time::timeout(LIMIT, async {
            let (transport, peer) = tokio::io::duplex(4096);
            let (reader, mut writer) = tokio::io::split(peer);
            let mut frames = FrameReader::new(reader);
            let scope = Scope::new();
            let (padding, _) = watch::channel(PaddingScheme::parse(b"stop=1")?);
            let session = Session::client(transport, &scope, padding, Box::new(|| {}))?;
            let sid = Sid::new(1).unwrap();
            let address = Bytes::from_static(&[1, 127, 0, 0, 1, 0, 80]);
            let (stream, ()) = tokio::try_join!(session.open(address, scope.track()?), async {
                assert!(matches!(frames.next().await?, Some(Frame::Settings(_))));
                assert_eq!(frames.next().await?, Some(Frame::Syn(sid)));
                assert!(matches!(frames.next().await?, Some(Frame::Push(_, _))));
                let mut reply = BytesMut::new();
                Frame::SynAck(sid, Bytes::new()).encode(&mut reply);
                Frame::Push(sid, Bytes::from_static(b"request rejected")).encode(&mut reply);
                Frame::Fin(sid).encode(&mut reply);
                Frame::HeartRequest.encode(&mut reply);
                writer.write_all(&reply).await?;
                // The heartbeat reply shows the session applied FIN before
                // the relay sees more of the request body.
                assert_eq!(frames.next().await?, Some(Frame::HeartResponse));
                Ok(())
            })?;

            let (local, mut app) = tokio::io::duplex(64);
            let mut local: BoxStream = Box::pin(local);
            app.write_all(b"pending request body").await?;
            app.shutdown().await?;
            kotoconn_protocol::relay(&mut local, stream).await?;
            drop(local);
            let mut response = Vec::new();
            app.read_to_end(&mut response).await?;
            assert_eq!(response, b"request rejected");
            Ok(())
        })
        .await?
    }

    #[tokio::test]
    async fn cancelling_an_open_before_publication_releases_its_reserved_session() {
        let scope = Scope::new();
        let (sender, receiver) = mpsc::channel(1);
        let caller = Scope::new();
        let (complete, _) = oneshot::channel();
        sender
            .send(Open {
                address: Bytes::new(),
                work: caller.track().unwrap(),
                complete,
            })
            .await
            .unwrap();
        let handle = Handle {
            sender,
            closed: CancellationToken::new(),
            scope: scope.clone(),
        };
        let mut opening = Box::pin(handle.open(Bytes::new(), caller.track().unwrap()));
        assert!(futures_util::poll!(&mut opening).is_pending());

        drop(opening);
        assert!(scope.is_closed());
        drop(receiver);
        caller.wait().await;
    }
}
