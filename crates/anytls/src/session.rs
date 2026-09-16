use crate::{
    stream::{LogicalStream, Received, Status},
    wire,
};
use anyhow::{Result, bail, ensure};
use anytls::core::{Command, Engine, Frame, PaddingFactory, ProtocolAction, State};
use bytes::Bytes;
use futures_util::StreamExt;
use kotoconn_protocol::{BoxStream, Scope};
use std::{collections::HashMap, io, sync::Arc};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{Semaphore, mpsc, oneshot, watch},
};
use tokio_util::{codec::FramedRead, sync::CancellationToken};

const MAX_STREAMS: usize = 128;
const RECEIVE_BYTES: usize = 4 * 1024 * 1024;

pub(crate) enum Message {
    Open {
        address: Bytes,
        complete: oneshot::Sender<Result<LogicalStream>>,
    },
    Data {
        sid: u32,
        bytes: Bytes,
    },
    Flush {
        sid: u32,
        complete: oneshot::Sender<io::Result<()>>,
    },
    Close {
        sid: u32,
        complete: oneshot::Sender<io::Result<()>>,
    },
}

pub(crate) struct Handle {
    pub sender: mpsc::Sender<Message>,
    pub closed: CancellationToken,
}

impl Handle {
    pub async fn open(&self, address: Bytes) -> Result<BoxStream> {
        let (complete, receive) = oneshot::channel();
        self.sender
            .send(Message::Open { address, complete })
            .await
            .map_err(|_| anyhow::anyhow!("AnyTLS session closed"))?;
        let mut stream = receive.await??;
        stream.wait_handshake().await?;
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

struct Write {
    bytes: Bytes,
    complete: Option<oneshot::Sender<io::Result<()>>>,
}

type Incoming = Box<dyn Fn(BoxStream) -> Result<()> + Send + Sync>;
type Idle = Box<dyn Fn() + Send + Sync>;

pub(crate) struct Session {
    client: bool,
    state: Arc<State>,
    padding: watch::Sender<PaddingFactory>,
    entries: HashMap<u32, Entry>,
    receive_credit: Arc<Semaphore>,
    next_sid: u32,
    started: bool,
    messages: mpsc::Receiver<Message>,
    sender: mpsc::Sender<Message>,
    dropped: mpsc::UnboundedReceiver<u32>,
    drop_sender: mpsc::UnboundedSender<u32>,
    writes: mpsc::Sender<Write>,
    incoming: Option<Incoming>,
    idle: Option<Idle>,
}

impl Session {
    pub fn start<T>(
        io: T,
        scope: &Scope,
        padding: watch::Sender<PaddingFactory>,
        incoming: Option<Incoming>,
        idle: Option<Idle>,
    ) -> Result<Arc<Handle>>
    where
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let client = incoming.is_none();
        let state = State::new(padding.borrow().clone());
        let (sender, messages) = mpsc::channel(64);
        let (drop_sender, dropped) = mpsc::unbounded_channel();
        let (writes, mut pending) = mpsc::channel::<Write>(16);
        let closed = CancellationToken::new();
        let handle = Arc::new(Handle {
            sender: sender.clone(),
            closed: closed.clone(),
        });
        let mut session = Self {
            client,
            state: state.clone(),
            padding,
            entries: HashMap::new(),
            receive_credit: Arc::new(Semaphore::new(RECEIVE_BYTES)),
            next_sid: 0,
            started: false,
            messages,
            sender,
            dropped,
            drop_sender,
            writes,
            incoming,
            idle,
        };
        let (reader, mut writer) = tokio::io::split(io);
        let guard = closed.drop_guard();
        scope.spawn(async move {
            let _guard = guard;
            let writing = async move {
                let mut packet = 1u32;
                while let Some(write) = pending.recv().await {
                    if write.bytes.is_empty() {
                        writer.flush().await?;
                        if let Some(complete) = write.complete {
                            let _ = complete.send(Ok(()));
                        }
                        continue;
                    }
                    let padding = client.then(|| state.padding());
                    let result = tokio::time::timeout(
                        crate::WRITE_TIMEOUT,
                        wire::write_records(&mut writer, write.bytes, padding.as_ref(), packet),
                    )
                    .await?;
                    packet = packet.saturating_add(1);
                    if let Some(complete) = write.complete {
                        let _ = complete.send(
                            result
                                .as_ref()
                                .map(|_| ())
                                .map_err(|e| io::Error::other(e.to_string())),
                        );
                    }
                    result?;
                }
                Ok::<(), anyhow::Error>(())
            };
            tokio::select! {
                result = session.run(FramedRead::new(reader, wire::Frames)) => result,
                result = writing => result,
            }
        })?;
        Ok(handle)
    }

    async fn run<R: AsyncRead + Unpin>(
        &mut self,
        mut reader: FramedRead<R, wire::Frames>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                frame = reader.next() => {
                    let frame = frame.ok_or_else(|| anyhow::anyhow!("AnyTLS peer disconnected"))??;
                    self.received(frame).await?;
                }
                Some(message) = self.messages.recv() => self.message(message).await?,
                Some(sid) = self.dropped.recv() => self.close(sid, None).await?,
            }
        }
    }

    fn stream(&mut self, sid: u32) -> LogicalStream {
        debug_assert!(!self.entries.contains_key(&sid));
        let (incoming, receive) = mpsc::unbounded_channel();
        let status = Arc::new(Status::default());
        let stream = LogicalStream::new(
            sid,
            receive,
            self.sender.clone(),
            self.drop_sender.clone(),
            status.clone(),
        );
        self.entries.insert(sid, Entry { incoming, status });
        stream
    }

    async fn send(
        &self,
        frames: &[Frame],
        complete: Option<oneshot::Sender<io::Result<()>>>,
    ) -> Result<()> {
        let mut bytes = Vec::new();
        for frame in frames {
            bytes.extend_from_slice(&frame.to_bytes()?);
        }
        self.writes
            .send(Write {
                bytes: bytes.into(),
                complete,
            })
            .await
            .map_err(|_| anyhow::anyhow!("AnyTLS writer closed"))
    }

    async fn flush_frames(&self, frames: &[Frame]) -> Result<()> {
        let (send, receive) = oneshot::channel();
        self.send(frames, Some(send)).await?;
        receive.await??;
        Ok(())
    }

    async fn message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Open { address, complete } => {
                ensure!(
                    self.client && self.entries.is_empty(),
                    "AnyTLS session is busy"
                );
                self.next_sid = self
                    .next_sid
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("AnyTLS stream IDs exhausted"))?;
                let sid = self.next_sid;
                let stream = self.stream(sid);
                let mut frames = Vec::new();
                if !self.started {
                    for action in Engine::on_session_start(
                        &self.state,
                        true,
                        concat!("kotoconn/", env!("CARGO_PKG_VERSION")),
                    )? {
                        if let ProtocolAction::SendFrame(frame) = action {
                            frames.push(frame);
                        }
                    }
                    self.started = true;
                }
                frames.push(Frame::new(Command::Syn, sid));
                frames.push(Frame::with_data(Command::Psh, sid, address));
                self.flush_frames(&frames).await?;
                // A cancelled opener drops its stream, queuing normal cleanup.
                let _ = complete.send(Ok(stream));
            }
            Message::Data { sid, bytes } => {
                if self.entries.contains_key(&sid) {
                    self.send(&[Frame::with_data(Command::Psh, sid, bytes)], None)
                        .await?;
                }
            }
            Message::Flush { sid, complete } => {
                if self.entries.contains_key(&sid) {
                    self.send(&[], Some(complete)).await?;
                } else {
                    let _ = complete.send(Err(io::ErrorKind::BrokenPipe.into()));
                }
            }
            Message::Close { sid, complete } => {
                self.close(sid, None).await?;
                let _ = complete.send(Ok(()));
            }
        }
        Ok(())
    }

    fn idle(&self) {
        if self.entries.is_empty()
            && let Some(idle) = &self.idle
        {
            idle();
        }
    }

    async fn close(&mut self, sid: u32, error: Option<String>) -> Result<()> {
        if let Some(entry) = self.entries.remove(&sid) {
            entry.status.close(error);
            drop(entry);
            self.flush_frames(&[Frame::new(Command::Fin, sid)]).await?;
            self.idle();
        }
        Ok(())
    }

    async fn received(&mut self, frame: Frame) -> Result<()> {
        if frame.cmd == Command::Alert {
            bail!("AnyTLS alert: {}", String::from_utf8_lossy(&frame.data));
        }
        if self.client && frame.cmd == Command::UpdatePaddingScheme {
            wire::padding_scheme(Some(std::str::from_utf8(&frame.data)?))?;
        }

        let actions = Engine::on_frame(&self.state, self.client, &frame)?;
        if self.client
            && frame.cmd == Command::SynAck
            && frame.data.is_empty()
            && let Some(entry) = self.entries.get(&frame.sid)
        {
            entry.status.accepted.cancel();
        }
        if self.client && frame.cmd == Command::UpdatePaddingScheme {
            self.padding.send_replace(self.state.padding());
        }

        for action in actions {
            match action {
                ProtocolAction::SendFrame(frame) => self.send(&[frame], None).await?,
                ProtocolAction::SendFrameSync(frame) => self.flush_frames(&[frame]).await?,
                ProtocolAction::PushStreamData { sid, data } => {
                    let full = if let Some(entry) = self.entries.get(&sid) {
                        // Charge descriptor overhead too, so tiny PSH frames cannot
                        // turn the byte budget into an unbounded message count.
                        let cost = (data.len() + std::mem::size_of::<Received>()) as u32;
                        match self.receive_credit.clone().try_acquire_many_owned(cost) {
                            Ok(credit) => entry
                                .incoming
                                .send(Received {
                                    bytes: data,
                                    _credit: credit,
                                })
                                .is_err(),
                            Err(_) => true,
                        }
                    } else {
                        false
                    };
                    if full {
                        self.close(
                            sid,
                            Some("AnyTLS receive queue full or consumer closed".into()),
                        )
                        .await?;
                    }
                }
                ProtocolAction::EnsureIncomingStream { sid } => {
                    ensure!(sid > self.next_sid, "AnyTLS stream IDs must increase");
                    self.next_sid = sid;
                    if self.entries.len() >= MAX_STREAMS {
                        self.send(
                            &[
                                Frame::with_data(
                                    Command::SynAck,
                                    sid,
                                    Bytes::from_static(b"stream limit reached"),
                                ),
                                Frame::new(Command::Fin, sid),
                            ],
                            None,
                        )
                        .await?;
                        continue;
                    }
                    let stream = self.stream(sid);
                    self.send(&[Frame::new(Command::SynAck, sid)], None).await?;
                    if let Some(incoming) = &self.incoming {
                        incoming(Box::pin(stream))?;
                    }
                }
                ProtocolAction::CloseLocalStream { sid } => {
                    // FIN closes both directions. Retain already queued bytes for
                    // the reader, suppress writes, and never echo the peer's FIN.
                    if self.entries.remove(&sid).is_some() {
                        self.idle();
                    }
                }
                ProtocolAction::CloseRemoteStream { sid, message } => {
                    self.close(sid, Some(message)).await?
                }
                ProtocolAction::AlertAndFail { message } => {
                    self.flush_frames(&[Frame::with_data(
                        Command::Alert,
                        0,
                        message.clone().into(),
                    )])
                    .await?;
                    bail!("{message}");
                }
                ProtocolAction::ReleaseWriteBuffering => {}
            }
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        for entry in self.entries.values() {
            entry.status.close(Some("AnyTLS session closed".into()));
        }
    }
}
