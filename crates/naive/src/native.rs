//! Async I/O adaptation only. Cronet implements TLS, H2, H3 and flow control.
use crate::padding::Padding;
use cronet::{
    BidirectionalStream, BidirectionalStreamHandle, BidirectionalStreamHandler, Engine, Header,
};
use kotoconn_protocol::{BoxStream, Scope, Stream};
use std::{
    future::poll_fn,
    io,
    pin::Pin,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    task::{Context, Poll, Waker, ready},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf},
    sync::oneshot,
};

const BUFFER: usize = 65536;

struct State {
    ready: bool,
    headers: Option<Vec<Header>>,
    terminal: bool,
    error: Option<String>,
    read: Box<[u8]>,
    reading: bool,
    available: std::ops::Range<usize>,
    eof: bool,
    write: Vec<u8>,
    writing: bool,
    shutdown: bool,
    reader: Option<Waker>,
    writer: Option<Waker>,
    control: Option<Waker>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            ready: false,
            headers: None,
            terminal: false,
            error: None,
            read: vec![0; BUFFER].into_boxed_slice(),
            reading: false,
            available: 0..0,
            eof: false,
            write: Vec::with_capacity(BUFFER),
            writing: false,
            shutdown: false,
            reader: None,
            writer: None,
            control: None,
        }
    }
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    finished: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn terminal(&self, error: Option<String>) {
        let mut state = self.lock();
        state.terminal = true;
        state.error = error;
        state.reading = false;
        state.writing = false;
        state.eof = true;
        wake(&mut state.reader);
        wake(&mut state.writer);
        wake(&mut state.control);
        self.finished.notify_all();
    }
}

fn wake(waker: &mut Option<Waker>) {
    if let Some(waker) = waker.take() {
        waker.wake();
    }
}

fn failure(state: &State) -> io::Result<()> {
    match &state.error {
        Some(error) => Err(io::Error::other(error.clone())),
        None => Ok(()),
    }
}

struct Callbacks(Arc<Shared>);

impl BidirectionalStreamHandler for Callbacks {
    fn on_stream_ready(&mut self, _: BidirectionalStreamHandle) {
        let mut state = self.0.lock();
        state.ready = true;
        wake(&mut state.reader);
        wake(&mut state.writer);
    }

    fn on_response_headers(
        &mut self,
        _: BidirectionalStreamHandle,
        headers: Vec<Header>,
        _: String,
    ) {
        let mut state = self.0.lock();
        state.headers = Some(headers);
        wake(&mut state.control);
    }

    fn on_read_completed(&mut self, _: BidirectionalStreamHandle, n: i32) {
        let mut state = self.0.lock();
        debug_assert!(state.reading);
        state.reading = false;
        if let Ok(n) = usize::try_from(n)
            && n <= state.read.len()
        {
            state.available = 0..n;
            state.eof = n == 0;
        } else {
            state.error = Some("Cronet returned an invalid read length".into());
        }
        wake(&mut state.reader);
    }

    fn on_write_completed(&mut self, _: BidirectionalStreamHandle) {
        let mut state = self.0.lock();
        debug_assert!(state.writing);
        state.writing = false;
        wake(&mut state.writer);
    }

    fn on_succeeded(&mut self, _: BidirectionalStreamHandle) {
        self.0.terminal(None);
    }

    fn on_failed(&mut self, _: BidirectionalStreamHandle, error: i32) {
        self.0
            .terminal(Some(format!("Cronet network error {error}")));
    }

    fn on_canceled(&mut self, _: BidirectionalStreamHandle) {
        self.0.terminal(Some("Cronet stream canceled".into()));
    }
}

/// Keeps buffers alive until native cancellation has finished, including when
/// Tokio drops the task during runtime shutdown.
struct Session<'a> {
    stream: BidirectionalStream<'a>,
    shared: Arc<Shared>,
    started: bool,
}

impl Session<'_> {
    async fn finish(&self) {
        if !self.shared.lock().terminal {
            self.stream.cancel();
        }
        poll_fn(|cx| {
            let mut state = self.shared.lock();
            if state.terminal {
                Poll::Ready(())
            } else {
                state.control = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        if !self.started || self.shared.lock().terminal {
            return;
        }
        self.stream.cancel();
        // Native callbacks run on Cronet's thread. Dialer hooks never wait for
        // Tokio, so this fallback also works after the runtime has stopped.
        let mut state = self.shared.lock();
        while !state.terminal {
            state = self
                .shared
                .finished
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

struct NativeIo<'a, 'engine> {
    session: &'a Session<'engine>,
}

impl AsyncRead for NativeIo<'_, '_> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if out.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut state = self.session.shared.lock();
        failure(&state)?;
        if !state.available.is_empty() {
            debug_assert!(!state.reading);
            let n = out.remaining().min(state.available.len());
            let start = state.available.start;
            out.put_slice(&state.read[start..start + n]);
            state.available.start += n;
            return Poll::Ready(Ok(()));
        }
        if state.eof {
            return Poll::Ready(Ok(()));
        }

        state.reader = Some(cx.waker().clone());
        if state.ready && !state.reading {
            state.reading = true;
            // SAFETY: The boxed buffer stays in Shared and is neither accessed
            // nor replaced until the read callback. Session waits for a terminal
            // callback before destroying the stream or freeing Shared.
            if unsafe { self.session.stream.read(&mut state.read) } == 0 {
                state.reading = false;
                return Poll::Ready(Err(io::Error::other("Cronet rejected read")));
            }
        }
        Poll::Pending
    }
}

impl AsyncWrite for NativeIo<'_, '_> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.session.shared.lock();
        failure(&state)?;
        if state.shutdown || state.terminal {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if !state.ready || state.writing {
            state.writer = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = bytes.len().min(BUFFER);
        state.write.clear();
        state.write.extend_from_slice(&bytes[..n]);
        state.writing = true;
        // SAFETY: Shared owns the write allocation. No mutation occurs while
        // writing is true; completion or terminal callbacks release it. Session
        // waits for native completion even when the Rust task is dropped.
        if unsafe { self.session.stream.write(&state.write, false) } == 0 {
            state.writing = false;
            return Poll::Ready(Err(io::Error::other("Cronet rejected write")));
        }
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.session.shared.lock();
        failure(&state)?;
        if state.writing {
            state.writer = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        ready!(self.as_mut().poll_flush(cx))?;
        let mut state = self.session.shared.lock();
        if state.shutdown {
            return Poll::Ready(Ok(()));
        }
        if !state.ready {
            state.writer = Some(cx.waker().clone());
            return Poll::Pending;
        }
        state.write.clear();
        state.shutdown = true;
        state.writing = true;
        state.writer = Some(cx.waker().clone());
        // SAFETY: The empty buffer remains owned by Shared until the EOS write
        // callback, with the same lifetime guarantees as ordinary writes above.
        if unsafe { self.session.stream.write(&state.write, true) } == 0 {
            state.writing = false;
            return Poll::Ready(Err(io::Error::other("Cronet rejected end of stream")));
        }
        Poll::Pending
    }
}

struct Tunnel {
    io: DuplexStream,
    scope: Scope,
    error: Arc<Mutex<Option<String>>>,
}

impl AsyncRead for Tunnel {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = out.filled().len();
        ready!(Pin::new(&mut self.io).poll_read(cx, out))?;
        if out.filled().len() == before
            && out.remaining() != 0
            && let Some(error) = &*self
                .error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            return Poll::Ready(Err(io::Error::other(error.clone())));
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for Tunnel {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

impl Stream for Tunnel {}

impl Drop for Tunnel {
    fn drop(&mut self) {
        self.scope.close();
    }
}

pub(crate) async fn connect(
    engine: Arc<Engine>,
    url: String,
    headers: Vec<Header>,
    scope: Scope,
) -> anyhow::Result<BoxStream> {
    let scope = scope.child();
    let guard = scope.track()?;
    let (io, mut relay) = tokio::io::duplex(BUFFER);
    let error = Arc::new(Mutex::new(None));
    let tunnel = Tunnel {
        io,
        scope: scope.clone(),
        error: error.clone(),
    };
    let (started, ready) = oneshot::channel();

    tokio::spawn(async move {
        let _guard = guard;
        let shared = Arc::new(Shared::default());
        let mut session = Session {
            stream: engine.create_bidirectional_stream(Callbacks(shared.clone())),
            shared: shared.clone(),
            started: false,
        };
        if !session.stream.start("CONNECT", &url, &headers, 0, false) {
            let _ = started.send(Err(io::Error::other("Cronet rejected CONNECT parameters")));
            return;
        }
        session.started = true;
        let mut started = Some(started);

        let work = async {
            let negotiated = poll_fn(|cx| {
                let mut state = shared.lock();
                failure(&state)?;
                if let Some(headers) = state.headers.take() {
                    Poll::Ready(Ok::<_, io::Error>(headers))
                } else {
                    state.control = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await?;
            if !negotiated
                .iter()
                .any(|h| h.name == ":status" && h.value == "200")
            {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    "Naive CONNECT was rejected",
                ));
            }
            let padded = negotiated
                .iter()
                .any(|h| h.name.eq_ignore_ascii_case("padding"));
            // SAFETY: Only this handshake consumes the sender, once.
            started
                .take()
                .expect("CONNECT handshake sender")
                .send(Ok(()))
                .map_err(|_| io::Error::from(io::ErrorKind::ConnectionAborted))?;

            let mut native = Padding::new(NativeIo { session: &session }, padded);
            tokio::io::copy_bidirectional_with_sizes(&mut relay, &mut native, BUFFER, BUFFER)
                .await?;
            Ok::<_, io::Error>(())
        };
        let result = tokio::select! {
            biased;
            _ = scope.cancelled() => Ok(()),
            result = work => result,
        };
        if let Err(failure) = result {
            *error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(failure.to_string());
            if let Some(started) = started.take() {
                let _ = started.send(Err(failure));
            }
        }
        session.finish().await;
        drop(session);
        drop(engine);
    });

    match ready.await {
        Ok(result) => result?,
        Err(_) => {
            let message = tunnel
                .error
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .unwrap_or_else(|| "Naive CONNECT canceled".into());
            anyhow::bail!(message);
        }
    }
    Ok(Box::pin(tunnel))
}
