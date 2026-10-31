use crate::{BoxStream, Datagram, Scope, Target};
use anyhow::Result;
use futures_util::future::BoxFuture;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub trait Handler: Send + Sync {
    /// Source is the accepted transport peer, or the original packet source for TUN.
    fn tcp(
        &self,
        source: SocketAddr,
        destination: Target,
        stream: BoxStream,
        scope: Scope,
    ) -> BoxFuture<'_, Result<()>>;

    /// A protocol association can contain multiple destination-specific sessions.
    /// Source is the peer when the association starts, even if its transport later migrates.
    fn udp(&self, source: SocketAddr, packets: Datagram) -> BoxFuture<'_, Result<()>>;
}

#[derive(Clone)]
pub struct ServerContext {
    pub handler: Arc<dyn Handler>,
    pub scope: Scope,
    pub stopping: CancellationToken,
    pub udp_idle_timeout: Duration,
}

pub struct BoundServer {
    pub local_addr: SocketAddr,
    pub run: BoxFuture<'static, Result<()>>,
}

pub trait Server: Send + Sync {
    fn bind(
        &self,
        address: SocketAddr,
        context: ServerContext,
    ) -> BoxFuture<'_, Result<BoundServer>>;
}

/// Retry after other accept errors, such as descriptor exhaustion, without spinning.
const ACCEPT_RETRY: Duration = Duration::from_secs(1);

/// Accept errors never end the loop: they come from one connection or from a
/// condition the OS can recover from, and the listener stays usable.
pub async fn accept_loop<F, Fut>(
    listener: TcpListener,
    context: ServerContext,
    accept: F,
) -> Result<()>
where
    F: Fn(BoxStream, SocketAddr, SocketAddr, Scope) -> Fut,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let admission = std::sync::Arc::new(tokio::sync::Semaphore::new(1024));
    loop {
        let permit = tokio::select! {
            biased;
            _ = context.stopping.cancelled() => return Ok(()),
            _ = context.scope.cancelled() => return Ok(()),
            permit = admission.clone().acquire_owned() => permit?,
        };

        let accepted = tokio::select! {
            biased;
            _ = context.stopping.cancelled() => return Ok(()),
            _ = context.scope.cancelled() => return Ok(()),
            result = listener.accept() => result,
        };

        let (stream, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(error) if is_connection_error(&error) => {
                tracing::debug!(%error, "connection failed before accept");
                continue;
            }
            Err(error) => {
                tracing::warn!(%error, "accept failed; retrying");
                tokio::select! {
                    biased;
                    _ = context.stopping.cancelled() => return Ok(()),
                    _ = context.scope.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(ACCEPT_RETRY) => continue,
                }
            }
        };

        // The peer can reset the connection before its address is read.
        let local = match stream.local_addr() {
            Ok(local) => local,
            Err(error) => {
                tracing::debug!(%peer, %error, "connection failed after accept");
                continue;
            }
        };

        let scope = context.scope.child();
        let work = accept(Box::pin(stream), peer, local, scope.clone());
        let span = tracing::info_span!("connection", %peer, %local);
        let spawned = span.in_scope(|| {
            scope.spawn(async move {
                let _permit = permit;
                work.await
            })
        });

        // Spawning fails only once the inbound scope closes, which ends the loop.
        if spawned.is_err() {
            debug_assert!(context.scope.is_closed());
            return Ok(());
        }
    }
}

/// Errors that belong to one pending connection. Linux also reports pending
/// network errors of the new connection through accept.
fn is_connection_error(error: &std::io::Error) -> bool {
    use std::io::ErrorKind::*;

    matches!(
        error.kind(),
        ConnectionAborted
            | ConnectionRefused
            | ConnectionReset
            | Interrupted
            | HostUnreachable
            | NetworkDown
            | NetworkUnreachable
            | TimedOut
    )
}
