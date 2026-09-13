use crate::{BoxStream, Datagram, Scope, Target};
use anyhow::Result;
use futures_util::future::BoxFuture;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

pub trait Handler: Send + Sync {
    fn tcp(
        &self,
        destination: Target,
        stream: BoxStream,
        scope: Scope,
    ) -> BoxFuture<'_, Result<()>>;
    /// A protocol association can contain multiple destination-specific sessions.
    fn udp(&self, packets: Datagram) -> BoxFuture<'_, Result<()>>;
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

        tokio::select! {
            biased;
            _ = context.stopping.cancelled() => return Ok(()),
            _ = context.scope.cancelled() => return Ok(()),
            result = listener.accept() => {
                let (stream, peer) = result?;
                let local = stream.local_addr()?;

                let scope = context.scope.child();
                let work = accept(Box::pin(stream), peer, local, scope.clone());
                scope.spawn(async move { let _permit = permit; work.await })?;
            }
        }
    }
}
