//! QuickJS policy runtime and native configuration bindings.

mod api;
mod data;
mod dns;
mod handler;
mod host;
mod loader;
mod log;
mod native;
mod process;
mod rejection;
mod task;
mod types;

pub use loader::ModuleSource;

use kotoconn_config::*;
use rquickjs::{AsyncContext, AsyncRuntime, CatchResultExt, Module};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

pub type NativeFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

/// The native resource manager. It exists before the entry module runs, so policy
/// can create and close resources at any time.
pub trait Resources: Send + Sync + 'static {
    /// Validate references and start the resource. Errors reject the creation.
    fn create(&self, config: ResourceConfig) -> NativeFuture<Result<ResourceId, String>>;

    /// Abort the resource. Closing a closed resource does nothing.
    fn close(&self, id: ResourceId) -> Result<(), String>;

    /// Stop admission and abort the remaining work after `timeout`.
    fn drain(&self, id: ResourceId, timeout: Duration) -> Result<(), String>;

    /// Completes once the resource has closed and released its native state.
    fn released(&self, id: ResourceId) -> NativeFuture<()>;

    fn dns_query(
        &self,
        service: DnsServiceId,
        request: DnsRequest,
    ) -> NativeFuture<Result<DnsResponse, String>>;
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Error(String);

/// A policy runtime and its JS functions. This runtime stays on its owning thread.
/// All access must be polled on that thread; the daemon supplies the Send + Sync handle.
pub struct Script {
    // Context and its traced host/functions must be freed before the runtime.
    context: AsyncContext,
    runtime: AsyncRuntime,
    rejections: rejection::Tracker,
    released: Arc<host::Released>,
}

impl Script {
    /// Interrupts must be signalled from outside the JS thread, including during startup.
    /// The callback must not block or enter QuickJS.
    pub async fn new(
        sources: impl ModuleSource + 'static,
        resources: Arc<dyn Resources>,
        interrupt: impl FnMut() -> bool + 'static,
    ) -> Result<Self, Error> {
        let runtime = AsyncRuntime::new().map_err(js_error)?;
        runtime
            .set_interrupt_handler(Some(Box::new(interrupt)))
            .await;
        runtime
            .set_loader(loader::Resolver, loader::Loader(sources))
            .await;
        let rejections = rejection::Tracker::new();
        runtime
            .set_host_promise_rejection_tracker(Some(rejections.handler()))
            .await;
        let context = AsyncContext::full(&runtime).await.map_err(js_error)?;

        let released = context
            .async_with(async |ctx| {
                (async {
                    loader::install(&ctx, resources)?;
                    let module =
                        Module::declare_def::<loader::Native, _>(ctx.clone(), loader::MODULE_NAME)?;
                    module.eval()?.1.into_future::<()>().await?;
                    Ok(loader::host(&ctx)?.borrow().released.clone())
                })
                .await
                .catch(&ctx)
                .map_err(js_error)
            })
            .await?;

        Ok(Self {
            context,
            runtime,
            rejections,
            released,
        })
    }

    /// Evaluate the entry module and its dependencies from explicit, relative module
    /// names. Startup finishes once evaluation, including top-level await, and every
    /// resource creation started until then have settled. An unhandled rejection
    /// before then fails startup. Handler calls may run concurrently.
    pub async fn evaluate(&self, entry: &str) -> Result<(), Error> {
        let evaluated = self
            .with(async |ctx| {
                Module::import(&ctx, entry)?.into_future::<()>().await?;

                let creations = loader::host(&ctx)?.borrow().creations.clone();
                loop {
                    creations.settled().await;
                    self.rejections.settled().await;
                    // A rejection handler can start another creation.
                    if creations.is_settled() {
                        break;
                    }
                }
                Ok(())
            })
            .await;

        let reasons = self.rejections.finish_startup();
        if let Err(error) = evaluated {
            for reason in reasons {
                tracing::warn!(target: "policy", %reason, "unhandled promise rejection");
            }
            return Err(error);
        }

        match reasons.as_slice() {
            [] => Ok(()),
            reasons => Err(Error(format!(
                "unhandled promise rejection during startup: {}",
                reasons.join("; ")
            ))),
        }
    }

    pub async fn route(
        &self,
        handler: &RoutingHandler,
        flow: Flow,
    ) -> Result<RouteDecision, Error> {
        let protocol = flow.protocol;

        let decision = self
            .with(async |ctx| {
                let function = loader::host(&ctx)?.borrow().handler(handler.id())?;
                Ok(host::RoutingHandler::new(function)
                    .call(data::Flow::from(flow))
                    .await?
                    .value)
            })
            .await?;

        match (&decision, protocol) {
            (RouteDecision::Route { .. }, TransportProtocol::Udp) => Err(Error(
                "UDP routing must use route_udp; target override is not supported".into(),
            )),
            (RouteDecision::Udp { .. }, TransportProtocol::Tcp) => {
                Err(Error("TCP routing must use route".into()))
            }
            _ => Ok(decision),
        }
    }

    pub async fn resolve(
        &self,
        handler: &ResolveHandler,
        name: &str,
    ) -> Result<Vec<IpAddr>, Error> {
        self.with(async |ctx| {
            let function = loader::host(&ctx)?.borrow().handler(handler.id())?;

            Ok(host::ResolveHandler::new(function)
                .call(name.to_owned())
                .await?
                .into_iter()
                .map(Into::into)
                .collect())
        })
        .await
    }

    pub async fn dns(
        &self,
        handler: &DnsHandler,
        request: DnsRequest,
        info: Option<DnsRequestInfo>,
    ) -> Result<DnsHandlerResult, Error> {
        self.with(async |ctx| {
            let function = loader::host(&ctx)?.borrow().handler(handler.id())?;

            Ok(host::DnsHandler::new(function)
                .call(dns::Request {
                    value: request,
                    info,
                })
                .await?
                .value)
        })
        .await
    }

    /// Start policy shutdown: reject pending and later sleeps, then run shutdown hooks
    /// one at a time, newest first. A failed hook is logged and the next one still runs.
    /// Handler calls may run concurrently, and hooks they register during this call run too.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.with(async |ctx| {
            let host = loader::host(&ctx)?;
            host.borrow().shutdown.cancel();

            loop {
                // Release the host borrow before calling JS, which may register more hooks.
                let Some(function) = host.borrow().next_shutdown_hook() else {
                    return Ok(());
                };

                if let Err(error) = handler::Hook::new(function).call().await.catch(&ctx) {
                    tracing::error!(%error, "shutdown hook failed");
                }
            }
        })
        .await
    }

    async fn with<T: 'static>(
        &self,
        f: impl for<'js> AsyncFnOnce(rquickjs::Ctx<'js>) -> rquickjs::Result<T>,
    ) -> Result<T, Error> {
        self.context
            .async_with(async |ctx| f(ctx.clone()).await.catch(&ctx).map_err(js_error))
            .await
    }

    /// Drive native futures started by JS even when no handler call is waiting.
    pub async fn idle(&self) {
        self.runtime.idle().await;
    }

    /// Continuously drive detached JS promises while the daemon waits for requests,
    /// and unregister handlers once their last native reference drops.
    pub async fn drive(&self) {
        let collect = async {
            loop {
                self.released.notify.notified().await;
                let collected = self
                    .with(async |ctx| {
                        loader::host(&ctx)?.borrow().collect_released();
                        Ok(())
                    })
                    .await;
                if let Err(error) = collected {
                    tracing::error!(%error, "releasing handlers failed");
                }
            }
        };

        tokio::join!(self.runtime.drive(), collect);
    }
}

fn js_error(error: impl std::fmt::Display) -> Error {
    Error(error.to_string())
}

/// Generated declarations for the exact native API installed by this crate.
pub fn typescript_declarations() -> String {
    types::declarations()
}

#[cfg(test)]
#[path = "tests/runtime.rs"]
mod tests;
