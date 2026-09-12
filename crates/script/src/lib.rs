//! QuickJS policy runtime and native configuration bindings.

mod api;
mod data;
mod handler;
mod host;
mod loader;
mod native;
mod task;
mod types;

pub use loader::ModuleSource;

use kotoconn_config::*;
use rquickjs::{AsyncContext, AsyncRuntime, CatchResultExt, Function, Module};
use std::{collections::HashMap, num::NonZeroU64};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Error(String);

/// A loaded policy and its JS functions. This runtime stays on its owning thread.
/// All access must be polled on that thread; the daemon supplies the Send + Sync handle.
pub struct Script {
    // Context and its traced host/functions must be freed before the runtime.
    context: AsyncContext,
    runtime: AsyncRuntime,
}

impl Script {
    /// Load an entry module and its dependencies from explicit, relative module names.
    /// Configuration registration closes when top-level evaluation finishes.
    pub async fn load(entry: &str, sources: HashMap<String, String>) -> Result<Self, Error> {
        Self::load_with_interrupt(entry, sources, || false).await
    }

    /// Interrupts must be signalled from outside the JS thread, including during startup.
    /// The callback must not block or enter QuickJS.
    pub async fn load_with_interrupt(
        entry: &str,
        sources: impl ModuleSource + 'static,
        interrupt: impl FnMut() -> bool + 'static,
    ) -> Result<Self, Error> {
        let runtime = AsyncRuntime::new().map_err(js_error)?;
        runtime
            .set_interrupt_handler(Some(Box::new(interrupt)))
            .await;
        runtime
            .set_loader(loader::Resolver, loader::Loader(sources))
            .await;
        let context = AsyncContext::full(&runtime).await.map_err(js_error)?;

        context
            .async_with(async |ctx| {
                (async {
                    let module =
                        Module::declare_def::<loader::Native, _>(ctx.clone(), loader::MODULE_NAME)?;
                    module.eval()?.1.into_future::<()>().await?;
                    Module::import(&ctx, entry)?.into_future::<()>().await?;
                    loader::host(&ctx)?.borrow().seal();
                    Ok::<_, rquickjs::Error>(())
                })
                .await
                .catch(&ctx)
                .map_err(js_error)
            })
            .await?;

        Ok(Self { context, runtime })
    }

    pub async fn config(&self) -> Result<Config, Error> {
        self.context
            .with(|ctx| {
                Ok::<_, rquickjs::Error>(loader::host(&ctx)?.borrow().config.borrow().clone())
            })
            .await
            .map_err(js_error)
    }

    pub async fn route(&self, id: RoutingHandlerId, flow: Flow) -> Result<RouteDecision, Error> {
        let protocol = flow.protocol;
        let decision = self
            .with(async |ctx| {
                let function = get(&loader::host(&ctx)?.borrow().routing.borrow(), id.0)?;
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

    pub async fn resolve(&self, id: ResolveHandlerId, name: &str) -> Result<Vec<IpAddr>, Error> {
        self.with(async |ctx| {
            let function = get(&loader::host(&ctx)?.borrow().resolving.borrow(), id.0)?;
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
        id: DnsHandlerId,
        request: DnsRequest,
    ) -> Result<DnsHandlerResult, Error> {
        self.with(async |ctx| {
            let function = get(&loader::host(&ctx)?.borrow().dns.borrow(), id.0)?;
            Ok(host::DnsHandler::new(function)
                .call(native::Request::from(request))
                .await?
                .value)
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

    /// Continuously drive detached JS promises while the daemon waits for requests.
    pub async fn drive(&self) {
        self.runtime.drive().await;
    }
}

fn get<'js>(functions: &[Function<'js>], id: NonZeroU64) -> rquickjs::Result<Function<'js>> {
    usize::try_from(id.get() - 1)
        .ok()
        .and_then(|index| functions.get(index))
        .cloned()
        .ok_or_else(|| native::invalid("unknown handler ID"))
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
