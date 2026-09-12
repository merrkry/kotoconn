//! QuickJS policy runtime and native configuration bindings.

mod api;
mod data;
mod handler;
mod host;
mod loader;
mod native;
mod types;

use kotoconn_config::*;
use rquickjs::{CatchResultExt, Context, Function, Module, Runtime};
use std::{collections::HashMap, num::NonZeroU64};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Error(String);

/// A loaded policy and its JS functions. This runtime stays on its owning thread.
/// Promises may use the QuickJS job queue; native asynchronous I/O is not installed.
pub struct Script {
    // Context and its traced host/functions must be freed before the runtime.
    context: Context,
    _runtime: Runtime,
}

impl Script {
    /// Load an entry module and its dependencies from explicit, relative module names.
    /// Configuration registration closes when top-level evaluation finishes.
    pub fn load(entry: &str, sources: HashMap<String, String>) -> Result<Self, Error> {
        let runtime = Runtime::new().map_err(js_error)?;
        runtime.set_loader(loader::Resolver, loader::Loader(sources));
        let context = Context::full(&runtime).map_err(js_error)?;

        context.with(|ctx| {
            (|| {
                let module =
                    Module::declare_def::<loader::Native, _>(ctx.clone(), loader::MODULE_NAME)?;
                module.eval()?.1.finish::<()>()?;
                Module::import(&ctx, entry)?.finish::<()>()?;
                loader::host(&ctx)?.borrow_mut().seal();
                Ok::<_, rquickjs::Error>(())
            })()
            .catch(&ctx)
            .map_err(js_error)
        })?;

        Ok(Self {
            context,
            _runtime: runtime,
        })
    }

    pub fn config(&self) -> Result<Config, Error> {
        self.with(|ctx| Ok(loader::host(ctx)?.borrow().config.clone()))
    }

    pub fn route(&self, id: RoutingHandlerId, flow: Flow) -> Result<RouteDecision, Error> {
        self.with(|ctx| {
            let function = get(&loader::host(ctx)?.borrow().routing, id.0)?;
            Ok(host::RoutingHandler::new(function)
                .call(data::Flow::from(flow))?
                .value)
        })
    }

    pub fn resolve(&self, id: ResolveHandlerId, name: &str) -> Result<Vec<IpAddr>, Error> {
        self.with(|ctx| {
            let function = get(&loader::host(ctx)?.borrow().resolving, id.0)?;
            Ok(host::ResolveHandler::new(function)
                .call(name.to_owned())?
                .into_iter()
                .map(Into::into)
                .collect())
        })
    }

    pub fn dns(&self, id: DnsHandlerId, request: DnsRequest) -> Result<DnsHandlerResult, Error> {
        self.with(|ctx| {
            let function = get(&loader::host(ctx)?.borrow().dns, id.0)?;
            Ok(host::DnsHandler::new(function)
                .call(native::Request::from(request))?
                .value)
        })
    }

    fn with<T>(
        &self,
        f: impl for<'js> FnOnce(&rquickjs::Ctx<'js>) -> rquickjs::Result<T>,
    ) -> Result<T, Error> {
        self.context
            .with(|ctx| f(&ctx).catch(&ctx).map_err(js_error))
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
