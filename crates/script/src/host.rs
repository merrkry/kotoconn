use crate::Resources;
use crate::api::{Optional, Parameter, Started, Void, api};
use crate::handler::{Handler, Hook};
use crate::log::LogLevel;
use crate::process::{ExecOptions, ExecResult};
use crate::{data::*, dns::*, native::*};
use kotoconn_config as config;
use rquickjs::{Function, Result};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    future::poll_fn,
    num::NonZeroU64,
    rc::Rc,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
use ts_rs::TS;

pub(crate) type RoutingHandler<'js> = Handler<'js, Flow, Decision>;

pub(crate) type ResolveHandler<'js> = Handler<'js, String, Vec<IpAddr>>;

pub(crate) type DnsHandler<'js> = Handler<'js, Request, DnsResult>;

#[derive(rquickjs::JsLifetime)]
#[rquickjs::class]
pub(crate) struct Host<'js> {
    pub resources: Arc<dyn Resources>,
    /// Functions that native references keep registered.
    handlers: RefCell<HashMap<u64, Function<'js>>>,
    next_handler: Cell<u64>,
    pub released: Arc<Released>,
    pub creations: Rc<Creations>,
    /// Cancelled when shutdown starts; pending and later sleeps reject.
    pub shutdown: CancellationToken,
    /// Shutdown hooks in registration order; `None` once they have all run.
    shutdown_hooks: RefCell<Option<Vec<Function<'js>>>>,
}

// Registration never calls JS while these cells are borrowed.
impl<'js> rquickjs::class::Trace<'js> for Host<'js> {
    fn trace<'a>(&self, tracer: rquickjs::class::Tracer<'a, 'js>) {
        for function in self.handlers.borrow().values() {
            function.trace(tracer);
        }
        self.shutdown_hooks.borrow().trace(tracer);
    }
}

/// Keeps a handler registered until the last native reference drops.
struct Registration {
    id: NonZeroU64,
    released: Arc<Released>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        // A poisoned list still holds valid IDs; a panic elsewhere must not leak handlers.
        self.released
            .ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(self.id);
        self.released.notify.notify_one();
    }
}

/// Handlers whose last native reference has dropped, possibly on another thread.
#[derive(Default)]
pub(crate) struct Released {
    ids: Mutex<Vec<NonZeroU64>>,
    /// Wakes the JS thread to unregister them.
    pub notify: tokio::sync::Notify,
}

/// Counts resource creations that have not settled, so startup can wait for them.
#[derive(Default)]
pub(crate) struct Creations {
    pending: Cell<usize>,
    settled: RefCell<Vec<Waker>>,
}

pub(crate) struct Creation(Rc<Creations>);

impl Drop for Creation {
    fn drop(&mut self) {
        let pending = self.0.pending.get();
        debug_assert!(pending > 0, "each creation is counted once");
        self.0.pending.set(pending - 1);

        if pending == 1 {
            for waker in self.0.settled.take() {
                waker.wake();
            }
        }
    }
}

impl Creations {
    pub fn start(self: &Rc<Self>) -> Creation {
        self.pending.set(self.pending.get() + 1);
        Creation(self.clone())
    }

    pub fn is_settled(&self) -> bool {
        self.pending.get() == 0
    }

    pub async fn settled(&self) {
        poll_fn(|cx| {
            if self.is_settled() {
                Poll::Ready(())
            } else {
                self.settled.borrow_mut().push(cx.waker().clone());
                Poll::Pending
            }
        })
        .await;
    }
}

impl<'js> Host<'js> {
    pub fn new(resources: Arc<dyn Resources>) -> Self {
        Self {
            resources,
            handlers: RefCell::default(),
            next_handler: Cell::new(0),
            released: Arc::default(),
            creations: Rc::default(),
            shutdown: CancellationToken::new(),
            shutdown_hooks: RefCell::new(Some(Vec::new())),
        }
    }

    /// Registers a function for native calls until the returned reference and all
    /// its clones drop.
    pub fn register<K>(&self, function: Function<'js>) -> Result<config::Handler<K>> {
        let id = self
            .next_handler
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| invalid("too many handlers"))?;
        self.next_handler.set(id.get());
        self.handlers.borrow_mut().insert(id.get(), function);

        let registration = Registration {
            id,
            released: self.released.clone(),
        };
        Ok(config::Handler::new(id, Arc::new(registration)))
    }

    /// Native callers hold a reference for the whole call, so a called handler is
    /// still registered.
    pub fn handler(&self, id: NonZeroU64) -> Result<Function<'js>> {
        self.handlers
            .borrow()
            .get(&id.get())
            .cloned()
            .ok_or_else(|| invalid("handler is no longer registered"))
    }

    /// Unregisters the handlers whose last native reference has dropped.
    pub fn collect_released(&self) {
        let released = std::mem::take(
            &mut *self
                .released
                .ids
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );

        let mut handlers = self.handlers.borrow_mut();
        for id in released {
            let removed = handlers.remove(&id.get());
            debug_assert!(removed.is_some(), "each handler is released once");
        }
    }

    /// Starts creation immediately, so startup counts it before the promise is polled.
    fn create<T: 'js>(
        &self,
        config: config::ResourceConfig,
        reference: fn(config::ResourceId) -> T,
    ) -> Started<'js, T> {
        let creation = self.creations.start();
        let created = self.resources.create(config);

        Started::new(async move {
            let _creation = creation;
            let id = crate::task::run(created).await?.map_err(invalid)?;
            Ok(reference(id))
        })
    }

    /// Takes the most recently registered hook that has not run yet. Once none
    /// remain, later registrations fail instead of being silently skipped.
    pub fn next_shutdown_hook(&self) -> Option<Function<'js>> {
        debug_assert!(
            self.shutdown.is_cancelled(),
            "hooks run only after sleeps are released"
        );

        let mut hooks = self.shutdown_hooks.borrow_mut();
        let next = hooks.as_mut()?.pop();
        if next.is_none() {
            *hooks = None;
        }
        next
    }
}

fn matches_domain_suffix(name: &str, suffix: &str, include_self: bool) -> Result<bool> {
    if name.is_empty() || suffix.is_empty() {
        return Err(invalid("domain and suffix must not be empty"));
    }

    let name = name
        .parse::<hickory_proto::rr::Name>()
        .map_err(|e| invalid(format!("invalid domain: {e}")))?;
    let suffix = suffix
        .parse::<hickory_proto::rr::Name>()
        .map_err(|e| invalid(format!("invalid domain suffix: {e}")))?;

    let matches_suffix = suffix.zone_of(&name);
    let has_extra_label = name.iter().count() > suffix.iter().count();

    Ok(matches_suffix && (include_self || has_extra_label))
}

api! {
    Host as Kotoconn {
        fn ip(self, text: String) -> IpAddr {
            text.parse::<config::IpAddr>()
                .map(Into::into)
                .map_err(|e| invalid(format!("invalid IP address: {e}")))
        }

        fn cidr(self, text: String) -> Cidr {
            text.parse::<ipnet::IpNet>()
                .map(Into::into)
                .map_err(|e| invalid(format!("invalid CIDR: {e}")))
        }

        fn domain_suffix(self, name: String, suffix: String) -> bool {
            matches_domain_suffix(&name, &suffix, true)
        }

        fn is_subdomain(self, name: String, parent: String) -> bool {
            matches_domain_suffix(&name, &parent, false)
        }

        fn timeout(self, milliseconds: Milliseconds) -> Timeout {
            Ok(Duration::from_millis(milliseconds.0.into()).into())
        }

        fn domain(self, name: String, port: Port) -> Target {
            Ok(config::Target::Domain { name, port: port.0 }.into())
        }

        fn ip_target(self, address: IpAddr, port: Port) -> Target {
            Ok(config::Target::Ip {
                address: address.value,
                port: port.0,
            }
            .into())
        }

        fn route(self, dialer: Dialer, target: Target) -> Decision {
            Ok(config::RouteDecision::Route {
                dialer: dialer.value,
                target: target.value,
            }
            .into())
        }

        fn route_udp(self, dialer: Dialer) -> Decision {
            Ok(config::RouteDecision::Udp { dialer: dialer.value }.into())
        }

        fn route_dns(self, handler: DnsHandler<'js>) -> Decision {
            Ok(config::RouteDecision::Dns {
                handler: self.register(handler.function)?,
            }
            .into())
        }

        fn reject(self) -> Decision {
            Ok(config::RouteDecision::Reject.into())
        }

        fn respond(self, response: Response) -> DnsResult {
            Ok(config::DnsHandlerResult::Response(response.value).into())
        }

        fn drop(self) -> DnsResult {
            Ok(config::DnsHandlerResult::Drop.into())
        }

        fn hysteria2_inbound(self, options: Hysteria2InboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::Hysteria2(options.into()).into())
        }

        fn hysteria2_outbound(self, options: Hysteria2OutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::Hysteria2(options.into()).into())
        }

        fn http_inbound(self, options: HttpInboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::Http(options.into()).into())
        }

        fn anytls_inbound(self, options: AnyTlsInboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::AnyTls(options.into()).into())
        }

        fn anytls_outbound(self, options: AnyTlsOutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::AnyTls(options.into()).into())
        }

        fn socks5_inbound(self, options: Socks5InboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::Socks5(options.into()).into())
        }

        fn shadowsocks2022_inbound(self, options: Shadowsocks2022InboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::Shadowsocks2022(options.into()).into())
        }

        fn http_outbound(self, options: HttpOutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::Http(options.into()).into())
        }

        fn naive_inbound(self, options: NaiveInboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::Naive(options.into()).into())
        }

        fn naive_outbound(self, options: NaiveOutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::Naive(options.into()).into())
        }

        fn shadowsocks2022_outbound(self, options: Shadowsocks2022OutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::Shadowsocks2022(options.into()).into())
        }

        fn direct_inbound(self, options: DirectInboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::Direct(options.into()).into())
        }

        fn tun_inbound(self, options: TunInboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::Tun(options.into()).into())
        }

        fn direct_outbound(self, options: DirectOutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::Direct(options.into()).into())
        }

        fn socks5_outbound(self, options: Socks5OutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::Socks5(options.into()).into())
        }

        fn host_network(self, options: HostNetworkConfig) -> HostNetwork {
            Ok(config::HostNetworkConfig::from(options).into())
        }

        fn dns_response(self, bytes: Bytes) -> Response {
            config::DnsResponse::from_buffer(bytes.0)
                .map(Into::into)
                .map_err(|e| invalid(e.to_string()))
        }

        fn dns_request(self, name: String, record_type: RecordType) -> Request {
            crate::dns::request(&name, record_type).map(Into::into)
        }

        fn request_bytes(self, request: Request) -> Bytes {
            request.value.to_vec().map(Bytes).map_err(|e| invalid(e.to_string()))
        }

        fn response_bytes(self, response: Response) -> Bytes {
            response.value.to_vec().map(Bytes).map_err(|e| invalid(e.to_string()))
        }

        fn dialer(self, options: DialerConfig<'js>) -> Started<'js, Dialer> {
            let config = config::ResourceConfig::Dialer(options.register(self)?);
            Ok(self.create(config, |id| config::DialerId(id.0).into()))
        }

        fn inbound(self, options: InboundConfig<'js>) -> Started<'js, Inbound> {
            let config = config::ResourceConfig::Inbound(options.register(self)?);
            Ok(self.create(config, |id| config::InboundId(id.0).into()))
        }

        fn dns_service(self, options: DnsServiceConfig) -> Started<'js, DnsServiceRef> {
            let config = config::ResourceConfig::DnsService(options.into());
            Ok(self.create(config, |id| config::DnsServiceId(id.0).into()))
        }

        fn dns_listener(self, options: DnsListenerConfig<'js>) -> Started<'js, Inbound> {
            let config = config::ResourceConfig::DnsListener(options.register(self)?);
            Ok(self.create(config, |id| config::InboundId(id.0).into()))
        }

        fn log(self, level: LogLevel, message: String) -> Void {
            crate::log::log(level, &message);
            Ok(Void)
        }

        fn on_shutdown(self, handler: Hook<'js>) -> Void {
            self.shutdown_hooks
                .borrow_mut()
                .as_mut()
                .ok_or_else(|| invalid("shutdown hooks have already run"))?
                .push(handler.function);

            Ok(Void)
        }
    }

    async {
        fn dns_query(self, service: DnsServiceRef, request: Request) -> Response {
            let response = crate::task::run(self.resources.dns_query(service.value, request.value))
                .await?
                .map_err(invalid)?;
            Ok(response.into())
        }

        fn probe(self, dialer: Dialer, options: ProbeOptions) -> ProbeResult {
            let probe = config::Probe {
                target: options.target.value,
                request: options.request.map(|request| request.0).unwrap_or_default(),
                length: options.length.map_or(1, |length| usize::from(length.0)),
                timeout: options.timeout.value,
            };
            let result = crate::task::run(self.resources.probe(dialer.value, probe))
                .await?
                .map_err(invalid)?;

            Ok(ProbeResult {
                latency: result.latency.as_secs_f64() * 1000.0,
                response: Bytes(result.response),
            })
        }

        fn lookup(self, name: String) -> Vec<IpAddr> {
            let addresses = crate::task::run(async move {
                let addresses = tokio::net::lookup_host((name.as_str(), 0)).await?;
                let mut result = Vec::new();
                for address in addresses {
                    if !result.contains(&address.ip()) {
                        result.push(address.ip());
                    }
                }
                Ok::<_, std::io::Error>(result)
            })
            .await?
            .map_err(|error| invalid(error.to_string()))?;
            Ok(addresses.into_iter().map(Into::into).collect())
        }

        fn sleep(self, timeout: Timeout) -> Void {
            let shutdown = self.shutdown.clone();

            let elapsed = crate::task::run(async move {
                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => false,
                    _ = tokio::time::sleep(timeout.value) => true,
                }
            })
            .await?;

            if elapsed {
                Ok(Void)
            } else {
                Err(invalid("sleep cancelled by shutdown"))
            }
        }

        fn exec(self, argv: Vec<String>, options: Optional<ExecOptions>) -> ExecResult {
            let (timeout, input) = match options.0 {
                Some(options) => (options.timeout.map(|timeout| timeout.value), options.input),
                None => (None, None),
            };

            crate::task::run(crate::process::exec(argv, timeout, input))
                .await?
                .map_err(invalid)
        }
    }
}
