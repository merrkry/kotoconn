use crate::{api::api, handler::Handler};
use crate::{data::*, native::*};
use kotoconn_config as config;
use rquickjs::{Function, Result};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    num::NonZeroU64,
    time::Duration,
};
use ts_rs::TS;

pub(crate) type RoutingHandler<'js> = Handler<'js, Flow, Decision>;

pub(crate) type ResolveHandler<'js> = Handler<'js, String, Vec<IpAddr>>;

pub(crate) type DnsHandler<'js> = Handler<'js, Request, DnsResult>;

#[derive(rquickjs::JsLifetime)]
#[rquickjs::class]
pub(crate) struct Host<'js> {
    pub config: RefCell<config::Config>,
    pub routing: RefCell<Vec<Function<'js>>>,
    pub resolving: RefCell<Vec<Function<'js>>>,
    pub dns: RefCell<Vec<Function<'js>>>,
    sealed: Cell<bool>,
}

// Registration never calls JS while these cells are borrowed.
impl<'js> rquickjs::class::Trace<'js> for Host<'js> {
    fn trace<'a>(&self, tracer: rquickjs::class::Tracer<'a, 'js>) {
        self.routing.borrow().trace(tracer);
        self.resolving.borrow().trace(tracer);
        self.dns.borrow().trace(tracer);
    }
}

impl<'js> Host<'js> {
    pub fn new() -> Self {
        Self {
            config: RefCell::new(config::Config {
                inbounds: HashMap::new(),
                dialers: HashMap::new(),
                routing_handlers: Default::default(),
                resolve_handlers: Default::default(),
                dns_handlers: Default::default(),
            }),
            routing: RefCell::new(Vec::new()),
            resolving: RefCell::new(Vec::new()),
            dns: RefCell::new(Vec::new()),
            sealed: Cell::new(false),
        }
    }

    fn mutable(&self) -> Result<()> {
        if self.sealed.get() {
            Err(invalid("configuration registration has finished"))
        } else {
            Ok(())
        }
    }

    pub fn seal(&self) {
        self.sealed.set(true);
    }
}

fn next_id(len: usize) -> Result<NonZeroU64> {
    u64::try_from(len)
        .ok()
        .and_then(|n| n.checked_add(1))
        .and_then(NonZeroU64::new)
        .ok_or_else(|| invalid("too many resources"))
}

api! {
    Host as Kotoconn {
        fn ip(self, text: String) -> IpAddr {
            text.parse::<config::IpAddr>()
                .map(Into::into)
                .map_err(|e| invalid(format!("invalid IP address: {e}")))
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

        fn reject(self) -> Decision {
            Ok(config::RouteDecision::Reject.into())
        }

        fn respond(self, response: Response) -> DnsResult {
            Ok(config::DnsHandlerResult::Response(response.value).into())
        }

        fn drop(self) -> DnsResult {
            Ok(config::DnsHandlerResult::Drop.into())
        }

        fn http_inbound(self, options: HttpInboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::Http(options.into()).into())
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

        fn shadowsocks2022_outbound(self, options: Shadowsocks2022OutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::Shadowsocks2022(options.into()).into())
        }

        fn direct_inbound(self, options: DirectInboundConfig) -> InboundImpl {
            Ok(config::InboundImpl::Direct(options.into()).into())
        }

        fn direct_outbound(self, options: DirectOutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::Direct(options.into()).into())
        }

        fn socks5_outbound(self, options: Socks5OutboundConfig) -> OutboundImpl {
            Ok(config::OutboundImpl::Socks5(options.into()).into())
        }

        fn routing_handler(self, handler: RoutingHandler<'js>) -> Routing {
            self.mutable()?;

            let id = config::RoutingHandlerId(next_id(self.routing.borrow().len())?);
            self.routing.borrow_mut().push(handler.function);
            self.config.borrow_mut().routing_handlers.insert(id);

            Ok(id.into())
        }

        fn resolve_handler(self, handler: ResolveHandler<'js>) -> Resolve {
            self.mutable()?;

            let id = config::ResolveHandlerId(next_id(self.resolving.borrow().len())?);
            self.resolving.borrow_mut().push(handler.function);
            self.config.borrow_mut().resolve_handlers.insert(id);

            Ok(id.into())
        }

        fn dns_handler(self, handler: DnsHandler<'js>) -> Dns {
            self.mutable()?;

            let id = config::DnsHandlerId(next_id(self.dns.borrow().len())?);
            self.dns.borrow_mut().push(handler.function);
            self.config.borrow_mut().dns_handlers.insert(id);

            Ok(id.into())
        }

        fn dialer(self, options: DialerConfig) -> Dialer {
            self.mutable()?;

            let options: config::DialerConfig = options.into();
            let id = config::DialerId(next_id(self.config.borrow().dialers.len())?);
            self.config.borrow_mut().dialers.insert(id, options);

            Ok(id.into())
        }

        fn inbound(self, options: InboundConfig) -> Inbound {
            self.mutable()?;

            let id = config::InboundId(next_id(self.config.borrow().inbounds.len())?);
            self.config.borrow_mut().inbounds.insert(id, options.into());

            Ok(id.into())
        }

        fn dns_response(self, bytes: Vec<Byte>) -> Response {
            config::DnsResponse::from_buffer(bytes.into_iter().map(|b| b.0).collect())
                .map(Into::into)
                .map_err(|e| invalid(e.to_string()))
        }

        fn request_bytes(self, request: Request) -> Vec<u8> {
            request.value.to_vec().map_err(|e| invalid(e.to_string()))
        }

        fn response_bytes(self, response: Response) -> Vec<u8> {
            response.value.to_vec().map_err(|e| invalid(e.to_string()))
        }
    }

    async {
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
            }).await?
                .map_err(|error| invalid(error.to_string()))?;
            Ok(addresses.into_iter().map(Into::into).collect())
        }
    }
}
