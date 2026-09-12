//! JS option structs. StructuralConvert checks correspondence with the config crate.

use crate::native::*;
use kotoconn_config as config;
use rquickjs::{Ctx, FromJs, IntoJs, Result, Value};
use structural_convert::StructuralConvert;
use ts_rs::TS;

#[derive(FromJs, IntoJs, TS, StructuralConvert)]
#[convert(from(config::Flow), into(config::Flow))]
#[ts(as = "config::Flow")]
pub(crate) struct Flow {
    pub protocol: TransportProtocol,
    pub dest: Target,
}

#[derive(TS, StructuralConvert)]
#[convert(from(config::TransportProtocol), into(config::TransportProtocol))]
#[ts(as = "config::TransportProtocol")]
pub(crate) enum TransportProtocol {
    Tcp,
    Udp,
}

impl<'js> FromJs<'js> for TransportProtocol {
    fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
        match String::from_js(ctx, value)?.as_str() {
            "tcp" => Ok(Self::Tcp),
            "udp" => Ok(Self::Udp),
            _ => Err(invalid("expected tcp or udp")),
        }
    }
}

impl<'js> IntoJs<'js> for TransportProtocol {
    fn into_js(self, ctx: &Ctx<'js>) -> Result<Value<'js>> {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
        .into_js(ctx)
    }
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::DialerConfig), into(config::DialerConfig))]
#[ts(as = "config::DialerConfig")]
pub(crate) struct DialerConfig {
    pub dialer: Option<Dialer>,
    pub outbound: OutboundConfig,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::OutboundConfig), into(config::OutboundConfig))]
#[ts(as = "config::OutboundConfig")]
pub(crate) struct OutboundConfig {
    pub resolve_handler: Resolve,
    pub implementation: OutboundImpl,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::InboundConfig), into(config::InboundConfig))]
#[ts(as = "config::InboundConfig")]
pub(crate) struct InboundConfig {
    pub routing_handler: Routing,
    pub udp_idle_timeout: Timeout,
    pub implementation: InboundImpl,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::DirectInboundConfig), into(config::DirectInboundConfig))]
#[ts(as = "config::DirectInboundConfig")]
pub(crate) struct DirectInboundConfig {
    pub listen: SocketAddr,
    pub target: Target,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::DirectOutboundConfig), into(config::DirectOutboundConfig))]
#[ts(as = "config::DirectOutboundConfig")]
pub(crate) struct DirectOutboundConfig {}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::Socks5OutboundConfig), into(config::Socks5OutboundConfig))]
#[ts(as = "config::Socks5OutboundConfig")]
pub(crate) struct Socks5OutboundConfig {
    pub server: Target,
}

#[derive(FromJs, TS)]
pub(crate) struct SocketAddr {
    pub address: IpAddr,
    pub port: Port,
}

impl From<SocketAddr> for std::net::SocketAddr {
    fn from(value: SocketAddr) -> Self {
        Self::new(value.address.value, value.port.0)
    }
}

impl From<std::net::SocketAddr> for SocketAddr {
    fn from(value: std::net::SocketAddr) -> Self {
        Self {
            address: value.ip().into(),
            port: value.port().into(),
        }
    }
}
