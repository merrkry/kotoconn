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
#[convert(from(config::TunAddress), into(config::TunAddress))]
#[ts(as = "config::TunAddress")]
pub(crate) struct TunAddress {
    pub address: IpAddr,
    pub prefix: Byte,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::TunInboundConfig), into(config::TunInboundConfig))]
#[ts(as = "config::TunInboundConfig")]
pub(crate) struct TunInboundConfig {
    pub name: String,
    pub mtu: Mtu,
    pub addresses: Vec<TunAddress>,
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

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::HttpInboundConfig), into(config::HttpInboundConfig))]
#[ts(as = "config::HttpInboundConfig")]
pub(crate) struct HttpInboundConfig {
    pub listen: SocketAddr,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::Socks5InboundConfig), into(config::Socks5InboundConfig))]
#[ts(as = "config::Socks5InboundConfig")]
pub(crate) struct Socks5InboundConfig {
    pub listen: SocketAddr,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(
    from(config::Shadowsocks2022InboundConfig),
    into(config::Shadowsocks2022InboundConfig)
)]
#[ts(as = "config::Shadowsocks2022InboundConfig")]
pub(crate) struct Shadowsocks2022InboundConfig {
    pub listen: SocketAddr,
    pub password: String,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::HttpOutboundConfig), into(config::HttpOutboundConfig))]
#[ts(as = "config::HttpOutboundConfig")]
pub(crate) struct HttpOutboundConfig {
    pub server: Target,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(
    from(config::Shadowsocks2022OutboundConfig),
    into(config::Shadowsocks2022OutboundConfig)
)]
#[ts(as = "config::Shadowsocks2022OutboundConfig")]
pub(crate) struct Shadowsocks2022OutboundConfig {
    pub server: Target,
    pub password: String,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::NaiveOutboundConfig), into(config::NaiveOutboundConfig))]
#[ts(as = "config::NaiveOutboundConfig")]
pub(crate) struct NaiveOutboundConfig {
    pub server: Target,
    pub username: String,
    pub password: String,
    pub server_name: Option<String>,
    pub certificate: Option<String>,
    pub quic: Option<bool>,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::NaiveInboundConfig), into(config::NaiveInboundConfig))]
#[ts(as = "config::NaiveInboundConfig")]
pub(crate) struct NaiveInboundConfig {
    pub listen: SocketAddr,
    pub username: String,
    pub password: String,
    pub certificate: String,
    pub private_key: String,
}
