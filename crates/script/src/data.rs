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
    pub inbound: Inbound,
    pub source: SocketAddr,
    pub protocol: TransportProtocol,
    pub dest: Target,
    pub sniff: Option<SniffResult>,
}

#[derive(FromJs, IntoJs, TS, StructuralConvert)]
#[convert(from(config::SniffResult), into(config::SniffResult))]
#[ts(as = "config::SniffResult")]
pub(crate) struct SniffResult {
    pub protocol: SniffProtocol,
    pub domain: Option<String>,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::SniffConfig), into(config::SniffConfig))]
#[ts(as = "config::SniffConfig")]
pub(crate) struct SniffConfig {
    pub timeout: Timeout,
}

#[derive(TS, StructuralConvert)]
#[convert(from(config::SniffProtocol), into(config::SniffProtocol))]
#[ts(as = "config::SniffProtocol")]
pub(crate) enum SniffProtocol {
    Http,
    Tls,
    Quic,
}

impl<'js> FromJs<'js> for SniffProtocol {
    fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
        match String::from_js(ctx, value)?.as_str() {
            "http" => Ok(Self::Http),
            "tls" => Ok(Self::Tls),
            "quic" => Ok(Self::Quic),
            _ => Err(invalid("expected http, tls or quic")),
        }
    }
}

impl<'js> IntoJs<'js> for SniffProtocol {
    fn into_js(self, ctx: &Ctx<'js>) -> Result<Value<'js>> {
        match self {
            Self::Http => "http",
            Self::Tls => "tls",
            Self::Quic => "quic",
        }
        .into_js(ctx)
    }
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
    pub carrier: CarrierConfig,
    pub outbound: OutboundConfig,
}

#[derive(TS, StructuralConvert)]
#[convert(from(config::CarrierConfig), into(config::CarrierConfig))]
#[ts(as = "config::CarrierConfig")]
pub(crate) enum CarrierConfig {
    Dialer(Dialer),
    Host(HostNetwork),
}

impl<'js> FromJs<'js> for CarrierConfig {
    fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
        if let Ok(dialer) = Dialer::from_js(ctx, value.clone()) {
            return Ok(Self::Dialer(dialer));
        }
        HostNetwork::from_js(ctx, value)
            .map(Self::Host)
            .map_err(|_| invalid("expected a dialer or host network carrier"))
    }
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::HostNetworkConfig), into(config::HostNetworkConfig))]
#[ts(as = "config::HostNetworkConfig")]
pub(crate) struct HostNetworkConfig {
    pub mark: Option<Mark>,
    pub interface: Option<String>,
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
    pub sniff: Option<SniffConfig>,
    pub udp_idle_timeout: Timeout,
    pub implementation: InboundImpl,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::DnsUpstreamConfig), into(config::DnsUpstreamConfig))]
#[ts(as = "config::DnsUpstreamConfig")]
pub(crate) struct DnsUpstreamConfig {
    pub protocol: TransportProtocol,
    pub server: SocketAddr,
}

#[derive(FromJs, TS)]
#[ts(as = "config::DnsServiceConfig")]
pub(crate) struct DnsServiceConfig {
    pub upstream: DnsUpstreamConfig,
    pub dialer: Dialer,
    pub cache_capacity: Capacity,
    pub timeout: Timeout,
}

impl From<DnsServiceConfig> for config::DnsServiceConfig {
    fn from(value: DnsServiceConfig) -> Self {
        Self {
            upstream: value.upstream.into(),
            dialer: value.dialer.value,
            cache_capacity: value.cache_capacity.0,
            timeout: value.timeout.value,
        }
    }
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::DnsListenerConfig), into(config::DnsListenerConfig))]
#[ts(as = "config::DnsListenerConfig")]
pub(crate) struct DnsListenerConfig {
    pub listen: SocketAddr,
    pub protocols: Vec<TransportProtocol>,
    pub dns_handler: Dns,
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

#[derive(FromJs, IntoJs, TS)]
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
#[convert(
    from(config::Hysteria2InboundConfig),
    into(config::Hysteria2InboundConfig)
)]
#[ts(as = "config::Hysteria2InboundConfig")]
pub(crate) struct Hysteria2InboundConfig {
    pub listen: SocketAddr,
    pub password: String,
    /// PEM certificate chain, leaf first.
    pub certificate: String,
    /// PEM private key matching the leaf certificate.
    pub private_key: String,
    pub obfs_password: Option<String>,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(
    from(config::Hysteria2OutboundConfig),
    into(config::Hysteria2OutboundConfig)
)]
#[ts(as = "config::Hysteria2OutboundConfig")]
pub(crate) struct Hysteria2OutboundConfig {
    pub server: Target,
    pub password: String,
    /// TLS server name; defaults to the configured server's domain or IP.
    pub server_name: Option<String>,
    /// Additional trusted PEM CA certificates. Public roots remain enabled.
    pub ca_certificate: Option<String>,
    pub obfs_password: Option<String>,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::TlsClientConfig), into(config::TlsClientConfig))]
#[ts(as = "config::TlsClientConfig")]
pub(crate) struct TlsClientConfig {
    pub server_name: Option<String>,
    pub certificate: Option<String>,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::TlsServerConfig), into(config::TlsServerConfig))]
#[ts(as = "config::TlsServerConfig")]
pub(crate) struct TlsServerConfig {
    pub certificate: String,
    pub private_key: String,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::AnyTlsInboundConfig), into(config::AnyTlsInboundConfig))]
#[ts(as = "config::AnyTlsInboundConfig")]
pub(crate) struct AnyTlsInboundConfig {
    pub listen: SocketAddr,
    pub password: String,
    pub tls: TlsServerConfig,
    pub padding_scheme: Option<String>,
}

#[derive(FromJs, TS, StructuralConvert)]
#[convert(from(config::AnyTlsOutboundConfig), into(config::AnyTlsOutboundConfig))]
#[ts(as = "config::AnyTlsOutboundConfig")]
pub(crate) struct AnyTlsOutboundConfig {
    pub server: Target,
    pub password: String,
    pub tls: TlsClientConfig,
    pub idle_session_timeout: Option<Timeout>,
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
