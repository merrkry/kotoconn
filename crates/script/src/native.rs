//! Native values remain native; TypeScript cannot manufacture resource references.

use kotoconn_config as config;
use rquickjs::{Ctx, FromJs, Result, Value};
use ts_rs::TS;

#[duplicate::duplicate_item(
    Wrapper Native TypeScript;
    [Dialer] [config::DialerId] [ts(as = "config::DialerId")];
    [Inbound] [config::InboundId] [ts(as = "config::InboundId")];
    [Routing] [config::RoutingHandlerId] [ts(as = "config::RoutingHandlerId")];
    [Resolve] [config::ResolveHandlerId] [ts(as = "config::ResolveHandlerId")];
    [Dns] [config::DnsHandlerId] [ts(as = "config::DnsHandlerId")];
    [Timeout] [std::time::Duration] [ts(type = "{ readonly __brand: unique symbol }")];
    [Request] [config::DnsRequest] [ts(type = "{ readonly __brand: unique symbol }")];
    [Response] [config::DnsResponse] [ts(type = "{ readonly __brand: unique symbol }")];
    [InboundImpl] [config::InboundImpl] [ts(as = "config::InboundImpl")];
    [OutboundImpl] [config::OutboundImpl] [ts(as = "config::OutboundImpl")];
    [Decision] [config::RouteDecision] [ts(as = "config::RouteDecision")];
    [DnsResult] [config::DnsHandlerResult] [ts(as = "config::DnsHandlerResult")];
)]
#[derive(
    Clone, rquickjs::class::Trace, rquickjs::JsLifetime, derive_more::From, derive_more::Into, TS,
)]
#[rquickjs::class]
#[TypeScript]
pub(crate) struct Wrapper {
    #[qjs(skip_trace)]
    pub value: Native,
}

#[derive(
    Clone, rquickjs::class::Trace, rquickjs::JsLifetime, derive_more::From, derive_more::Into, TS,
)]
#[rquickjs::class]
#[ts(
    type = "({ readonly version: 4 } | { readonly version: 6 }) & { equals(other: IpAddr): boolean; toString(): string; readonly __brand: unique symbol }"
)]
pub(crate) struct IpAddr {
    #[qjs(skip_trace)]
    pub value: config::IpAddr,
}

#[rquickjs::methods]
impl IpAddr {
    #[qjs(get)]
    pub fn version(&self) -> u8 {
        if self.value.is_ipv4() { 4 } else { 6 }
    }

    pub fn equals(&self, other: Self) -> bool {
        self.value == other.value
    }

    #[qjs(rename = "toString")]
    pub fn display(&self) -> String {
        self.value.to_string()
    }
}

#[derive(
    Clone, rquickjs::class::Trace, rquickjs::JsLifetime, derive_more::From, derive_more::Into, TS,
)]
#[rquickjs::class]
#[ts(as = "config::Target")]
pub(crate) struct Target {
    #[qjs(skip_trace)]
    pub value: config::Target,
}

#[rquickjs::methods]
impl Target {
    #[qjs(get)]
    pub fn port(&self) -> u16 {
        self.value.port()
    }

    #[qjs(get)]
    pub fn domain(&self) -> Option<String> {
        match &self.value {
            config::Target::Domain { name, .. } => Some(name.clone()),
            config::Target::Ip { .. } => None,
        }
    }

    #[qjs(get)]
    pub fn ip(&self) -> Option<IpAddr> {
        match self.value {
            config::Target::Ip { address, .. } => Some(address.into()),
            config::Target::Domain { .. } => None,
        }
    }
}

// QuickJS integer conversions truncate, so validate configuration numbers first.
#[duplicate::duplicate_item(Wrapper Native; [Port] [u16]; [Milliseconds] [u32]; [Byte] [u8];)]
mod checked_number {
    use super::*;

    #[derive(Clone, derive_more::From, derive_more::Into, TS)]
    #[ts(type = "number")]
    pub(crate) struct Wrapper(pub Native);

    impl<'js> FromJs<'js> for Wrapper {
        fn from_js(_: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
            let number = value
                .as_number()
                .ok_or_else(|| invalid("expected a number"))?;
            if !number.is_finite()
                || number.fract() != 0.0
                || number < 0.0
                || number > Native::MAX as f64
            {
                return Err(invalid("integer out of range"));
            }
            Ok(Self(number as Native))
        }
    }
}
pub(crate) use checked_number_byte::Byte;
pub(crate) use checked_number_milliseconds::Milliseconds;
pub(crate) use checked_number_port::Port;

pub(crate) fn invalid(message: impl Into<String>) -> rquickjs::Error {
    rquickjs::Error::new_from_js_message("value", "native value", message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rquickjs::{Context, Runtime};

    #[test]
    fn unsigned_numbers_reject_coercion_truncation_and_overflow() {
        let runtime = Runtime::new().unwrap();
        let context = Context::full(&runtime).unwrap();
        context.with(|ctx| {
            assert_eq!(ctx.eval::<Port, _>("65535").unwrap().0, u16::MAX);
            assert_eq!(ctx.eval::<Byte, _>("255").unwrap().0, u8::MAX);
            assert_eq!(
                ctx.eval::<Milliseconds, _>("4294967295").unwrap().0,
                u32::MAX
            );
            for source in ["-1", "1.5", "65536", "NaN", "Infinity", "'80'", "null"] {
                assert!(ctx.eval::<Port, _>(source).is_err(), "{source}");
            }
        });
    }
}
