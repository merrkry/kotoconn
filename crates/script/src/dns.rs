//! DNS messages exchanged with policy: request construction and read-only message views.

use crate::native::{Inbound, IpAddr, TypeCode, invalid};
use hickory_proto::{
    op::{Message, Query},
    rr::{self, Name, RData, rdata},
    serialize::binary::{BinEncodable, BinEncoder, NameEncoding},
};
use kotoconn_config as config;
use rquickjs::{Ctx, FromJs, IntoJs, Object, Result, Value};
use ts_rs::TS;

/// Record types whose data is a normalized domain name.
const NAME_TYPES: [rr::RecordType; 4] = [
    rr::RecordType::ANAME,
    rr::RecordType::CNAME,
    rr::RecordType::NS,
    rr::RecordType::PTR,
];

/// Mnemonics of the RFC 1035 and RFC 2136 response codes, indexed by code.
const RESPONSE_CODE_NAMES: [&str; 11] = [
    "NOERROR", "FORMERR", "SERVFAIL", "NXDOMAIN", "NOTIMP", "REFUSED", "YXDOMAIN", "YXRRSET",
    "NXRRSET", "NOTAUTH", "NOTZONE",
];

// These types have TypeScript declarations derived from hickory and the tables above.
macro_rules! declared {
    ($type:ident, $cfg:ident => $inline:expr) => {
        impl TS for $type {
            type WithoutGenerics = Self;
            type OptionInnerType = Self;

            fn name(_: &ts_rs::Config) -> String {
                stringify!($type).into()
            }

            fn inline($cfg: &ts_rs::Config) -> String {
                $inline
            }

            fn decl(cfg: &ts_rs::Config) -> String {
                format!("type {} = {};", <Self as TS>::name(cfg), Self::inline(cfg))
            }

            fn decl_concrete(cfg: &ts_rs::Config) -> String {
                Self::decl(cfg)
            }
        }
    };
}

/// Builds an IN-class query with recursion desired and a random message ID.
pub(crate) fn request(name: &str, record_type: RecordType) -> Result<config::DnsRequest> {
    if name.is_empty() {
        return Err(invalid("DNS name must not be empty"));
    }

    let mut name = name
        .parse::<Name>()
        .map_err(|e| invalid(format!("invalid DNS name: {e}")))?;
    name.set_fqdn(true);

    let mut message = Message::query();
    message.metadata.recursion_desired = true;
    message.queries.push(Query::query(name, record_type.0));
    Ok(message.into())
}

/// Lowercase without the final dot, except for the root `.`.
fn view_name(name: &Name) -> String {
    let name = name.to_lowercase().to_utf8();
    match name.strip_suffix('.') {
        Some(stripped) if !stripped.is_empty() => stripped.to_owned(),
        _ => name,
    }
}

/// Known types by name, others by number.
#[derive(Clone, Copy)]
pub(crate) struct RecordType(pub rr::RecordType);

impl RecordType {
    /// Names that hickory both prints and parses, so policy can pass every name it reads.
    fn known_name(self) -> Option<&'static str> {
        let name: &'static str = self.0.into();
        (name.parse::<rr::RecordType>().ok() == Some(self.0)).then_some(name)
    }

    fn names() -> impl Iterator<Item = &'static str> {
        (0..=u16::MAX).filter_map(|code| Self(code.into()).known_name())
    }
}

declared!(RecordType, _cfg => RecordType::names()
    .map(|name| format!("\"{name}\""))
    .chain(["number".into()])
    .collect::<Vec<_>>()
    .join(" | "));

impl<'js> FromJs<'js> for RecordType {
    fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
        if !value.is_string() {
            return Ok(Self(TypeCode::from_js(ctx, value)?.0.into()));
        }

        let name = String::from_js(ctx, value)?;
        name.parse::<rr::RecordType>()
            .ok()
            .map(Self)
            .filter(|record_type| record_type.known_name() == Some(name.as_str()))
            .ok_or_else(|| invalid(format!("unknown DNS record type name: {name}")))
    }
}

impl<'js> IntoJs<'js> for RecordType {
    fn into_js(self, ctx: &Ctx<'js>) -> Result<Value<'js>> {
        match self.known_name() {
            Some(name) => name.into_js(ctx),
            None => u16::from(self.0).into_js(ctx),
        }
    }
}

/// Known response codes by mnemonic, others by number.
pub(crate) struct ResponseCode(pub hickory_proto::op::ResponseCode);

declared!(ResponseCode, _cfg => RESPONSE_CODE_NAMES
    .iter()
    .map(|name| format!("\"{name}\""))
    .chain(["number".into()])
    .collect::<Vec<_>>()
    .join(" | "));

impl<'js> IntoJs<'js> for ResponseCode {
    fn into_js(self, ctx: &Ctx<'js>) -> Result<Value<'js>> {
        let code = u16::from(self.0);
        match RESPONSE_CODE_NAMES.get(usize::from(code)) {
            Some(name) => name.into_js(ctx),
            None => code.into_js(ctx),
        }
    }
}

enum RecordData {
    Address(IpAddr),
    Name(String),
    /// Uncompressed RDATA wire bytes.
    Bytes(Vec<u8>),
}

pub(crate) struct DnsRecord {
    name: String,
    record_type: RecordType,
    class: u16,
    ttl: u32,
    data: RecordData,
}

declared!(DnsRecord, cfg => {
    let names = NAME_TYPES
        .map(|record_type| {
            // SAFETY: every name-valued type is a named hickory record type.
            let name = RecordType(record_type).known_name().expect("named record type");
            format!("\"{name}\"")
        })
        .join(" | ");

    format!(
        "{{ readonly name: string; readonly class: number; readonly ttl: number }} & (\
         {{ readonly type: \"A\"; readonly data: Extract<IpAddr, {{ readonly version: 4 }}> }} | \
         {{ readonly type: \"AAAA\"; readonly data: Extract<IpAddr, {{ readonly version: 6 }}> }} | \
         {{ readonly type: {names}; readonly data: string }} | \
         {{ readonly type: Exclude<{}, \"A\" | \"AAAA\" | {names}>; readonly data: readonly number[] }})",
        <RecordType as TS>::name(cfg),
    )
});

impl TryFrom<&rr::Record> for DnsRecord {
    type Error = rquickjs::Error;

    fn try_from(record: &rr::Record) -> Result<Self> {
        let record_type = record.record_type();

        let data = match &record.data {
            RData::A(rdata::A(address)) => RecordData::Address(config::IpAddr::V4(*address).into()),
            RData::AAAA(rdata::AAAA(address)) => {
                RecordData::Address(config::IpAddr::V6(*address).into())
            }
            RData::ANAME(rdata::ANAME(name))
            | RData::CNAME(rdata::CNAME(name))
            | RData::NS(rdata::NS(name))
            | RData::PTR(rdata::PTR(name)) => {
                debug_assert!(NAME_TYPES.contains(&record_type));
                RecordData::Name(view_name(name))
            }
            data => {
                // hickory decodes empty RDATA of any type without validating it.
                if matches!(record_type, rr::RecordType::A | rr::RecordType::AAAA)
                    || NAME_TYPES.contains(&record_type)
                {
                    return Err(invalid(format!("malformed DNS {record_type} record data")));
                }

                let mut bytes = Vec::new();
                let mut encoder = BinEncoder::new(&mut bytes);
                encoder.set_name_encoding(NameEncoding::Uncompressed);
                data.emit(&mut encoder)
                    .map_err(|e| invalid(format!("DNS record data: {e}")))?;
                RecordData::Bytes(bytes)
            }
        };

        Ok(Self {
            name: view_name(&record.name),
            record_type: RecordType(record_type),
            class: record.dns_class.into(),
            ttl: record.ttl,
            data,
        })
    }
}

impl<'js> IntoJs<'js> for DnsRecord {
    fn into_js(self, ctx: &Ctx<'js>) -> Result<Value<'js>> {
        let view = Object::new(ctx.clone())?;
        view.prop("name", self.name)?;
        view.prop("type", self.record_type)?;
        view.prop("class", self.class)?;
        view.prop("ttl", self.ttl)?;

        match self.data {
            RecordData::Address(address) => view.prop("data", address)?,
            RecordData::Name(name) => view.prop("data", name)?,
            RecordData::Bytes(bytes) => view.prop("data", bytes)?,
        };
        Ok(view.into_value())
    }
}

#[derive(Clone, rquickjs::class::Trace, rquickjs::JsLifetime, TS)]
#[rquickjs::class]
#[ts(
    type = "{ readonly query: { readonly name: string; readonly type: RecordType; readonly class: number }; readonly inbound: Inbound | undefined; readonly source: SocketAddr | undefined; readonly protocol: TransportProtocol | undefined; readonly __brand: unique symbol }"
)]
pub(crate) struct Request {
    #[qjs(skip_trace)]
    pub value: config::DnsRequest,
    #[qjs(skip_trace)]
    pub info: Option<config::DnsRequestInfo>,
}

impl From<config::DnsRequest> for Request {
    fn from(value: config::DnsRequest) -> Self {
        Self { value, info: None }
    }
}

#[rquickjs::methods]
impl Request {
    #[qjs(get)]
    pub fn query<'js>(&self, ctx: Ctx<'js>) -> Result<Object<'js>> {
        let [query] = self.value.queries.as_slice() else {
            return Err(invalid("expected exactly one DNS question"));
        };

        let view = Object::new(ctx.clone())?;
        view.prop("name", view_name(&query.name))?;
        view.prop("type", RecordType(query.query_type))?;
        view.prop("class", u16::from(query.query_class))?;
        Ok(view)
    }

    #[qjs(get)]
    pub fn inbound(&self) -> Option<Inbound> {
        self.info.map(|info| info.inbound.into())
    }

    #[qjs(get)]
    pub fn source(&self) -> Option<crate::data::SocketAddr> {
        self.info.map(|info| info.source.into())
    }

    #[qjs(get)]
    pub fn protocol(&self) -> Option<crate::data::TransportProtocol> {
        self.info.map(|info| info.protocol.into())
    }
}

#[derive(
    Clone, rquickjs::class::Trace, rquickjs::JsLifetime, derive_more::From, derive_more::Into, TS,
)]
#[rquickjs::class]
#[ts(
    type = "{ readonly code: ResponseCode; readonly answers: readonly DnsRecord[]; readonly __brand: unique symbol }"
)]
pub(crate) struct Response {
    #[qjs(skip_trace)]
    pub value: config::DnsResponse,
}

#[rquickjs::methods]
impl Response {
    #[qjs(get)]
    pub fn code(&self) -> ResponseCode {
        ResponseCode(self.value.metadata.response_code)
    }

    #[qjs(get)]
    pub fn answers(&self) -> Result<Vec<DnsRecord>> {
        self.value.answers.iter().map(DnsRecord::try_from).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hickory_proto::{
        op::{OpCode, ResponseCode},
        rr::{
            Record,
            rdata::{MX, SOA},
        },
    };
    use rquickjs::{Context, Runtime};

    fn name(text: &str) -> Name {
        Name::from_ascii(text).unwrap()
    }

    fn response(answers: Vec<Record>) -> Response {
        let mut message = Message::response(7, OpCode::Query);
        message.metadata.response_code = ResponseCode::NXDomain;
        message.answers = answers;
        config::DnsResponse::from_message(message).unwrap().into()
    }

    #[test]
    fn policy_requests_are_recursive_internet_queries() {
        let runtime = Runtime::new().unwrap();
        let context = Context::full(&runtime).unwrap();
        context.with(|ctx| {
            let parsed = |source: &str| ctx.eval::<RecordType, _>(source).map(|t| t.0);
            assert_eq!(parsed("'AAAA'").unwrap(), rr::RecordType::AAAA);
            assert_eq!(parsed("28").unwrap(), rr::RecordType::AAAA);
            assert_eq!(parsed("65280").unwrap(), rr::RecordType::Unknown(65280));
            for source in ["'aaaa'", "'*'", "'IXFR'", "65536", "1.5"] {
                assert!(parsed(source).is_err(), "{source}");
            }

            let wire = request("WWW.Example.com", RecordType(rr::RecordType::AAAA)).unwrap();
            let [query] = wire.queries.as_slice() else {
                panic!("one question")
            };
            assert!(wire.metadata.recursion_desired);
            assert!(query.name.is_fqdn());
            assert_eq!(query.query_class, rr::DNSClass::IN);
            assert!(Message::from_vec(&wire.to_vec().unwrap()).is_ok());
            assert!(request("", RecordType(rr::RecordType::A)).is_err());

            let unknown = request(".", RecordType(rr::RecordType::Unknown(65280))).unwrap();
            ctx.globals().set("named", Request::from(wire)).unwrap();
            ctx.globals().set("unknown", Request::from(unknown)).unwrap();
            ctx.eval::<(), _>(
                r#"
                'use strict';
                const {query} = named;
                if (query.name !== 'www.example.com' || query.type !== 'AAAA' || query.class !== 1) throw Error('named query');
                if (named.inbound !== undefined || named.source !== undefined || named.protocol !== undefined) throw Error('listener context');
                if (unknown.query.name !== '.' || unknown.query.type !== 65280) throw Error('unknown query');
            "#,
            )
            .unwrap();
        });
    }

    #[test]
    fn answer_views_expose_addresses_names_and_uncompressed_data() {
        let runtime = Runtime::new().unwrap();
        let context = Context::full(&runtime).unwrap();
        context.with(|ctx| {
            let answers = vec![
                Record::from_rdata(
                    name("WWW.Example.com."),
                    300,
                    RData::CNAME(rdata::CNAME(name("Edge.Example.NET."))),
                ),
                Record::from_rdata(
                    name("edge.example.net."),
                    60,
                    RData::A(rdata::A([192, 0, 2, 1].into())),
                ),
                Record::from_rdata(
                    name("edge.example.net."),
                    60,
                    RData::AAAA(rdata::AAAA("2001:db8::1".parse().unwrap())),
                ),
                Record::from_rdata(
                    name("example.com."),
                    60,
                    RData::MX(MX::new(10, name("mail.example.com."))),
                ),
                // A compressing encoder would replace the shared suffix with a pointer.
                Record::from_rdata(
                    name("example."),
                    60,
                    RData::SOA(SOA::new(name("ns.example."), name("h.example."), 1, 2, 3, 4, 5)),
                ),
            ];
            ctx.globals().set("response", response(answers)).unwrap();
            ctx.eval::<(), _>(
                r#"
                'use strict';
                if (response.code !== 'NXDOMAIN') throw Error('code');
                const [alias, v4, v6, mx, soa] = response.answers;
                if (alias.name !== 'www.example.com' || alias.type !== 'CNAME' || alias.data !== 'edge.example.net') throw Error('cname');
                if (alias.ttl !== 300 || alias.class !== 1) throw Error('cname metadata');
                if (v4.type !== 'A' || v4.data.version !== 4 || v4.data.toString() !== '192.0.2.1') throw Error('a');
                if (v6.type !== 'AAAA' || v6.data.version !== 6 || v6.data.toString() !== '2001:db8::1') throw Error('aaaa');
                const bytes = (record) => JSON.stringify(record.data);
                const mail = [4, 109, 97, 105, 108, 7, 101, 120, 97, 109, 112, 108, 101, 3, 99, 111, 109, 0];
                if (mx.type !== 'MX' || bytes(mx) !== JSON.stringify([0, 10, ...mail])) throw Error('mx');
                const names = [2, 110, 115, 7, 101, 120, 97, 109, 112, 108, 101, 0, 1, 104, 7, 101, 120, 97, 109, 112, 108, 101, 0];
                const counters = [0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0, 0, 0, 5];
                if (bytes(soa) !== JSON.stringify([...names, ...counters])) throw Error('soa');
            "#,
            )
            .unwrap();
        });
    }

    #[test]
    fn empty_address_data_is_rejected_instead_of_mistyped() {
        let runtime = Runtime::new().unwrap();
        let context = Context::full(&runtime).unwrap();
        context.with(|ctx| {
            // hickory decodes a zero-length RDATA of any type into this variant.
            let empty = Record::from_rdata(name("example."), 60, RData::Update0(rr::RecordType::A));
            ctx.globals()
                .set("response", response(vec![empty]))
                .unwrap();
            assert!(ctx.eval::<(), _>("response.answers").is_err());
        });
    }
}
