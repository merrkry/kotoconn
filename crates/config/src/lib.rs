mod dns;
mod flow;
mod resource;
mod route;
mod target;

pub use dns::{DnsAnswer, ResolvedAddress};
pub use flow::{DomainFact, DomainSource, Flow, TransportProtocol};
pub use resource::{
    Config, OutboundConfig, OutboundId, ResolverConfig, ResolverId, ResolverTransport,
    RuleSetConfig, RuleSetId,
};
pub use route::RouteDecision;
pub use target::Target;
