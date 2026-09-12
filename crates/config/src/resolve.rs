use std::num::NonZeroU64;

// Receives a domain name and returns a Vec<IpAddr>.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ts_rs::TS)]
#[ts(rename = "Resolve")]
#[ts(type = "{ readonly __brand: unique symbol }")]
pub struct ResolveHandlerId(pub NonZeroU64);
