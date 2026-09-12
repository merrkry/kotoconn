use std::num::NonZeroU64;

// Receives a domain name and returns a Vec<IpAddr>.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResolveHandlerId(pub NonZeroU64);
