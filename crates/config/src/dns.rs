use std::num::NonZeroU64;

pub use hickory_proto::op::{DnsRequest, DnsResponse};

// Receives a DnsRequest and returns a DnsHandlerResult.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DnsHandlerId(pub NonZeroU64);

// A handler exception is an execution error, not Drop.
#[derive(Debug, Clone)]
pub enum DnsHandlerResult {
    Response(DnsResponse),
    Drop,
}
