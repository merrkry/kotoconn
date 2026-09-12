# Kotoconn

Kotoconn is a programmable proxy. User policy controls routing and name resolution while native protocol implementations carry the traffic.

## Language

**Policy**:
The user's resource registrations and handlers that control routing, name resolution, and DNS responses. Its handlers share the same user state.

**Inbound**:
An entry point for client traffic, such as a proxy server, TUN interface, or direct listener.

**Outbound**:
A client-side protocol configuration used to carry traffic, including direct forwarding.
_Avoid_: Dialer, when referring only to protocol-specific settings.

**Dialer**:
A connection configuration combining an outbound with its carrier and address-resolution policy. A carrier is another dialer; a chain ends at the I/O layer.

**Routing handler**:
A user policy function that selects a dialer and target, or rejects traffic, for either TCP or UDP.

**Resolve handler**:
A user policy function that resolves a domain name to a list of IP addresses for routing or connection establishment.
_Avoid_: DNS server, DNS handler.

**DNS handler**:
A user policy function that answers a received DNS request with a DNS response or drops it without responding.

**Destination**:
The client's requested endpoint. Routing and resolution do not change it.

**Target**:
The endpoint selected by routing for a connection attempt. It can differ from the client's destination.

**Flow**:
The traffic described to a routing policy by its transport protocol and destination.

**UDP idle timeout**:
The inactivity period after which an inbound's client UDP session expires. It is distinct from the lifetime of a carrier connection.
