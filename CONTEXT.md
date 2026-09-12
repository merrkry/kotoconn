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
A connection configuration combining an outbound with its carrier and address-resolution policy. A chain ends at the I/O layer.

**Client**:
A stateful runtime entry that provides TCP or UDP outbound connections. The entries for one dialer can share a protocol instance and can be closed independently.

**Carrier**:
The relationship through which a dialer uses another dialer to carry its connections. At runtime, one carrier entry supplies the lower-layer transport capabilities required by a protocol adapter.

**Session**:
A logical connection established through an inbound and routed independently. A multiplexed inbound connection can carry multiple sessions. UDP sessions are separated by association and original destination, and keep one routing decision until they end.

**Routing handler**:
A user policy function that selects a dialer or rejects traffic. TCP routing can also select a replacement target; UDP routing keeps the original destination.

**Resolve handler**:
A user policy function that resolves a domain name to a list of IP addresses for routing or connection establishment.
_Avoid_: DNS server, DNS handler.

**DNS handler**:
A user policy function that answers a received DNS request with a DNS response or drops it without responding.

**Destination**:
The client's requested endpoint. Routing and resolution do not change it.

**Target**:
The endpoint used for a connection attempt. TCP routing can replace the destination; UDP always uses the original destination.

**Flow**:
The traffic described to a routing policy by its transport protocol and destination.

**UDP idle timeout**:
The inactivity period after which an inbound's client UDP session expires. It is distinct from the lifetime of a carrier connection.
