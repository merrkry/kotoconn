# Kotoconn

Kotoconn is a programmable proxy. User policy controls routing and name resolution while native protocol implementations carry the traffic.

## Language

**Policy**:
The user's code that creates resources and the handlers that control routing, name resolution, and DNS responses. Its handlers share the same user state.

**Resource**:
An inbound, dialer, DNS service, or DNS listener that policy creates and closes. Handlers are not resources.

**Inbound**:
An entry point for client traffic, such as a proxy server, TUN interface, or direct listener.

**Outbound**:
A client-side protocol configuration used to carry traffic, including direct forwarding.
_Avoid_: Dialer, when referring only to protocol-specific settings.

**Dialer**:
A connection configuration combining an outbound with its carrier and address-resolution policy. A chain of carriers ends at the host network.

**Client**:
A stateful runtime entry that provides TCP or UDP outbound connections. The entries for one dialer can share a protocol instance and can be closed independently.

**Carrier**:
What carries a dialer's connections: another dialer, or the host network with its socket options. A carrier offers TCP, UDP or both, and either accepts domain targets or requires IP targets.

**Session**:
A logical connection established through an inbound and routed independently. A multiplexed inbound connection can carry multiple sessions. UDP sessions are separated by association and original destination, and keep one routing decision until they end.

**Routing handler**:
A user policy function that selects a dialer or rejects traffic. TCP routing can also select a replacement target; UDP routing keeps the original destination.

**Resolve handler**:
A user policy function that resolves a domain name to a list of IP addresses for routing or connection establishment.
_Avoid_: DNS server, DNS handler.

**DNS handler**:
A user policy function that answers a received DNS request with a DNS response or drops it without responding.

**DNS service**:
A reusable query service configured with an upstream and a dialer. DNS handlers and resolve handlers can call and compose services, each with its own forwarding and caching behavior.

**Destination**:
The client's requested endpoint. Routing and resolution do not change it.

**Target**:
The endpoint used for a connection attempt. TCP routing can replace the destination; UDP always uses the original destination.

**Flow**:
The traffic described to a routing policy by its transport protocol, destination, and optional sniff result.

**UDP idle timeout**:
The inactivity period after which an inbound's client UDP session expires. It is distinct from the lifetime of a carrier connection.
