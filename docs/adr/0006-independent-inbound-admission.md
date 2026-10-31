# Admit inbound connections independently of outbound establishment

An inbound handshake admits a connection without waiting for routing or outbound availability. A later rejection or connection failure closes it. Inbound payload is therefore available for sniffing before outbound selection, without a handshake dependency cycle.

Each adapter terminates its own transport. The daemon exchanges byte streams or complete datagrams between adapters. It never pairs connections or passes transport feedback such as RTT, congestion windows or receive windows across adapters. Backpressure, errors and cancellation propagate through the stream and datagram contracts. TUN terminates TCP in user space; socket-based inbounds use kernel transports.

TCP preserves half-close when the protocol can represent it. A protocol whose wire format cannot, such as AnyTLS, follows its own contract.

Each session progresses independently of supervision and of other sessions. Shared mutable protocol state has one owner coordinated by messages, so no lock is held across a wait. Stopping admission can fail handshakes that still await policy; shutdown does not promise to route every accepted connection.
