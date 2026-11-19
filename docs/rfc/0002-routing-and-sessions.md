# RFC 0002. Routing and session semantics

## Inbounds and outbounds

An **inbound** accepts client traffic, for example through a proxy listener
or TUN interface.

An **outbound** sends traffic through the host network with its own socket
settings or through a remote proxy. It can use another outbound to reach
its remote endpoint, forming a chain that ends at the host network.

## Admit sessions independently of routing

A **session** is the unit of traffic that policy routes. TCP traffic is
routed per stream. One inbound connection can carry several streams.

Kotoconn admits inbound sessions independently of routing and outbound
establishment. Policy can route on inspected payload even when a client
waits for the inbound handshake before sending its first bytes.

Each adapter terminates its own transport, including TCP on a TUN inbound.
Adapters exchange byte streams or datagrams and handle transport feedback
on their own leg.

## Route sessions

Within a UDP association, each original destination has its own session.
The first datagram triggers routing, and the decision lasts until that
session ends. Other sessions stay usable when one expires. Shared UDP
ingress can drop datagrams under load so one stalled session cannot block
the others.

## Abort or drain

Abort and drain differ in what happens to work already accepted. An **abort**
stops work immediately and discards pending traffic. A **drain** stops
accepting new work and lets accepted work finish. The protocol determines
when and how each applies.
