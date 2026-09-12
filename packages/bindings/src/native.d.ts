import type { IpAddr, Kotoconn } from "./generated.js";

export type IPv4 = Extract<IpAddr, { version: 4 }>;

export type IPv6 = Extract<IpAddr, { version: 6 }>;

export type RoutingHandler = Parameters<Kotoconn["routing_handler"]>[0];

export type ResolveHandler = Parameters<Kotoconn["resolve_handler"]>[0];

export type DnsHandler = Parameters<Kotoconn["dns_handler"]>[0];
