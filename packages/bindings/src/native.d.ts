import type { IpAddr } from "./generated.js";

export type IPv4 = Extract<IpAddr, { version: 4 }>;

export type IPv6 = Extract<IpAddr, { version: 6 }>;
