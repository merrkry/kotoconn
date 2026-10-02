import { kotoconn as k, type Flow, type Inbound } from "../src/index.js";

export function matches(flow: Flow, inbound: Inbound, domain: string): boolean {
  const address = flow.source.address;
  const network = k.cidr("192.168.0.0/16");
  const flags: boolean[] = [
    address.is_private(),
    address.is_loopback(),
    address.is_link_local(),
    address.is_multicast(),
    address.is_unspecified(),
  ];
  const label: string = network.toString();

  // @ts-expect-error CIDRs accept native IP values rather than strings.
  network.contains("192.168.0.1");

  const dialer = k.dialer({
    dialer: null,
    outbound: {
      resolve_handler: k.resolve_handler(() => []),
      implementation: k.direct_outbound({}),
    },
  });

  // @ts-expect-error Inbound references are distinct from dialer references.
  flow.inbound.equals(dialer);

  // @ts-expect-error Native references cannot be manufactured from an ID.
  const fake: Inbound = 1;

  return (
    flow.inbound.equals(inbound) &&
    flow.source.port > 0 &&
    (flags.some(Boolean) ||
      network.contains(address) ||
      k.domain_suffix(domain, "example.com") ||
      k.is_subdomain(domain, "example.net")) &&
    label.length > 0 &&
    !flow.inbound.equals(fake)
  );
}
