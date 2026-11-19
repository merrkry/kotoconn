import { kotoconn as k, type SessionInfo, type Inbound } from "../src/index.js";

export async function matches(
  flow: SessionInfo,
  inbound: Inbound,
  domain: string,
): Promise<boolean> {
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

  const outbound = await k.outbound({
    via: k.host_network({}),
    resolve_server: () => [],
    protocol: k.direct_outbound({}),
  });

  const pending = k.outbound({
    via: outbound,
    resolve_server: () => [],
    protocol: k.direct_outbound({}),
  });
  // @ts-expect-error Creation resolves to a reference instead of returning one.
  k.route(pending, flow.destination);
  outbound.drain({ timeout: k.timeout(1000) });
  await outbound.wait();

  // @ts-expect-error Inbound references are distinct from outbound references.
  flow.inbound.equals(outbound);

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
