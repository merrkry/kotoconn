import { kotoconn as k } from "@kotoconn/bindings";
import type {
  Dialer,
  DnsHandler,
  DnsUpstreamConfig,
  IpAddr,
  Request,
  Resource,
  Response,
  Timeout,
} from "@kotoconn/bindings";

export interface DnsServiceOptions {
  upstream: DnsUpstreamConfig;
  dialer: Dialer;
  /** TTL cache capacity in responses. Defaults to 4096; false disables caching. */
  cache?: false | { capacity: number };
  /** Total upstream query deadline. Defaults to 5000 milliseconds. */
  timeout?: Timeout;
}

/** A native DNS service; closing it ends its queries and cached state. */
export interface DnsService extends Resource {
  readonly query: (request: Request) => Promise<Response>;
  readonly handler: DnsHandler;
  /**
   * A and AAAA addresses of a name and the aliases its CNAME chain reaches; records for
   * other owners are ignored. NXDOMAIN and NODATA give no addresses; other response
   * codes and query errors fail that query. When one query fails and the other gives
   * addresses, they are returned and the failure is logged as a warning; otherwise a failure
   * rejects. Usable directly as a resolve handler.
   */
  readonly resolve: (name: string) => Promise<IpAddr[]>;
}

/** Create one upstream service. */
export async function dns_service(options: DnsServiceOptions): Promise<DnsService> {
  if (options.cache !== undefined && options.cache !== false) {
    const capacity = options.cache.capacity;
    if (!Number.isInteger(capacity) || capacity <= 0 || capacity > 0xffff_ffff) {
      throw new RangeError("DNS cache capacity must be a positive 32-bit integer");
    }
  }

  const service = await k.dns_service({
    upstream: options.upstream,
    dialer: options.dialer,
    cache_capacity: options.cache === false ? 0 : (options.cache?.capacity ?? 4096),
    timeout: options.timeout ?? k.timeout(5000),
  });

  const query = (request: Request) => k.dns_query(service, request);
  const handler: DnsHandler = async (request) => k.respond(await query(request));

  const lookup = async (name: string, type: "A" | "AAAA") => {
    const request = k.dns_request(name, type);
    const response = await query(request);
    if (response.code !== "NOERROR" && response.code !== "NXDOMAIN") {
      throw new Error(`${type} query for ${name} returned ${response.code}`);
    }

    // Addresses count only for the queried name and the aliases its CNAME chain reaches.
    const aliases = new Map(
      response.answers.flatMap((record) =>
        record.type === "CNAME" ? [[record.name, record.data] as const] : [],
      ),
    );
    const chain = new Set<string>();
    for (
      let owner: string | undefined = request.query.name;
      owner !== undefined && !chain.has(owner);
      owner = aliases.get(owner)
    ) {
      chain.add(owner);
    }

    return response.answers.flatMap((record) =>
      (record.type === "A" || record.type === "AAAA") &&
      record.type === type &&
      chain.has(record.name)
        ? [record.data]
        : [],
    );
  };

  const resolve = async (name: string) => {
    const results = await Promise.allSettled([lookup(name, "A"), lookup(name, "AAAA")]);
    const addresses = results.flatMap((result) =>
      result.status === "fulfilled" ? result.value : [],
    );
    const failures = results.flatMap((result) =>
      result.status === "rejected"
        ? [result.reason instanceof Error ? result.reason.message : String(result.reason)]
        : [],
    );

    if (failures.length > 0 && addresses.length === 0) {
      throw new Error(`DNS resolution of ${name} failed: ${failures.join("; ")}`);
    }

    for (const failure of failures) {
      k.log("warn", `DNS resolution of ${name} ignored a failure: ${failure}`);
    }
    return addresses;
  };

  return Object.freeze({
    query,
    handler,
    resolve,
    close: () => service.close(),
    drain: (options: { timeout: Timeout }) => service.drain(options),
    wait: () => service.wait(),
  });
}
