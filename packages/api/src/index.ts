import { kotoconn as k } from "@kotoconn/bindings";
import type {
  Dialer,
  DnsHandler,
  DnsUpstreamConfig,
  IpAddr,
  Request,
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

export interface DnsService {
  readonly query: (request: Request) => Promise<Response>;
  readonly handler: DnsHandler;
  /**
   * A and AAAA addresses of a name. NXDOMAIN and NODATA give no addresses; other response
   * codes and query errors fail that query. When one query fails and the other gives
   * addresses, they are returned and the failure is logged as a warning; otherwise a failure
   * rejects. Usable directly as a resolve handler.
   */
  readonly resolve: (name: string) => Promise<IpAddr[]>;
}

/** Register one upstream service. Querying is available after configuration loads. */
export function dns_service(options: DnsServiceOptions): DnsService {
  if (options.cache !== undefined && options.cache !== false) {
    const capacity = options.cache.capacity;
    if (!Number.isInteger(capacity) || capacity <= 0 || capacity > 0xffff_ffff) {
      throw new RangeError("DNS cache capacity must be a positive 32-bit integer");
    }
  }

  const service = k.dns_service({
    upstream: options.upstream,
    dialer: options.dialer,
    cache_capacity: options.cache === false ? 0 : (options.cache?.capacity ?? 4096),
    timeout: options.timeout ?? k.timeout(5000),
  });

  const query = (request: Request) => k.dns_query(service, request);
  const handler: DnsHandler = async (request) => k.respond(await query(request));

  const lookup = async (name: string, type: "A" | "AAAA") => {
    const response = await query(k.dns_request(name, type));
    if (response.code !== "NOERROR" && response.code !== "NXDOMAIN") {
      throw new Error(`${type} query for ${name} returned ${response.code}`);
    }

    return response.answers.flatMap((record) =>
      record.type === "A" || record.type === "AAAA" ? [record.data] : [],
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

  return Object.freeze({ query, handler, resolve });
}
