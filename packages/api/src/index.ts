import { kotoconn as k } from "@kotoconn/bindings";
import type {
  Dialer,
  DnsHandler,
  DnsUpstreamConfig,
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

  return Object.freeze({ query, handler });
}
