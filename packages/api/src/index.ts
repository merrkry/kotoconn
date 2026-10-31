import { kotoconn as k } from "@kotoconn/bindings";
import type {
  Dialer,
  DialerConfig,
  DnsHandler,
  DnsUpstreamConfig,
  Inbound,
  IpAddr,
  Request,
  Resource,
  Response,
  Target,
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

/** Measures one dialer, resolving to a latency in milliseconds or rejecting when unhealthy. */
export type HealthCheck = (dialer: Dialer) => Promise<number>;

export interface HttpCheckOptions {
  /** The HTTP server, reached through the probed dialer. */
  target: Target;
  /** Defaults to `/generate_204`. */
  path?: string;
  timeout: Timeout;
}

/**
 * A plain HTTP health check: a HEAD request through the dialer, healthy when the
 * status is 2xx or 3xx. The latency includes connecting through the carrier chain.
 */
export function http_check(options: HttpCheckOptions): HealthCheck {
  const { domain, ip, port } = options.target;
  const name = domain ?? (ip?.version === 6 ? `[${ip.toString()}]` : String(ip));
  const host = port === 80 ? name : `${name}:${port}`;
  const request = ascii(
    `HEAD ${options.path ?? "/generate_204"} HTTP/1.1\r\nHost: ${host}\r\nConnection: close\r\n\r\n`,
  );

  return async (dialer) => {
    const result = await k.probe(dialer, {
      target: options.target,
      request,
      // "HTTP/1.1 204", which can arrive in several reads.
      length: 12,
      timeout: options.timeout,
    });
    const status = /^HTTP\/1\.[01] ([0-9]{3})/.exec(
      String.fromCharCode(...result.response.subarray(0, 16)),
    );
    const code = Number(status?.[1]);
    if (!(code >= 200 && code < 400)) {
      throw new Error(`health check returned ${status ? code : "no HTTP status"}`);
    }
    return result.latency;
  };
}

function ascii(text: string): Uint8Array {
  return Uint8Array.from(text, (character) => character.charCodeAt(0));
}

/** Chooses the dialer that routing uses for a set of members. */
export interface Group {
  /** The dialer routing should use now. */
  readonly current: Dialer;
  /** Replace the members, for example after a subscription update. */
  update(dialers: readonly Dialer[]): void;
  /** Stop health checks. Members stay open; closing them is up to their owner. */
  close(): void;
}

export interface Selector extends Group {
  /** Make a member current until the next selection or update. */
  select(dialer: Dialer): void;
}

/** A group whose current member policy picks explicitly; it starts with the first one. */
export function selector(dialers: readonly Dialer[]): Selector {
  let members = nonEmpty(dialers);
  let current = members[0]!;

  return {
    get current() {
      return current;
    },
    update(dialers) {
      members = nonEmpty(dialers);
      if (!members.some((member) => member.equals(current))) current = members[0]!;
    },
    select(dialer) {
      if (!members.some((member) => member.equals(dialer))) {
        throw new Error("dialer is not a member of this group");
      }
      current = dialer;
    },
    close() {},
  };
}

export interface CheckedGroupOptions {
  dialers: readonly Dialer[];
  check: HealthCheck;
  /** Time between check rounds. Rounds start when the group is created and end at shutdown. */
  interval: Timeout;
}

/** A group that picks its member from health checks. */
export interface CheckedGroup extends Group {
  /** Check every member now and pick again. */
  check(): Promise<void>;
}

export interface FastestOptions extends CheckedGroupOptions {
  /** Keep the current member unless another is faster by more than this many milliseconds. */
  tolerance?: number;
}

/** Uses the member with the lowest checked latency; until a round finds one, the first. */
export function fastest(options: FastestOptions): CheckedGroup {
  const tolerance = options.tolerance ?? 0;
  return checked(options, (members, latencies, current) => {
    let best: Dialer | undefined;
    for (const member of members) {
      const latency = latencies.get(member);
      if (latency !== undefined && (best === undefined || latency < latencies.get(best)!)) {
        best = member;
      }
    }
    if (best === undefined) return current;

    const kept = latencies.get(current);
    return kept !== undefined && kept - latencies.get(best)! <= tolerance ? current : best;
  });
}

/** Uses the first member, in order, that passed its last check; the first if none did. */
export function fallback(options: CheckedGroupOptions): CheckedGroup {
  return checked(
    options,
    (members, latencies) => members.find((member) => latencies.has(member)) ?? members[0]!,
  );
}

/** Runs check rounds and picks the current member from the latencies of healthy members. */
function checked(
  options: CheckedGroupOptions,
  pick: (members: readonly Dialer[], latencies: Map<Dialer, number>, current: Dialer) => Dialer,
): CheckedGroup {
  let members = nonEmpty(options.dialers);
  let current = members[0]!;
  let closed = false;
  let rounds = 0;

  const check = async () => {
    const round = ++rounds;
    const checked = members;
    // A check that throws instead of rejecting fails only its own member.
    const results = await Promise.allSettled(checked.map(async (member) => options.check(member)));
    // A later round or an update makes these measurements stale.
    if (round !== rounds || checked !== members) return;

    const latencies = new Map<Dialer, number>();
    results.forEach((result, index) => {
      if (result.status === "fulfilled") latencies.set(checked[index]!, result.value);
    });
    current = pick(members, latencies, current);
  };

  (async () => {
    while (!closed) {
      await check();
      await k.sleep(options.interval);
    }
  })().catch(() => {
    // Shutdown rejects the sleep, which ends the rounds.
  });

  return {
    get current() {
      return current;
    },
    update(dialers) {
      members = nonEmpty(dialers);
      if (!members.some((member) => member.equals(current))) current = members[0]!;
      check().catch((error) => k.log("warn", `group check failed: ${String(error)}`));
    },
    check,
    close() {
      closed = true;
    },
  };
}

function nonEmpty(dialers: readonly Dialer[]): readonly Dialer[] {
  if (dialers.length === 0) throw new Error("a group needs at least one dialer");
  return [...dialers];
}

export interface SubscriptionOptions {
  /**
   * Loads the subscribed dialers. Each key identifies one dialer and must change
   * whenever its configuration changes.
   */
  load: () => Promise<ReadonlyMap<string, DialerConfig>>;
  /** Time between loads. Updates end at shutdown. */
  interval: Timeout;
  /** Receives the open dialers after each change, for example `group.update`. */
  update: (dialers: readonly Dialer[]) => void;
  /** Deadline for dialers that left the subscription to finish their sessions. */
  drain: Timeout;
}

export interface Subscription {
  readonly dialers: readonly Dialer[];
  /** Load and apply the subscription now. */
  refresh(): Promise<void>;
  /** Stop periodic updates. Dialers stay open; drain them through `dialers` if needed. */
  close(): void;
}

/**
 * Keeps a set of dialers in sync with a subscription. Dialers for new keys are created
 * and passed to `update` before the dialers of removed keys drain, so routing moves
 * away from them first. The first load must succeed; later failures, including a
 * load without any usable dialer, are logged and keep the current dialers.
 */
export async function subscription(options: SubscriptionOptions): Promise<Subscription> {
  let dialers = new Map<string, Dialer>();
  let closed = false;
  // Refreshes run one at a time, so each compares against the dialers the last one left.
  let previous = Promise.resolve();

  const refresh = () => {
    const applied = previous.then(apply);
    previous = applied.catch(() => {});
    return applied;
  };

  const apply = async () => {
    const loaded = await options.load();
    const next = new Map<string, Dialer>();
    for (const [key, config] of loaded) {
      const existing = dialers.get(key);
      if (existing) {
        next.set(key, existing);
        continue;
      }
      try {
        next.set(key, await k.dialer(config));
      } catch (error) {
        k.log("warn", `subscription dialer ${key} failed: ${String(error)}`);
      }
    }

    // Routing needs a dialer, so an update without any keeps the current ones.
    if (next.size === 0) throw new Error("subscription has no usable dialers");

    const removed = [...dialers].filter(([key]) => !next.has(key));
    try {
      options.update([...next.values()]);
    } catch (error) {
      // Routing kept the current dialers, so the new ones were never used.
      for (const [key, dialer] of next) {
        if (!dialers.has(key)) dialer.close();
      }
      throw error;
    }
    dialers = next;
    for (const [key, dialer] of removed) {
      try {
        dialer.drain({ timeout: options.drain });
      } catch (error) {
        k.log("warn", `subscription dialer ${key} could not drain: ${String(error)}`);
      }
    }
  };

  await refresh();
  (async () => {
    while (!closed) {
      await k.sleep(options.interval);
      if (closed) return;
      await refresh().catch((error) =>
        k.log("warn", `subscription update failed: ${String(error)}`),
      );
    }
  })().catch(() => {
    // Shutdown rejects the sleep, which ends the updates.
  });

  return {
    get dialers() {
      return [...dialers.values()];
    },
    refresh,
    close() {
      closed = true;
    },
  };
}

export interface AutoRouteOptions {
  /** The TUN interface name given to `k.tun_inbound`. */
  interface: string;
  /** The TUN inbound. At shutdown it drains before the routes are removed. */
  inbound: Inbound;
  /**
   * Socket mark of traffic that bypasses the TUN. Set the same `mark` on the
   * host network carriers of dialers that reach the network directly.
   */
  mark: number;
  /** Routing table that holds the default route through the TUN. Defaults to 2022. */
  table?: number;
  /** The rules use this priority and the next two. Defaults to 9000. */
  priority?: number;
  /** Deadline for the TUN to drain before the routes are removed. Defaults to 5000 milliseconds. */
  drain?: Timeout;
  /**
   * Whether the TUN drains before the routes are removed at shutdown, the default.
   * Traffic then cannot bypass the proxy while routes are removed, and new
   * connections fail meanwhile. Otherwise the routes are removed first and the
   * daemon drains the TUN after the shutdown hooks.
   */
  drain_first?: boolean;
}

/**
 * Routes the host's IPv4 and IPv6 traffic through a TUN inbound with Linux policy
 * routing, through the `ip` program. Marked traffic bypasses the TUN, and routes in
 * the main table that are more specific than a default route, such as local networks,
 * stay in use. The preset first removes what an earlier run left in its table and
 * priorities, so a start after a crash repairs them. A shutdown hook removes the
 * rules and the table's routes.
 */
export async function auto_route(options: AutoRouteOptions): Promise<void> {
  const table = String(options.table ?? 2022);
  const priority = options.priority ?? 9000;
  const priorities = [0, 1, 2].map((offset) => String(priority + offset));
  const rules = [
    ["fwmark", String(options.mark), "lookup", "main"],
    ["lookup", "main", "suppress_prefixlength", "0"],
    ["lookup", table],
  ];

  const remove = async () => {
    for (const family of ["-4", "-6"]) {
      for (const priority of priorities) {
        // Each deletion removes one rule; a failure means none is left.
        while ((await k.exec(["ip", family, "rule", "del", "priority", priority])).status === 0) {}
      }
      // Flushing fails when the table does not exist, which leaves nothing to remove.
      await k.exec(["ip", family, "route", "flush", "table", table]);
    }
  };

  // Registered first, so a startup that fails later still removes the routes.
  let stopping = false;
  k.on_shutdown(async () => {
    stopping = true;
    if (options.drain_first ?? true) {
      options.inbound.drain({ timeout: options.drain ?? k.timeout(5000) });
      await options.inbound.wait();
    }
    await remove();
  });

  await remove();
  try {
    for (const family of ["-4", "-6"]) {
      await ip([family, "route", "replace", "default", "dev", options.interface, "table", table]);
      for (const [index, rule] of rules.entries()) {
        await ip([family, "rule", "add", "priority", priorities[index]!, ...rule!]);
      }
    }
  } catch (error) {
    // Partial routes could send traffic into the TUN without the bypass rules.
    await remove();
    throw error;
  }
  // Shutdown may have removed the routes while these commands still ran.
  if (stopping) await remove();
}

async function ip(argv: string[]): Promise<void> {
  const result = await k.exec(["ip", ...argv]);
  if (result.status !== 0) {
    throw new Error(`ip ${argv.join(" ")} failed: ${result.stderr.trim()}`);
  }
}
