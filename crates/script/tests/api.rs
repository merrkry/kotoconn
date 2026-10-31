#[path = "support/resources.rs"]
mod resources;

use kotoconn_script::Script;
use resources::{Ending, load};
use std::collections::HashMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_subscription_update_keeps_the_current_dialers() {
    let result = evaluate(r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        import { subscription } from '@kotoconn/api';
        const config = {carrier: k.host_network({}), outbound: {
            resolve_handler: () => [], implementation: k.direct_outbound({}),
        }};
        let entries = new Map([['a', config]]);
        let reject = false;
        const subscribed = await subscription({
            load: async () => entries, interval: k.timeout(3600000), drain: k.timeout(1000),
            update: () => { if (reject) throw Error('consumer could not apply update'); },
        });
        const [original] = subscribed.dialers;
        entries = new Map([['b', config]]);
        reject = true;
        try { await subscribed.refresh(); } catch {}
        subscribed.close();
        if (!subscribed.dialers[0].equals(original)) throw Error('failed update replaced current dialers');
    "#).await;
    assert!(result.is_ok(), "{}", result.err().unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_removes_routes_that_an_installation_adds_meanwhile() {
    let (script, recorder) = load(r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        import { auto_route } from '@kotoconn/api';
        let release;
        let entered;
        const installing = new Promise(resolve => { entered = resolve; });
        const resume = new Promise(resolve => { release = resolve; });
        const rules = new Set();
        // Hooks run newest first, so the blocked command finishes after the preset's cleanup.
        k.on_shutdown(() => release());
        k.exec = async argv => {
            const [, family, kind, operation] = argv;
            if (kind === 'rule') {
                const priority = argv[5];
                const key = `${family}/${priority}`;
                if (operation === 'del') {
                    return {status: rules.delete(key) ? 0 : 2, stdout: '', stderr: ''};
                }
                if (family === '-6' && priority === '9002') {
                    entered();
                    await resume;
                }
                rules.add(key);
            }
            return {status: 0, stdout: '', stderr: ''};
        };
        await k.dialer({carrier: k.host_network({}), outbound: {
            implementation: k.direct_outbound({}),
            resolve_handler: () => {
                if (rules.size !== 0) throw Error(`installation added rules after cleanup: ${[...rules]}`);
                return [];
            },
        }});
        auto_route({
            interface: 'testtun0', mark: 8226,
            inbound: {drain() {}, async wait() {}},
        });
        await installing;
    "#).await;

    script.shutdown().await.unwrap();
    script.idle().await;
    let result = script.resolve(&recorder.resolve_handler(), "check").await;
    assert!(result.is_ok(), "{}", result.err().unwrap());
}

async fn evaluate(source: &str) -> Result<Script, kotoconn_script::Error> {
    resources::start(HashMap::from([("main.ts".to_owned(), source.to_owned())]))
        .await
        .map(|(script, _)| script)
}

// Minimal resources exercise the public Script entry points, without a proxy configuration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resources_register_their_handlers_and_each_handler_family_is_callable() {
    use kotoconn_config::{
        DnsHandlerResult, DnsRequest, Flow, ResourceConfig, RouteDecision, Target,
        TransportProtocol,
    };

    let (script, recorder) = load(r#"
            import { kotoconn as k } from '@kotoconn/bindings';
            const addresses = await k.lookup('127.0.0.1');
            const dialer = await k.dialer({carrier: k.host_network({}), outbound: {
                resolve_handler: async name => addresses, implementation: k.direct_outbound({}),
            }});
            await k.inbound({
                implementation: k.http_inbound({listen: {address: k.ip('127.0.0.1'), port: 0}}),
                udp_idle_timeout: k.timeout(1000),
                routing_handler: flow => {
                    if (flow.protocol !== 'udp' || flow.dest.domain !== 'test.invalid' || flow.dest.port !== 53) {
                        throw Error('flow was not preserved');
                    }
                    if (!flow.source.address.equals(k.ip('192.0.2.1')) || flow.source.port !== 12345) {
                        throw Error('source was not preserved');
                    }
                    return k.reject();
                },
            });
            let dnsCalls = 0;
            await k.dns_listener({listen: {address: k.ip('127.0.0.1'), port: 0}, protocols: ['udp'], dns_handler: request => {
                if (++dnsCalls === 2) return k.drop();
                if (dnsCalls === 3) throw Error('dns handler failed');
                const wire = k.request_bytes(request);
                wire[2] |= 0x80;
                const response = k.dns_response(wire);
                const roundtrip = k.response_bytes(response);
                if (roundtrip.length !== wire.length || roundtrip.some((byte, index) => byte !== wire[index])) {
                    throw Error('DNS bytes changed');
                }
                return k.respond(response);
            }});
        "#).await;

    let created = recorder.created();
    let [
        (_, ResourceConfig::Dialer(dialer)),
        (inbound, ResourceConfig::Inbound(routing)),
        (_, ResourceConfig::DnsListener(listener)),
    ] = created.as_slice()
    else {
        panic!("unexpected resources: {created:?}");
    };
    assert_eq!(
        script
            .resolve(&dialer.outbound.resolve_handler, "test")
            .await
            .unwrap(),
        vec!["127.0.0.1".parse::<std::net::IpAddr>().unwrap()]
    );
    assert_eq!(
        script
            .route(
                &routing.routing_handler,
                Flow {
                    inbound: kotoconn_config::InboundId(inbound.0),
                    source: "192.0.2.1:12345".parse().unwrap(),
                    protocol: TransportProtocol::Udp,
                    sniff: None,
                    dest: Target::Domain {
                        name: "test.invalid".into(),
                        port: 53
                    },
                }
            )
            .await
            .unwrap(),
        RouteDecision::Reject
    );

    let request = || {
        DnsRequest::new(
            hickory_proto::op::Message::from_vec(&[0; 12]).unwrap(),
            Default::default(),
        )
    };
    let dns = &listener.dns_handler;
    let response = script.dns(dns, request(), None).await.unwrap();
    let DnsHandlerResult::Response(response) = response else {
        panic!("expected a DNS response")
    };
    assert_eq!(response.to_vec().unwrap()[2] & 0x80, 0x80);

    assert!(matches!(
        script.dns(dns, request(), None).await.unwrap(),
        DnsHandlerResult::Drop
    ));
    assert!(
        script
            .dns(dns, request(), None)
            .await
            .unwrap_err()
            .to_string()
            .contains("dns handler failed")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resources_close_drain_and_wait_through_their_references() {
    use kotoconn_config::ResourceId;
    use std::{num::NonZeroU64, time::Duration};

    let (script, recorder) = load(r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        const host = await k.dialer({carrier: k.host_network({}), outbound: {
            resolve_handler: () => [], implementation: k.direct_outbound({}),
        }});
        const upper = await k.dialer({carrier: host, outbound: {
            resolve_handler: () => [], implementation: k.socks5_outbound({server: k.domain('proxy.invalid', 1080)}),
        }});
        if (!upper.equals(upper) || upper.equals(host)) throw Error('dialer identity');
        upper.drain({timeout: k.timeout(1500)});
        upper.drain({timeout: k.timeout(10)});
        host.close();
        await upper.wait();
        let rejected = false;
        try {
            await k.dialer({carrier: host, outbound: {resolve_handler: () => [], implementation: k.direct_outbound({})}});
        } catch (error) {
            rejected = String(error).includes('referenced resource is closed');
        }
        if (!rejected) throw Error('created a dialer on a closed carrier');
    "#).await;

    let id = |id| ResourceId(NonZeroU64::new(id).unwrap());
    assert_eq!(recorder.ended(id(1)), Some(Ending::Abort));
    assert_eq!(
        recorder.ended(id(2)),
        Some(Ending::Drain(Duration::from_millis(1500)))
    );
    assert_eq!(recorder.created().len(), 2);
    drop(script);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_waits_for_creations_and_fails_on_unhandled_rejections() {
    let policy = |floating: &str| {
        format!(
            r#"
            import {{ kotoconn as k }} from '@kotoconn/bindings';
            const outbound = {{resolve_handler: () => [], implementation: k.direct_outbound({{}})}};
            const closed = await k.dialer({{carrier: k.host_network({{}}), outbound}});
            closed.close();
            // Evaluation ends before this creation settles.
            k.dialer({{carrier: closed, outbound}}){floating};
        "#
        )
    };

    let error = evaluate(&policy("")).await.err().unwrap().to_string();
    assert!(
        error.contains("unhandled promise rejection during startup")
            && error.contains("referenced resource is closed"),
        "{error}"
    );
    evaluate(&policy(".catch(() => {})")).await.unwrap();
}

#[tokio::test]
async fn ip_helpers_match_native_addresses_and_reject_invalid_cidrs() {
    let source = r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        for (const [text, ...expected] of [
            ['10.0.0.1', true, false, false, false, false],
            ['fd00::1', true, false, false, false, false],
            ['127.0.0.1', false, true, false, false, false],
            ['::1', false, true, false, false, false],
            ['169.254.1.1', false, false, true, false, false],
            ['fe80::1', false, false, true, false, false],
            ['224.0.0.1', false, false, false, true, false],
            ['ff02::1', false, false, false, true, false],
            ['0.0.0.0', false, false, false, false, true],
            ['::', false, false, false, false, true],
            ['100.64.0.1', false, false, false, false, false],
            ['2001:db8::1', false, false, false, false, false],
            ['::ffff:10.0.0.1', false, false, false, false, false],
        ]) {
            const ip = k.ip(text);
            const actual = [
                ip.is_private(), ip.is_loopback(), ip.is_link_local(),
                ip.is_multicast(), ip.is_unspecified(),
            ];
            if (actual.some((value, index) => value !== expected[index])) {
                throw Error(`incorrect IP classification: ${text}`);
            }
        }

        for (const [prefix, address, expected] of [
            ['192.168.0.0/24', '192.168.0.255', true],
            ['192.168.0.0/24', '192.168.1.0', false],
            ['192.168.0.42/24', '192.168.0.1', true],
            ['192.168.0.0/24', '::ffff:192.168.0.1', false],
            ['0.0.0.0/0', '203.0.113.1', true],
            ['0.0.0.0/0', '::1', false],
            ['192.0.2.1/32', '192.0.2.1', true],
            ['192.0.2.1/32', '192.0.2.2', false],
            ['fd00::/8', 'fdff:ffff::1', true],
            ['fd00::/8', 'fc00::1', false],
            ['::/0', '2001:db8::1', true],
            ['::/0', '127.0.0.1', false],
            ['::1/128', '::1', true],
            ['::1/128', '::2', false],
        ]) {
            if (k.cidr(prefix).contains(k.ip(address)) !== expected) {
                throw Error(`incorrect CIDR match: ${prefix}, ${address}`);
            }
        }
        if (k.cidr('192.168.0.0/24').toString() !== '192.168.0.0/24') throw Error('CIDR display');

        const network = k.cidr('192.168.0.0/24');
        for (const address of ['192.168.0.1', {version: 4}, network, null]) {
            let rejected = false;
            try { network.contains(address); } catch { rejected = true; }
            if (!rejected) throw Error('CIDR accepted a value without a native IP');
        }

        for (const prefix of ['', 'invalid', '192.168.0.0', '192.168.0.0/33', '::/129']) {
            let rejected = false;
            try { k.cidr(prefix); } catch (error) {
                rejected = String(error).includes('invalid CIDR');
            }
            if (!rejected) throw Error(`accepted invalid CIDR: ${prefix}`);
        }
    "#;
    load(source).await;
}

#[tokio::test]
async fn domain_suffix_matching_observes_dns_label_boundaries() {
    let source = r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        for (const [name, suffix, matches, subdomain] of [
            ['example.com', 'example.com', true, false],
            ['a.b.example.com', 'example.com', true, true],
            ['*.example.com', 'example.com', true, true],
            ['badexample.com', 'example.com', false, false],
            ['example.com.bad', 'example.com', false, false],
            ['example.net', 'example.com', false, false],
            ['example.com.', 'example.com', true, false],
            ['example.com', 'example.com.', true, false],
            ['a.EXAMPLE.com.', 'EXAMPLE.COM.', true, true],
            ['a\\.example.com', 'example.com', false, false],
            ['example.com', '.', true, true],
            ['.', '.', true, false],
            ['.', 'example.com', false, false],
        ]) {
            if (k.domain_suffix(name, suffix) !== matches || k.is_subdomain(name, suffix) !== subdomain) {
                throw Error(`incorrect domain suffix match: ${name}, ${suffix}`);
            }
        }

        for (const [name, suffix] of [
            ['', 'example.com'], ['example.com', ''],
            ['a..example.com', 'example.com'], ['example.com', 'example..com'],
            ['.example.com', 'example.com'], ['example.com', '.example.com'],
        ]) {
            for (const match of [k.domain_suffix.bind(k), k.is_subdomain.bind(k)]) {
                let rejected = false;
                try { match(name, suffix); } catch (error) {
                    rejected = String(error).includes('domain');
                }
                if (!rejected) throw Error(`accepted invalid domain: ${name}, ${suffix}`);
            }
        }
    "#;
    load(source).await;
}

#[tokio::test]
async fn native_ip_and_target_views_preserve_address_families() {
    load(r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        for (const [text, version] of [['192.0.2.1', 4], ['::1', 6], ['::ffff:192.0.2.1', 6]]) {
            const ip = k.ip(text);
            if (ip.version !== version || ip.toString() !== text || !ip.equals(k.ip(text))) {
                throw Error(`incorrect IP view: ${text}`);
            }
        }
        if (k.ip('192.0.2.1').equals(k.ip('192.0.2.2')) || k.ip('192.0.2.1').equals(k.ip('::ffff:192.0.2.1'))) {
            throw Error('distinct IP addresses compared equal');
        }

        const domain = k.domain('example.com', 443);
        if (domain.domain !== 'example.com' || domain.port !== 443 || domain.ip !== undefined) {
            throw Error('incorrect domain target view');
        }
        const ip = k.ip_target(k.ip('::1'), 53);
        if (ip.domain !== undefined || ip.port !== 53 || !ip.ip.equals(k.ip('::1'))) {
            throw Error('incorrect IP target view');
        }

        for (const text of ['', 'invalid', '256.0.0.1', '192.0.2.1/24']) {
            let rejected = false;
            try { k.ip(text); } catch (error) {
                rejected = String(error).includes('invalid IP address');
            }
            if (!rejected) throw Error(`accepted invalid IP: ${text}`);
        }
    "#).await;
}

#[tokio::test]
async fn proxy_configuration_preserves_native_targets_and_optional_fields() {
    use kotoconn_config::*;
    use std::time::Duration;

    let (_script, recorder) = load(r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        const server = k.domain('proxy.invalid', 8443);
        const resolve_handler = () => [];
        let previous = k.host_network({mark: 0xffffffff, interface: 'eth0'});
        for (const implementation of [
            k.hysteria2_outbound({server, password: 'secret', server_name: 'server.invalid',
                ca_certificate: 'CA', obfs_password: 'obfs'}),
            k.shadowsocks2022_outbound({server, password: 'secret'}),
            k.socks5_outbound({server}),
        ]) {
            previous = await k.dialer({carrier: previous, outbound: {resolve_handler, implementation}});
        }
        await k.inbound({
            implementation: k.direct_inbound({listen: {address: k.ip('::1'), port: 10000},
                target: k.ip_target(k.ip('2001:db8::1'), 443)}),
            routing_handler: flow => k.route(previous, flow.dest),
            sniff: {timeout: k.timeout(300)}, udp_idle_timeout: k.timeout(1000),
        });
    "#).await;
    let server = Target::Domain {
        name: "proxy.invalid".into(),
        port: 8443,
    };
    let expected = [
        OutboundImpl::Hysteria2(Hysteria2OutboundConfig {
            server: server.clone(),
            password: "secret".into(),
            server_name: Some("server.invalid".into()),
            ca_certificate: Some("CA".into()),
            obfs_password: Some("obfs".into()),
        }),
        OutboundImpl::Shadowsocks2022(Shadowsocks2022OutboundConfig {
            server: server.clone(),
            password: "secret".into(),
        }),
        OutboundImpl::Socks5(Socks5OutboundConfig { server }),
    ];

    let created = recorder.created();
    assert_eq!(created.len(), expected.len() + 1);
    let mut lower = None;
    for ((id, config), implementation) in created.iter().zip(expected) {
        let ResourceConfig::Dialer(dialer) = config else {
            panic!("expected a dialer: {config:?}");
        };
        assert_eq!(dialer.outbound.implementation, implementation);
        let carrier = match lower {
            Some(lower) => CarrierConfig::Dialer(DialerId(lower)),
            None => CarrierConfig::Host(HostNetworkConfig {
                mark: Some(u32::MAX),
                interface: Some("eth0".into()),
            }),
        };
        assert_eq!(dialer.carrier, carrier);
        lower = Some(id.0);
    }

    let ResourceConfig::Inbound(inbound) = &created[3].1 else {
        panic!("expected an inbound");
    };
    assert_eq!(inbound.udp_idle_timeout, Duration::from_millis(1000));
    assert_eq!(
        inbound.sniff.as_ref().unwrap().timeout,
        Duration::from_millis(300)
    );
    assert_eq!(
        inbound.implementation,
        InboundImpl::Direct(DirectInboundConfig {
            listen: "[::1]:10000".parse().unwrap(),
            target: Target::Ip {
                address: "2001:db8::1".parse().unwrap(),
                port: 443
            },
        })
    );
}

#[tokio::test]
async fn numeric_api_arguments_reject_truncation_and_dns_packets_require_header_bytes() {
    load(r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        for (const [convert, max] of [
            [value => k.domain('example.com', value), 65535],
            [value => k.ip_target(k.ip('::1'), value), 65535],
            [value => k.timeout(value), 4294967295],
        ]) {
            convert(0);
            convert(max);
            for (const value of [-1, 1.5, max + 1, NaN, Infinity, '1']) {
                let rejected = false;
                try { convert(value); } catch (error) {
                    const message = String(error);
                    rejected = message.includes('integer out of range') || message.includes('expected a number');
                }
                if (!rejected) throw Error(`accepted invalid number: ${value}`);
            }
        }

        const header = new Uint8Array(12);
        header[2] = 0x80;
        k.dns_response(header);
        for (const [bytes, message] of [
            [new Uint8Array(), 'accepted DNS packet without a header'],
            [Array.from(header), 'accepted a plain array as DNS bytes'],
        ]) {
            let rejected = false;
            try { k.dns_response(bytes); } catch { rejected = true; }
            if (!rejected) throw Error(message);
        }
    "#).await;
}

#[tokio::test]
async fn tun_configuration_preserves_native_addresses_and_checks_numeric_ranges() {
    let source = r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        await k.inbound({implementation: k.tun_inbound({name: 'test0', mtu: 1500, addresses: [
            {address: k.ip('192.0.2.1'), prefix: 30}, {address: k.ip('fd00::1'), prefix: 126}
        ]}), routing_handler: () => k.reject(), udp_idle_timeout: k.timeout(1000)});
    "#;
    let (_script, recorder) = load(source).await;

    let created = recorder.created();
    let kotoconn_config::ResourceConfig::Inbound(inbound) = &created[0].1 else {
        panic!("expected an inbound");
    };
    let kotoconn_config::InboundImpl::Tun(tun) = &inbound.implementation else {
        panic!("expected a TUN inbound");
    };
    assert_eq!(tun.name, "test0");
    assert_eq!(tun.mtu, 1500);
    assert_eq!(
        tun.addresses[0].address,
        "192.0.2.1".parse::<std::net::IpAddr>().unwrap()
    );
    assert_eq!(tun.addresses[1].prefix, 126);

    for value in ["1.5", "65536", "-1", "NaN", "Infinity", "'1500'"] {
        let bad = source.replace("mtu: 1500", &format!("mtu: {value}"));
        assert!(evaluate(&bad).await.is_err());
    }
}

#[tokio::test]
async fn dns_helpers_create_services_and_validate_cache_options() {
    use kotoconn_config::ResourceConfig;

    let (_script, recorder) = load(r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        import { dns_service } from '@kotoconn/api';
        const dialer = await k.dialer({carrier: k.host_network({}), outbound: {
            resolve_handler: () => [], implementation: k.direct_outbound({})
        }});
        const options = {dialer, upstream: {protocol: 'tcp', server: {address: k.ip('192.0.2.53'), port: 53}}};
        const service = await dns_service(options);
        const uncached = await dns_service({...options, cache: false, timeout: k.timeout(1234)});
        for (const capacity of [0, -1, 1.5, 4294967296, NaN, Infinity]) {
            let rejected = false;
            try { await dns_service({...options, cache: {capacity}}); } catch (error) {
                rejected = String(error).includes('cache capacity');
            }
            if (!rejected) throw Error('accepted invalid capacity');
        }
        await k.dns_listener({listen: {address: k.ip('127.0.0.1'), port: 1053}, protocols: ['udp', 'tcp'], dns_handler: service.handler});
        uncached.drain({timeout: k.timeout(10)});
        service.close();
    "#).await;

    let services: Vec<_> = recorder
        .created()
        .into_iter()
        .filter_map(|(id, config)| match config {
            ResourceConfig::DnsService(service) => Some((id, service)),
            _ => None,
        })
        .collect();
    let [(cached_id, cached), (uncached_id, uncached)] = services.as_slice() else {
        panic!("expected two DNS services: {services:?}");
    };
    assert_eq!(cached.cache_capacity, 4096);
    assert_eq!(cached.timeout, std::time::Duration::from_secs(5));
    assert_eq!(uncached.cache_capacity, 0);
    assert_eq!(uncached.timeout, std::time::Duration::from_millis(1234));
    assert_eq!(recorder.ended(*cached_id), Some(Ending::Abort));
    assert_eq!(
        recorder.ended(*uncached_id),
        Some(Ending::Drain(std::time::Duration::from_millis(10)))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn groups_pick_members_from_health_checks() {
    use kotoconn_config::{ProbeResult, ResourceId};
    use std::{num::NonZeroU64, sync::Arc, time::Duration};

    let recorder = Arc::new(resources::Recorder::default());
    let healthy = |milliseconds, status: &str| {
        Ok(ProbeResult {
            latency: Duration::from_millis(milliseconds),
            response: format!("HTTP/1.1 {status} OK\r\n").into_bytes(),
        })
    };
    let id = |id| ResourceId(NonZeroU64::new(id).unwrap());
    recorder.set_probe(id(1), healthy(50, "204"));
    recorder.set_probe(id(2), healthy(20, "204"));
    recorder.set_probe(id(3), Err("unreachable".into()));
    recorder.set_probe(id(4), healthy(5, "500"));

    resources::start_with(
        recorder,
        HashMap::from([(
            "main.ts".to_owned(),
            r#"
            import { kotoconn as k } from '@kotoconn/bindings';
            import { fallback, fastest, http_check, selector } from '@kotoconn/api';
            const outbound = {resolve_handler: () => [], implementation: k.direct_outbound({})};
            const dialers = [];
            for (let index = 0; index < 4; index++) {
                dialers.push(await k.dialer({carrier: k.host_network({}), outbound}));
            }
            const expect = (group, dialer, message) => {
                if (!group.current.equals(dialer)) throw Error(message);
            };
            const check = http_check({target: k.domain('check.invalid', 80), timeout: k.timeout(1000)});
            const interval = k.timeout(3_600_000);

            const latest = fastest({dialers, check, interval});
            await latest.check();
            expect(latest, dialers[1], 'fastest healthy member');
            const sticky = fastest({dialers, check, interval, tolerance: 40});
            await sticky.check();
            expect(sticky, dialers[0], 'tolerance keeps the current member');
            sticky.update([dialers[2], dialers[0]]);
            await sticky.check();
            expect(sticky, dialers[0], 'update keeps a remaining member');

            const ordered = fallback({dialers: [dialers[2], dialers[3], dialers[1], dialers[0]], check, interval});
            await ordered.check();
            expect(ordered, dialers[1], 'first healthy member');
            const unhealthy = fallback({dialers: [dialers[2], dialers[3]], check, interval});
            await unhealthy.check();
            expect(unhealthy, dialers[2], 'first member without a healthy one');

            const manual = selector(dialers);
            expect(manual, dialers[0], 'selector starts with the first member');
            manual.select(dialers[2]);
            expect(manual, dialers[2], 'selected member');
            manual.update([dialers[1]]);
            expect(manual, dialers[1], 'update replaces a removed selection');
            let rejected = false;
            try { manual.select(dialers[0]); } catch { rejected = true; }
            if (!rejected) throw Error('selected a dialer outside the group');
            for (const group of [latest, sticky, ordered, unhealthy, manual]) group.close();
        "#
            .to_owned(),
        )]),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscriptions_create_new_dialers_before_draining_removed_ones() {
    let (_script, recorder) = load(
        r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        import { subscription } from '@kotoconn/api';
        const config = (port) => ({carrier: k.host_network({}), outbound: {
            resolve_handler: () => [], implementation: k.socks5_outbound({server: k.domain('proxy.invalid', port)}),
        }});
        let entries = new Map([['a', config(1)], ['b', config(2)]]);
        const updates = [];
        const subscribed = await subscription({
            load: async () => entries,
            interval: k.timeout(3_600_000),
            drain: k.timeout(1234),
            update: dialers => updates.push(dialers),
        });
        const [a] = subscribed.dialers;

        entries = new Map([['a', config(1)], ['c', config(3)]]);
        await subscribed.refresh();
        const [kept, added] = subscribed.dialers;
        if (!kept.equals(a) || updates.length !== 2 || updates[1].length !== 2 || !updates[1][1].equals(added)) {
            throw Error('subscription did not keep a and add c');
        }

        // Refreshes apply one at a time, so each new dialer is created once.
        entries = new Map([['a', config(1)], ['c', config(3)], ['d', config(4)]]);
        await Promise.all([subscribed.refresh(), subscribed.refresh()]);
        if (subscribed.dialers.length !== 3) throw Error('concurrent refreshes');

        // Routing needs a dialer, so a load without any keeps the current ones.
        entries = new Map();
        let rejected = false;
        try { await subscribed.refresh(); } catch { rejected = true; }
        if (!rejected || subscribed.dialers.length !== 3 || updates.length !== 4) {
            throw Error('an empty load replaced the dialers');
        }
        subscribed.close();
    "#,
    )
    .await;

    use kotoconn_config::ResourceId;
    use std::{num::NonZeroU64, time::Duration};
    let id = |id| ResourceId(NonZeroU64::new(id).unwrap());
    assert_eq!(recorder.created().len(), 4);
    assert_eq!(
        recorder.ended(id(2)),
        Some(Ending::Drain(Duration::from_millis(1234)))
    );
    for kept in [1, 3, 4] {
        assert_eq!(recorder.ended(id(kept)), None);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_auto_route_installation_removes_what_it_added() {
    evaluate(
        r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        import { auto_route } from '@kotoconn/api';
        // Rules and routes the recorded `ip` commands leave, as family/kind/priority keys.
        const installed = new Set();
        k.exec = async argv => {
            const [, family, kind, operation] = argv;
            const key = `${family}/${kind}/${kind === 'rule' ? argv[5] : 'default'}`;
            if (operation === 'del' || operation === 'flush') {
                return {status: installed.delete(key) ? 0 : 2, stdout: '', stderr: ''};
            }
            // IPv6 is unavailable.
            if (family === '-6') return {status: 2, stdout: '', stderr: 'IPv6 is disabled'};
            installed.add(key);
            return {status: 0, stdout: '', stderr: ''};
        };

        let rejected = false;
        try {
            await auto_route({interface: 'testtun0', mark: 8226, inbound: {drain() {}, async wait() {}}});
        } catch (error) {
            rejected = String(error).includes('IPv6 is disabled');
        }
        if (!rejected || installed.size !== 0) throw Error(`left ${[...installed]}`);
    "#,
    )
    .await
    .unwrap();
}
