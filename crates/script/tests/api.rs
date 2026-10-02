use kotoconn_script::Script;
use std::collections::HashMap;

async fn load(source: &str) -> Script {
    Script::load(
        "main.ts",
        HashMap::from([("main.ts".into(), source.into())]),
    )
    .await
    .unwrap()
}

// Minimal registrations exercise the public Script entry points, without a proxy configuration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seals_registration_after_native_await_and_calls_each_handler_family() {
    use kotoconn_config::{
        DnsHandlerResult, DnsRequest, Flow, RouteDecision, Target, TransportProtocol,
    };

    let script = load(r#"
            import { kotoconn as k } from '@kotoconn/bindings';
            const addresses = await k.lookup('127.0.0.1');
            const resolve = k.resolve_handler(async name => {
                if (registrations[name]) registrations[name]();
                return addresses;
            });
            const routing = k.routing_handler(flow => {
                if (flow.protocol !== 'udp' || flow.dest.domain !== 'test.invalid' || flow.dest.port !== 53) {
                    throw Error('flow was not preserved');
                }
                if (!flow.source.address.equals(k.ip('192.0.2.1')) || flow.source.port !== 12345) {
                    throw Error('source was not preserved');
                }
                return k.reject();
            });
            let dnsCalls = 0;
            k.dns_handler(request => {
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
            });
            const outbound = {resolve_handler: resolve, implementation: k.direct_outbound({})};
            const inbound = {
                implementation: k.http_inbound({listen: {address: k.ip('127.0.0.1'), port: 0}}),
                routing_handler: routing, udp_idle_timeout: k.timeout(1000),
            };
            const registrations = {
                resolve: () => k.resolve_handler(() => []),
                routing: () => k.routing_handler(() => k.reject()),
                dns: () => k.dns_handler(() => k.drop()),
                dialer: () => k.dialer({outbound}),
                inbound: () => k.inbound(inbound),
            };
        "#).await;

    let config = script.config().await.unwrap();
    let resolve = *config.resolve_handlers.iter().next().unwrap();
    let routing = *config.routing_handlers.iter().next().unwrap();
    let dns = *config.dns_handlers.iter().next().unwrap();
    assert_eq!(
        script.resolve(resolve, "test").await.unwrap(),
        vec!["127.0.0.1".parse::<std::net::IpAddr>().unwrap()]
    );
    for name in ["resolve", "routing", "dns", "dialer", "inbound"] {
        assert!(
            script
                .resolve(resolve, name)
                .await
                .unwrap_err()
                .to_string()
                .contains("registration has finished"),
            "registration remained open for {name}"
        );
    }
    assert_eq!(
        script
            .route(
                routing,
                Flow {
                    inbound: kotoconn_config::InboundId(std::num::NonZeroU64::new(1).unwrap()),
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
    let response = script.dns(dns, request()).await.unwrap();
    let DnsHandlerResult::Response(response) = response else {
        panic!("expected a DNS response")
    };
    assert_eq!(response.to_vec().unwrap()[2] & 0x80, 0x80);

    assert!(matches!(
        script.dns(dns, request()).await.unwrap(),
        DnsHandlerResult::Drop
    ));
    assert!(
        script
            .dns(dns, request())
            .await
            .unwrap_err()
            .to_string()
            .contains("dns handler failed")
    );
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
    use std::{num::NonZeroU64, time::Duration};

    let script = load(r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        const server = k.domain('proxy.invalid', 8443);
        const resolve = k.resolve_handler(() => []);
        let previous = null;
        for (const implementation of [
            k.hysteria2_outbound({server, password: 'secret', server_name: 'server.invalid',
                ca_certificate: 'CA', obfs_password: 'obfs'}),
            k.shadowsocks2022_outbound({server, password: 'secret'}),
            k.socks5_outbound({server}),
        ]) {
            previous = k.dialer({dialer: previous, outbound: {resolve_handler: resolve, implementation}});
        }
        const routing = k.routing_handler(flow => k.route(previous, flow.dest));
        k.inbound({
            implementation: k.direct_inbound({listen: {address: k.ip('::1'), port: 10000},
                target: k.ip_target(k.ip('2001:db8::1'), 443)}),
            routing_handler: routing, sniff: {timeout: k.timeout(300)}, udp_idle_timeout: k.timeout(1000),
        });
    "#).await;
    let config = script.config().await.unwrap();
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

    assert_eq!(config.dialers.len(), expected.len());
    for (index, implementation) in expected.into_iter().enumerate() {
        let id = DialerId(NonZeroU64::new(index as u64 + 1).unwrap());
        let dialer = &config.dialers[&id];
        assert_eq!(dialer.outbound.implementation, implementation);
        assert_eq!(
            dialer.outbound.resolve_handler,
            *config.resolve_handlers.iter().next().unwrap()
        );
        assert_eq!(dialer.dialer, NonZeroU64::new(index as u64).map(DialerId));
    }

    let inbound = config.inbounds.values().next().unwrap();
    assert_eq!(
        inbound.routing_handler,
        *config.routing_handlers.iter().next().unwrap()
    );
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
async fn numeric_api_arguments_reject_truncation_and_dns_packets_require_a_header() {
    load(r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        const bytes = Array(12).fill(0);
        bytes[2] = 0x80;
        for (const [convert, max] of [
            [value => k.domain('example.com', value), 65535],
            [value => k.ip_target(k.ip('::1'), value), 65535],
            [value => k.timeout(value), 4294967295],
            [value => k.dns_response(bytes.map((byte, index) => index === 0 ? value : byte)), 255],
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

        let rejected = false;
        try { k.dns_response([]); } catch { rejected = true; }
        if (!rejected) throw Error('accepted DNS packet without a header');
    "#).await;
}

#[tokio::test]
async fn tun_configuration_preserves_native_addresses_and_checks_numeric_ranges() {
    let source = r#"
        import { kotoconn as k } from '@kotoconn/bindings';
        const routing = k.routing_handler(() => k.reject());
        k.inbound({implementation: k.tun_inbound({name: 'test0', mtu: 1500, addresses: [
            {address: k.ip('192.0.2.1'), prefix: 30}, {address: k.ip('fd00::1'), prefix: 126}
        ]}), routing_handler: routing, udp_idle_timeout: k.timeout(1000)});
    "#;
    let script = load(source).await;

    let config = script.config().await.unwrap();
    let kotoconn_config::InboundImpl::Tun(tun) =
        &config.inbounds.values().next().unwrap().implementation
    else {
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
        assert!(
            Script::load("main.ts", HashMap::from([("main.ts".into(), bad)]))
                .await
                .is_err()
        );
    }
}
