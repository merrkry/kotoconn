use kotoconn_script::Script;
use std::collections::HashMap;

#[tokio::test]
async fn normalizes_imports_and_evaluates_shared_modules_once() {
    let sources = HashMap::from([
        (
            "lib/value.ts".into(),
            "export const state: { count: number } = { count: 0 };".into(),
        ),
        (
            "lib/entry.ts".into(),
            "export { state } from './value.ts';".into(),
        ),
        (
            "main.ts".into(),
            r#"
            import { state } from './lib/entry.ts';
            const again = await import('./lib/../lib/value.ts');
            state.count++;
            if (again.state !== state || again.state.count !== 1) throw Error('module cache');
        "#
            .into(),
        ),
    ]);
    Script::load("main.ts", sources).await.unwrap();
}

#[tokio::test]
async fn propagates_module_resolution_syntax_and_execution_errors() {
    for (source, message) in [
        ("import './missing.ts';", "missing.ts"),
        ("import '../outside.ts';", "relative to the source root"),
        ("import '/absolute.ts';", "relative to the source root"),
        ("const value: =", "main.ts"),
        ("throw new Error('evaluation failed');", "evaluation failed"),
        (
            "await Promise.reject(new Error('job failed'));",
            "job failed",
        ),
    ] {
        let result = Script::load(
            "main.ts",
            HashMap::from([("main.ts".into(), source.into())]),
        )
        .await;
        let error = result.err().expect("expected module loading to fail");
        assert!(error.to_string().contains(message), "{error}");
    }
}

// Minimal registrations exercise the public Script entry points, without a proxy configuration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seals_registration_after_native_await_and_calls_each_handler_family() {
    use kotoconn_config::{
        DnsHandlerResult, DnsRequest, Flow, RouteDecision, Target, TransportProtocol,
    };

    let script = Script::load("main.ts", HashMap::from([
        ("main.ts".into(), r#"
            import { kotoconn as k } from '@kotoconn/bindings';
            const addresses = await k.lookup('127.0.0.1');
            k.resolve_handler(async name => {
                if (name === 'late') k.resolve_handler(() => []);
                return addresses;
            });
            k.routing_handler(flow => {
                if (flow.protocol !== 'udp' || flow.dest.domain !== 'test.invalid' || flow.dest.port !== 53) {
                    throw Error('flow was not preserved');
                }
                return k.reject();
            });
            let dnsCalls = 0;
            k.dns_handler(request => {
                if (++dnsCalls === 2) return k.drop();
                if (dnsCalls === 3) throw Error('dns handler failed');
                const wire = k.request_bytes(request);
                wire[2] |= 0x80;
                return k.respond(k.dns_response(wire));
            });
        "#.into()),
    ])).await.unwrap();
    let config = script.config().await.unwrap();
    let resolve = *config.resolve_handlers.iter().next().unwrap();
    let routing = *config.routing_handlers.iter().next().unwrap();
    let dns = *config.dns_handlers.iter().next().unwrap();
    assert_eq!(
        script.resolve(resolve, "test").await.unwrap(),
        vec!["127.0.0.1".parse::<std::net::IpAddr>().unwrap()]
    );
    assert!(
        script
            .resolve(resolve, "late")
            .await
            .unwrap_err()
            .to_string()
            .contains("registration has finished")
    );
    assert_eq!(
        script
            .route(
                routing,
                Flow {
                    protocol: TransportProtocol::Udp,
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

    let response = script
        .dns(
            dns,
            DnsRequest::new(
                hickory_proto::op::Message::from_vec(&[0; 12]).unwrap(),
                Default::default(),
            ),
        )
        .await
        .unwrap();
    let DnsHandlerResult::Response(response) = response else {
        panic!("expected a DNS response")
    };
    assert_eq!(response.to_vec().unwrap()[2] & 0x80, 0x80);
    let request = || {
        DnsRequest::new(
            hickory_proto::op::Message::from_vec(&[0; 12]).unwrap(),
            Default::default(),
        )
    };
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
