#[path = "support/types.rs"]
mod types;

use rquickjs::{AsyncContext, AsyncRuntime, CatchResultExt, Class, Function, Module};
use std::time::Duration;
use types::{Reply, Service, handler::Handler};

// These tests use independent sample types, without config or daemon resources.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_futures_release_js_and_resume_on_the_owning_thread() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let runtime = AsyncRuntime::new().unwrap();
        let context = AsyncContext::full(&runtime).await.unwrap();
        let owner = std::thread::current().id();
        context
            .async_with(async |ctx| {
                let service = Class::instance(
                    ctx.clone(),
                    Service {
                        callbacks: Default::default(),
                    },
                )
                .unwrap();
                ctx.globals().set("service", service.clone()).unwrap();
                ctx.globals()
                    .set(
                        "checkThread",
                        Function::new(ctx.clone(), move || {
                            assert_eq!(owner, std::thread::current().id());
                        })
                        .unwrap(),
                    )
                    .unwrap();

                let source = kotoconn_typescript::transpile(
                    "case.ts",
                    r#"
                type Reply = { label: string; count: number };
                const echo = service.echo({ label: 'value', count: 42 });
                if (echo.label !== 'value' || echo.count !== 42) throw Error('echo');
                if (service.count({
                    details: { label: '', enabled: true }, parent: null, values: [1, 2]
                }) !== 2) throw Error('options');
                let release!: () => void;
                let count = 0;
                const gate = new Promise<void>(resolve => { release = resolve; });
                service.register(async (input: Reply): Promise<Reply> => {
                    checkThread();
                    count++;
                    const value = await service.echo_later(input);
                    await gate;
                    checkThread();
                    return { label: value.label, count };
                });
                service.register(async (input: Reply): Promise<Reply> => {
                    checkThread();
                    const value = await service.echo_later(input);
                    count++;
                    release();
                    checkThread();
                    return value;
                });
            "#,
                )
                .unwrap();
                Module::evaluate(ctx.clone(), "case.ts", source)
                    .unwrap()
                    .into_future::<()>()
                    .await
                    .catch(&ctx)
                    .unwrap();

                let callbacks = service.borrow().callbacks.borrow().clone();
                let (first, second) = tokio::join!(
                    Handler::<Reply, Reply>::new(callbacks[0].clone()).call(Reply {
                        label: "first".into(),
                        count: 0
                    }),
                    Handler::<Reply, Reply>::new(callbacks[1].clone()).call(Reply {
                        label: "second".into(),
                        count: 42
                    }),
                );
                assert_eq!(
                    first.catch(&ctx).unwrap(),
                    Reply {
                        label: "first".into(),
                        count: 2
                    }
                );
                assert_eq!(
                    second.catch(&ctx).unwrap(),
                    Reply {
                        label: "second".into(),
                        count: 42
                    }
                );
            })
            .await;
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_native_promises_are_catchable_and_js_rejections_reach_rust() {
    let runtime = AsyncRuntime::new().unwrap();
    let context = AsyncContext::full(&runtime).await.unwrap();
    context
        .async_with(async |ctx| {
            ctx.globals()
                .set(
                    "service",
                    Class::instance(
                        ctx.clone(),
                        Service {
                            callbacks: Default::default(),
                        },
                    )
                    .unwrap(),
                )
                .unwrap();
            let caught: Handler<String, String> = ctx
                .eval(
                    r#"
            async input => {
                try { await service.fail(); }
                catch (error) {
                    if (!String(error).includes('native failed')) throw error;
                    return input;
                }
                throw Error('native error was ignored');
            }
        "#,
                )
                .unwrap();
            assert_eq!(
                caught.call("caught".into()).await.catch(&ctx).unwrap(),
                "caught"
            );

            let rejected: Handler<String, String> = ctx
                .eval(
                    r#"
            async () => { await service.fail().catch(() => {}); throw Error('js failed'); }
        "#,
                )
                .unwrap();
            assert!(
                rejected
                    .call(String::new())
                    .await
                    .catch(&ctx)
                    .unwrap_err()
                    .to_string()
                    .contains("js failed")
            );
        })
        .await;
}
