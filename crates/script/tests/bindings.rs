#[path = "support/types.rs"]
mod types;

use rquickjs::{CatchResultExt, Class, Context, FromJs, Module, Runtime};
use std::num::NonZeroU64;
use types::{Options, Reference, Reply, Service, handler::Handler, model};

#[test]
fn nested_options_and_native_references_convert_without_serializing_ids() {
    let runtime = Runtime::new().unwrap();
    let context = Context::full(&runtime).unwrap();

    context.with(|ctx| {
        let reference = model::Reference(NonZeroU64::new(u64::MAX).unwrap());
        ctx.globals()
            .set("reference", Reference::from(reference.clone()))
            .unwrap();

        let options: Options = ctx
            .eval(
                r#"({
            details: { label: 'sample', enabled: true },
            parent: reference,
            values: [0, 65535]
        })"#,
            )
            .unwrap();
        let options: model::Options = options.into();
        assert_eq!(
            options,
            model::Options {
                details: model::Details {
                    label: "sample".into(),
                    enabled: true
                },
                parent: Some(reference),
                values: vec![0, 65535],
            }
        );

        let without_parent: Options = ctx
            .eval(
                r#"({
            details: { label: '', enabled: false },
            parent: null,
            values: []
        })"#,
            )
            .unwrap();
        assert!(model::Options::from(without_parent).parent.is_none());

        assert!(
            ctx.eval::<Options, _>(
                r#"({
            details: { label: '', enabled: false },
            parent: { value: 1 },
            values: []
        })"#
            )
            .is_err()
        );
    });
}

#[test]
fn generated_method_signatures_match_plain_objects_and_callbacks() {
    let runtime = Runtime::new().unwrap();
    let context = Context::full(&runtime).unwrap();

    context.with(|ctx| {
        let service = Class::instance(
            ctx.clone(),
            Service {
                callbacks: Vec::new(),
            },
        )
        .unwrap();
        ctx.globals().set("service", service.clone()).unwrap();

        let source = kotoconn_typescript::transpile(
            "case.ts",
            r#"
            type Item = { label: string; count: number };

            const reply: Item = service.echo({ label: 'value', count: 42 });
            if (reply.label !== 'value' || reply.count !== 42) throw Error('echo');

            const count = service.count({
                details: { label: '', enabled: true },
                parent: null,
                values: [1, 2]
            });
            if (count !== 2) throw Error('options');

            service.register(async (input: Item): Promise<Item> => {
                await Promise.resolve();

                return { label: input.label, count: input.count + 1 };
            });
        "#,
        )
        .unwrap();
        Module::evaluate(ctx.clone(), "case.ts", source)
            .unwrap()
            .finish::<()>()
            .catch(&ctx)
            .unwrap();

        let callback = service.borrow().callbacks[0].clone();
        let result = Handler::<Reply, Reply>::new(callback)
            .call(Reply {
                label: "result".into(),
                count: 41,
            })
            .unwrap();
        assert_eq!(
            result,
            Reply {
                label: "result".into(),
                count: 42
            }
        );
    });
}

#[test]
fn callback_exceptions_rejections_and_wrong_result_types_are_errors() {
    let runtime = Runtime::new().unwrap();
    let context = Context::full(&runtime).unwrap();

    context.with(|ctx| {
        for source in [
            "() => { throw Error('failure'); }",
            "async () => { throw Error('failure'); }",
            "() => 'wrong result'",
        ] {
            let function = ctx.eval(source).unwrap();
            let handler = Handler::<Reply, Reply>::from_js(&ctx, function).unwrap();
            assert!(
                handler
                    .call(Reply {
                        label: String::new(),
                        count: 0
                    })
                    .catch(&ctx)
                    .is_err()
            );
        }

        let handler: Handler<String, String> = ctx.eval("value => value.toUpperCase()").unwrap();
        assert_eq!(handler.call("test".into()).unwrap(), "TEST");
    });
}

#[test]
fn mismatched_adapters_and_callback_signatures_fail_compilation() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/*.rs");
}
