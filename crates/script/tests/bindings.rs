#[path = "support/types.rs"]
mod types;

use rquickjs::{CatchResultExt, Context, FromJs, Runtime};
use std::num::NonZeroU64;
use types::{Options, Reference, Reply, handler::Handler, model};

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

#[tokio::test]
async fn typed_callbacks_accept_sync_results_and_reject_invalid_results() {
    let runtime = rquickjs::AsyncRuntime::new().unwrap();
    let context = rquickjs::AsyncContext::full(&runtime).await.unwrap();

    context
        .async_with(async |ctx| {
            for source in [
                "() => { throw Error('failure'); }",
                "() => 'wrong result'",
                "async () => 'wrong result'",
                "() => ({ count: 42 })",
            ] {
                let function = ctx.eval(source).unwrap();
                let handler = Handler::<Reply, Reply>::from_js(&ctx, function).unwrap();
                assert!(
                    handler
                        .call(Reply {
                            label: String::new(),
                            count: 0
                        })
                        .await
                        .catch(&ctx)
                        .is_err()
                );
            }

            let handler: Handler<String, String> =
                ctx.eval("value => value.toUpperCase()").unwrap();
            assert_eq!(handler.call("test".into()).await.unwrap(), "TEST");
        })
        .await;
}

#[test]
fn mismatched_adapters_and_callback_signatures_fail_compilation() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/*.rs");
}
