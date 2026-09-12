use kotoconn_typescript::transpile;

#[test]
fn rejects_parser_and_semantic_errors() {
    for source in ["const x: =", "const x = 1; const x = 2;"] {
        let error = transpile("broken.ts", source).unwrap_err();
        assert_eq!(error.filename, "broken.ts");
        assert!(!error.diagnostics.is_empty());
    }
}

#[test]
fn async_methods_await_order_rejections_and_finally_survive_transpilation() {
    use rquickjs::{CatchResultExt, Context, Module, Runtime};

    let source = transpile(
        "async.ts",
        r#"
        enum Stage { Ready = 42 }
        const events: string[] = [];
        class Service {
            constructor(readonly value: number) {}
            async run<T>(input: T): Promise<T> {
                events.push('start');
                try {
                    if (await Promise.resolve(this.value) !== Stage.Ready) throw Error('constructor');
                    events.push('resume');
                    await Promise.reject(new Error('expected'));
                } catch (error: unknown) {
                    if (!(error instanceof Error) || error.message !== 'expected') throw error;
                    events.push('catch');
                    return input;
                } finally {
                    events.push('finally');
                }
            }
        }
        const pending = new Service(Stage.Ready).run<string>('result');
        events.push('caller');
        if (await pending !== 'result') throw Error('wrong return value');
        if (events.join(',') !== 'start,caller,resume,catch,finally') throw Error(events.join(','));
    "#,
    )
    .unwrap();
    let runtime = Runtime::new().unwrap();
    let context = Context::full(&runtime).unwrap();
    context.with(|ctx| {
        Module::evaluate(ctx.clone(), "async.js", source)
            .unwrap()
            .finish::<()>()
            .catch(&ctx)
            .unwrap();
    });
}

#[test]
fn erases_type_only_imports_and_preserves_runtime_module_specifiers() {
    use rquickjs::{
        CatchResultExt, Context, Module, Runtime,
        loader::{BuiltinLoader, BuiltinResolver},
    };

    let source = transpile("main.ts", r#"
        import type { Missing } from './types-only.ts';
        import { value, type AlsoMissing } from './value.ts';
        type Shape = Missing & AlsoMissing;
        const result = value as Shape;
        if (result !== 42 || (await import('./value.ts')).value !== 42) throw Error('module import');
    "#).unwrap();
    let runtime = Runtime::new().unwrap();
    runtime.set_loader(
        BuiltinResolver::default().with_module("value.ts"),
        BuiltinLoader::default().with_module("value.ts", "export const value = 42;"),
    );
    let context = Context::full(&runtime).unwrap();
    context.with(|ctx| {
        Module::evaluate(ctx.clone(), "main.ts", source)
            .unwrap()
            .finish::<()>()
            .catch(&ctx)
            .unwrap();
    });
}
