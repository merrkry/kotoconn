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
