use kotoconn_script::Script;
use std::collections::HashMap;

#[test]
fn loads_relative_typescript_modules_and_finishes_top_level_jobs() {
    let sources = HashMap::from([
        (
            "lib/value.ts".into(),
            r#"
                export enum Value { Answer = 42 }

                export class Box {
                    constructor(readonly value: Value) {}
                }
            "#
            .into(),
        ),
        (
            "lib/entry.ts".into(),
            "export { Value, Box } from './value.ts';".into(),
        ),
        (
            "main.ts".into(),
            r#"
                import { kotoconn } from '@kotoconn/bindings';
                import { Value, Box } from './lib/entry.ts';

                if (typeof kotoconn !== 'object') throw Error('native module');

                const box = await Promise.resolve(new Box(Value.Answer));
                if (box.value !== 42) throw Error('wrong result');
            "#
            .into(),
        ),
    ]);

    Script::load("main.ts", sources).unwrap();
}

#[test]
fn propagates_module_resolution_syntax_and_execution_errors() {
    for (source, message) in [
        ("import './missing.ts';", "missing.ts"),
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
        );
        let error = result.err().expect("expected module loading to fail");
        assert!(error.to_string().contains(message), "{error}");
    }
}
