use kotoconn_typescript::transpile;

#[test]
fn rejects_parser_and_semantic_errors() {
    for source in ["const x: =", "const x = 1; const x = 2;"] {
        let error = transpile("broken.ts", source).unwrap_err();
        assert_eq!(error.filename, "broken.ts");
        assert!(!error.diagnostics.is_empty());
    }
}
