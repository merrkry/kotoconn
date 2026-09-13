use std::process::Command;

#[test]
fn startup_failure_is_one_json_event_on_stderr() {
    let output = Command::new(env!("CARGO_BIN_EXE_kotoconn"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "--config",
            "Cargo.toml/missing.ts",
            "--log-format",
            "json",
        ])
        .env("RUST_LOG", "info")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    let lines: Vec<_> = stderr.lines().collect();
    assert_eq!(lines.len(), 1, "{stderr}");
    let event: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(event["level"], "ERROR");
    assert_eq!(event["fields"]["message"], "daemon failed");
    assert!(
        event["fields"]["error"]
            .as_str()
            .unwrap()
            .contains("configuration")
    );
}

#[test]
fn filter_can_disable_logs_without_hiding_failure_exit_status() {
    let output = Command::new(env!("CARGO_BIN_EXE_kotoconn"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "run",
            "--config",
            "Cargo.toml/missing.ts",
            "--log-format",
            "json",
        ])
        .env("RUST_LOG", "off")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(output.stderr.is_empty());
}
