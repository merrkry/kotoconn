use std::process::Command;

#[test]
fn startup_failure_respects_log_filter_without_hiding_failure_exit_status() {
    for filter in ["info", "off"] {
        let output = Command::new(env!("CARGO_BIN_EXE_kotoconn"))
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .args([
                "run",
                "--config",
                "Cargo.toml/missing.ts",
                "--log-format",
                "json",
            ])
            .env("RUST_LOG", filter)
            .output()
            .unwrap();

        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        if filter == "off" {
            assert!(output.stderr.is_empty());
            continue;
        }

        let stderr = String::from_utf8(output.stderr).unwrap();
        let lines: Vec<_> = stderr.lines().collect();
        assert_eq!(lines.len(), 1, "{stderr}");
        let event: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(event["level"], "ERROR");
        assert_eq!(event["fields"]["event"], "daemon_failed");
        assert!(
            event["fields"]["error"]
                .as_str()
                .unwrap()
                .contains("configuration")
        );
    }
}
