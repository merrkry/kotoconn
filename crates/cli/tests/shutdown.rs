#![cfg(unix)]

use rustix::process::{Pid, Signal, kill_process};
use serde_json::Value;
use std::{process::Stdio, time::Duration};
use tokio::{io::AsyncBufReadExt, process::Command, time::timeout};

#[tokio::test]
async fn signals_run_shutdown_hooks_before_daemon_ready() {
    let directory = tempfile::tempdir().unwrap();
    let policy = directory.path().join("main.ts");

    for (signal, hook_wait, shutdown_timeout, success) in [
        (Signal::TERM, "", "30", true),
        (Signal::INT, "await new Promise(() => {});", "3", false),
    ] {
        std::fs::write(
            &policy,
            format!(
                r#"
                import {{ kotoconn as k }} from '@kotoconn/bindings';
                k.on_shutdown(async () => {{
                    k.log('info', 'shutdown hook ran');
                    {hook_wait}
                }});
                k.log('info', 'shutdown hook registered');
                await new Promise(() => {{}});
                "#
            ),
        )
        .unwrap();

        let mut child = Command::new(env!("CARGO_BIN_EXE_kotoconn"))
            .args(["run", "--log-format", "json", "--config"])
            .arg(&policy)
            .args(["--shutdown-timeout", shutdown_timeout])
            .env("RUST_LOG", "info")
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut logs = tokio::io::BufReader::new(child.stderr.take().unwrap()).lines();

        let (status, events) = timeout(Duration::from_secs(40), async {
            let mut events = Vec::new();
            while let Some(line) = logs.next_line().await.unwrap() {
                let event: Value = serde_json::from_str(&line).unwrap();
                if event["fields"]["message"] == "shutdown hook registered" {
                    let pid = Pid::from_raw(child.id().unwrap().try_into().unwrap()).unwrap();
                    kill_process(pid, signal).unwrap();
                }
                events.push(event);
            }
            (child.wait().await.unwrap(), events)
        })
        .await
        .expect("CLI did not exit after the shutdown signal");

        assert!(
            events
                .iter()
                .any(|event| event["fields"]["message"] == "shutdown hook ran"),
            "{signal:?}: {events:?}"
        );
        assert!(
            events
                .iter()
                .all(|event| event["fields"]["event"] != "daemon_ready"),
            "{signal:?}: {events:?}"
        );
        assert_eq!(status.success(), success, "{signal:?}: {events:?}");
        if !success {
            assert!(
                events.iter().any(|event| {
                    event["fields"]["event"] == "daemon_failed"
                        && event["fields"]["error"]
                            .as_str()
                            .is_some_and(|error| error.contains("shutdown deadline exceeded"))
                }),
                "{signal:?}: {events:?}"
            );
        }
    }
}
