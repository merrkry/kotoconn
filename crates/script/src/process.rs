//! Subprocesses started by policy. Policy is trusted, so programs run with the daemon's privileges.

use crate::native::Timeout;
use rquickjs::{FromJs, IntoJs};
use std::{io, process::Stdio, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command};
use ts_rs::TS;

#[derive(FromJs, TS)]
pub(crate) struct ExecOptions {
    #[ts(optional)]
    pub timeout: Option<Timeout>,
    #[ts(optional)]
    pub input: Option<String>,
}

#[derive(IntoJs, TS)]
pub(crate) struct ExecResult {
    #[ts(type = "number | undefined")]
    pub status: Option<i32>,
    #[ts(type = "number | undefined")]
    pub signal: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

/// Run a program without a shell. Dropping the future kills the child.
pub(crate) async fn exec(
    argv: Vec<String>,
    timeout: Option<Duration>,
    input: Option<String>,
) -> Result<ExecResult, String> {
    let Some((program, args)) = argv.split_first() else {
        return Err("exec needs a program".into());
    };

    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(if input.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let run = async {
        let mut child = command
            .spawn()
            .map_err(|error| format!("spawn {program}: {error}"))?;

        let stdin = child.stdin.take();
        debug_assert_eq!(
            stdin.is_some(),
            input.is_some(),
            "stdin is piped only for input"
        );

        // Feed input while collecting output, so neither side can fill its pipe and stall.
        let (written, output) = tokio::join!(write(stdin, input), child.wait_with_output());
        let output = output.map_err(|error| format!("wait for {program}: {error}"))?;
        written.map_err(|error| format!("write input to {program}: {error}"))?;

        Ok(ExecResult {
            status: output.status.code(),
            signal: signal(output.status),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    };

    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, run)
            .await
            .map_err(|_| format!("{program} timed out after {} ms", timeout.as_millis()))?,
        None => run.await,
    }
}

async fn write(stdin: Option<tokio::process::ChildStdin>, input: Option<String>) -> io::Result<()> {
    let (Some(mut stdin), Some(input)) = (stdin, input) else {
        return Ok(());
    };

    // A program may exit without reading its input; its result still counts.
    match stdin.write_all(input.as_bytes()).await {
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        result => result,
    }
}

#[cfg(unix)]
fn signal(status: std::process::ExitStatus) -> Option<i32> {
    std::os::unix::process::ExitStatusExt::signal(&status)
}

#[cfg(not(unix))]
fn signal(_: std::process::ExitStatus) -> Option<i32> {
    None
}
