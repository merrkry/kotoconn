//! Subprocesses started by policy. Policy is trusted, so programs run with the daemon's privileges.

use crate::native::Timeout;
use rquickjs::{FromJs, IntoJs};
use std::{io, process::Stdio, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
};
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

/// Run a program without a shell, until it exits and its output closes. Timing out
/// or dropping the future before the program exits kills it and the processes it started.
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
    // Its own group also keeps terminal signals meant for the daemon away from it.
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command
        .spawn()
        .map_err(|error| format!("spawn {program}: {error}"))?;
    let mut group = ProcessGroup::of(&child);

    let stdin = child.stdin.take();
    debug_assert_eq!(
        stdin.is_some(),
        input.is_some(),
        "stdin is piped only for input"
    );

    // SAFETY: both output streams are piped above, and nothing took them since spawning.
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");

    // Feed input while collecting output, so neither side can fill its pipe and stall.
    let run = async {
        let exited = async {
            let status = child.wait().await;
            // Processes the program left running are its own business, even when
            // they keep its output open.
            if status.is_ok() {
                group.release();
            }
            status
        };
        let (written, status, stdout, stderr) =
            tokio::join!(write(stdin, input), exited, read(stdout), read(stderr));

        let status = status.map_err(|error| format!("wait for {program}: {error}"))?;
        let stdout = stdout.map_err(|error| format!("read output of {program}: {error}"))?;
        let stderr = stderr.map_err(|error| format!("read output of {program}: {error}"))?;
        written.map_err(|error| format!("write input to {program}: {error}"))?;

        Ok(ExecResult {
            status: status.code(),
            signal: signal(status),
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        })
    };

    match timeout {
        Some(timeout) => tokio::time::timeout(timeout, run)
            .await
            .map_err(|_| format!("{program} timed out after {} ms", timeout.as_millis()))?,
        None => run.await,
    }
}

async fn read(mut pipe: impl AsyncRead + Unpin) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    pipe.read_to_end(&mut bytes).await?;
    Ok(bytes)
}

/// Kills the process group of a spawned program when dropped before the program
/// exits. Release it once the program is reaped, after which its ID can be reused.
struct ProcessGroup(Option<u32>);

impl ProcessGroup {
    fn of(child: &tokio::process::Child) -> Self {
        Self(child.id())
    }

    fn release(&mut self) {
        self.0 = None;
    }
}

impl Drop for ProcessGroup {
    #[cfg(unix)]
    fn drop(&mut self) {
        use rustix::process::{Pid, Signal, WaitId, WaitIdOptions, kill_process_group, waitid};

        let Some(leader) = self.0.and_then(|id| Pid::from_raw(id.try_into().ok()?)) else {
            return;
        };

        // The unreaped program still holds its ID, which also names the group. A
        // program that exited before anyone observed it leaves its processes alone.
        let exited = waitid(
            WaitId::Pid(leader),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT,
        );
        if !matches!(exited, Ok(Some(_))) {
            let _ = kill_process_group(leader, Signal::KILL);
        }
    }

    #[cfg(not(unix))]
    fn drop(&mut self) {}
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

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancelling_after_an_unobserved_exit_leaves_its_processes_alone() {
        let directory = tempfile::tempdir().unwrap();
        let fifo = directory.path().join("fifo");
        let pid = directory.path().join("pid");
        assert!(
            std::process::Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );

        let mut running = Box::pin(exec(
            vec![
                "sh".into(),
                "-c".into(),
                r#"cat "$1" > /dev/null & echo $$ > "$2.tmp" && mv "$2.tmp" "$2""#.into(),
                "sh".into(),
                fifo.to_str().unwrap().into(),
                pid.to_str().unwrap().into(),
            ],
            None,
            None,
        ));
        // Spawn the program, then stop polling so nothing observes its exit.
        assert!(poll_once(running.as_mut()).is_pending());

        let pid = loop {
            if let Some(pid) = std::fs::read_to_string(&pid)
                .ok()
                .and_then(|pid| pid.trim().parse::<u32>().ok())
            {
                break pid;
            }
            tokio::task::yield_now().await;
        };
        while !std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .unwrap()
            .rsplit_once(") ")
            .is_some_and(|(_, rest)| rest.starts_with('Z'))
        {
            tokio::task::yield_now().await;
        }
        drop(running);

        // Opening the FIFO for writing completes only while the background reader lives.
        let (opened, written) = tokio::sync::oneshot::channel();
        std::thread::spawn(move || opened.send(std::fs::write(fifo, "alive")));
        tokio::time::timeout(Duration::from_secs(3), written)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    fn poll_once<F: std::future::Future>(
        future: std::pin::Pin<&mut F>,
    ) -> std::task::Poll<F::Output> {
        future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }
}
