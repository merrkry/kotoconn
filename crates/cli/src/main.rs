use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use kotoconn_daemon::{Daemon, Shutdown};
use std::{path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(name = "kotoconn", version, about = "Programmable proxy daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a policy in the foreground until SIGINT or SIGTERM.
    Run {
        /// TypeScript entry file; relative imports stay inside its directory.
        #[arg(short, long)]
        config: PathBuf,
        /// Seconds to drain accepted calls before cancelling remaining work.
        #[arg(long, default_value_t = 30)]
        shutdown_timeout: u64,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(cli));
    // The daemon already drained or cancelled its work. OS resolver calls may be
    // uncancellable; they must not extend the shutdown deadline during runtime drop.
    runtime.shutdown_background();
    result
}

async fn run(Cli { command }: Cli) -> Result<()> {
    match command {
        Command::Run {
            config,
            shutdown_timeout,
        } => {
            let signal = shutdown_signal();
            tokio::pin!(signal);

            let daemon = tokio::select! {
                biased;
                result = &mut signal => {
                    result?;
                    return Ok(());
                }
                result = Daemon::start(config, Duration::from_secs(shutdown_timeout)) => result?,
            };
            eprintln!(
                "Daemon ready: {} inbounds, {} dialers.",
                daemon.policy().config().inbounds.len(),
                daemon.policy().config().dialers.len()
            );

            tokio::select! {
                result = &mut signal => {
                    daemon.stop();
                    eprintln!("Daemon stopping.");
                    // Even a signal registration error must finish cleanup.
                    let shutdown = daemon.wait().await?;
                    result?;
                    if shutdown == Shutdown::TimedOut {
                        bail!("shutdown deadline exceeded; remaining policy work was cancelled");
                    }
                }
                result = daemon.wait() => {
                    result?;
                    bail!("policy worker stopped unexpectedly");
                }
            }
            eprintln!("Daemon stopped.");
        }
    }
    Ok(())
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("listen for Ctrl-C"),
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.context("listen for Ctrl-C")
}
