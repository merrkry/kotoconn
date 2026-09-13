use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use kotoconn_daemon::{Daemon, Shutdown};
use std::{io::IsTerminal, path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(name = "kotoconn", version, about = "Programmable proxy daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
    /// Log output format on stderr; RUST_LOG controls filtering.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,
}

#[derive(Clone, Copy, ValueEnum)]
enum LogFormat {
    Text,
    Json,
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

fn main() -> Result<std::process::ExitCode> {
    let cli = Cli::parse();
    init_logging(cli.log_format)?;

    let result = execute(cli);
    if let Err(error) = &result {
        tracing::error!(event = "daemon_failed", error = %format_args!("{error:#}"), "daemon failed");
    }

    Ok(if result.is_ok() {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    })
}

fn execute(cli: Cli) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(cli));
    // The daemon already drained or cancelled its work. OS resolver calls may be
    // uncancellable; they must not extend the shutdown deadline during runtime drop.
    runtime.shutdown_background();

    result
}

fn init_logging(format: LogFormat) -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
        .from_env()
        .context("invalid RUST_LOG filter")?;
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);

    match format {
        LogFormat::Text => subscriber
            .with_ansi(std::io::stderr().is_terminal())
            .try_init(),
        LogFormat::Json => subscriber.json().try_init(),
    }
    .map_err(|error| anyhow::anyhow!("initialize logging: {error}"))
}

async fn run(Cli { command, .. }: Cli) -> Result<()> {
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
            tracing::info!(
                event = "daemon_ready",
                inbounds = daemon.policy().config().inbounds.len(),
                dialers = daemon.policy().config().dialers.len(),
                "daemon ready"
            );

            tokio::select! {
                result = &mut signal => {
                    daemon.stop();
                    tracing::info!(event = "daemon_stopping", "daemon stopping");
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
            tracing::info!(event = "daemon_stopped", "daemon stopped");
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
