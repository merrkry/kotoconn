use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use kotoconn_daemon::{Daemon, Shutdown};
use std::{future::Future, io::IsTerminal, path::PathBuf, time::Duration};

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
        /// Seconds to run shutdown hooks and drain accepted work.
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
            let signal = shutdown_signal()?;
            let daemon = Daemon::launch(config, Duration::from_secs(shutdown_timeout))?;

            let running = async {
                daemon.wait_ready().await?;
                tracing::info!(
                    event = "daemon_ready",
                    resources = daemon.resources().len(),
                    "daemon ready"
                );

                daemon.wait().await?;
                bail!("policy worker stopped unexpectedly");
            };
            tokio::select! {
                biased;
                _ = signal => {}
                result = running => return result,
            }

            daemon.stop();
            tracing::info!(event = "daemon_stopping", "daemon stopping");
            if daemon.wait().await? == Shutdown::TimedOut {
                bail!("shutdown deadline exceeded; remaining policy work was cancelled");
            }
            tracing::info!(event = "daemon_stopped", "daemon stopped");
        }
    }
    Ok(())
}

fn shutdown_signal() -> Result<impl Future<Output = ()>> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt = signal(SignalKind::interrupt()).context("listen for Ctrl-C")?;
        let mut terminate = signal(SignalKind::terminate()).context("listen for SIGTERM")?;
        Ok(async move {
            tokio::select! {
                _ = interrupt.recv() => {}
                _ = terminate.recv() => {}
            }
        })
    }
    #[cfg(windows)]
    {
        let mut interrupt = tokio::signal::windows::ctrl_c().context("listen for Ctrl-C")?;
        Ok(async move {
            interrupt.recv().await;
        })
    }
}
