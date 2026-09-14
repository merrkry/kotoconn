//! Socket workloads shared by isolated TUN correctness and performance runners.
mod payload;
mod stats;
mod tcp;
mod udp;
#[cfg(target_os = "linux")]
mod udp_echo;

use anyhow::{Context, Result, ensure};
use clap::{Parser, ValueEnum};
use serde_json::json;
use std::{io::Write, net::IpAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::{Barrier, oneshot},
    task::JoinSet,
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Workload {
    Bulk,
    Churn,
    Sparse,
    Mixed,
    Boundaries,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Protocol {
    Tcp,
    Udp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Direction {
    Upload,
    Download,
    Duplex,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum CloseMode {
    HalfClose,
    Exchange,
}

#[derive(Clone, Parser)]
struct Args {
    #[arg(long)]
    source: IpAddr,
    #[arg(long)]
    target: IpAddr,
    #[arg(long, default_value_t = 9001)]
    port: u16,
    /// Echo SO_RCVBUF request in bytes; zero inherits the host default.
    #[arg(long, default_value_t = if cfg!(target_os = "linux") { 1048576 } else { 0 })]
    udp_server_receive_buffer: u32,
    /// Receive and echo up to N datagrams per syscall; one selects ordinary I/O.
    #[arg(long, default_value_t = if cfg!(target_os = "linux") { 32 } else { 1 }, value_parser = clap::value_parser!(u8).range(1..=32))]
    udp_echo_batch: u8,
    /// Explicit source ports for strict UDP churn, partitioned evenly among flows.
    #[arg(long)]
    udp_source_ports: Option<udp::SourcePorts>,
    /// Generator workers, independent of the daemon CPU budget.
    #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(u16).range(1..=64))]
    worker_threads: u16,
    #[arg(long, value_enum, default_value = "tcp")]
    protocol: Protocol,
    #[arg(long, value_enum, default_value = "bulk")]
    workload: Workload,
    #[arg(long, value_enum, default_value = "duplex")]
    direction: Direction,
    /// E2E checks replies after FIN; comparisons can finish their protocol before FIN.
    #[arg(long, value_enum, default_value = "half-close")]
    close_mode: CloseMode,
    #[arg(long, default_value_t = 1)]
    connections: usize,
    #[arg(long, default_value_t = 8)]
    rounds: u64,
    /// A nonzero duration replaces the fixed round count. Complete the last operation.
    #[arg(long, default_value_t = 0)]
    duration_ms: u64,
    #[arg(long, default_value_t = 1048576)]
    bytes: usize,
    #[arg(long, default_value_t = 1200)]
    datagram_bytes: usize,
    #[arg(long, default_value_t = 1500)]
    mtu: usize,
    #[arg(long, default_value_t = 10)]
    interval_ms: u64,
    /// Per-flow offered UDP rate. Zero uses request/reply operation.
    #[arg(long, default_value_t = 0)]
    rate: u64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 30)]
    timeout: u64,
    /// Permit loss only for the explicitly paced UDP load mode.
    #[arg(long)]
    allow_loss: bool,
    /// Wait for a start line on stdin, then remain alive until a finish line.
    #[arg(long)]
    controlled: bool,
}

impl Args {
    fn more(&self, round: u64, started: Instant) -> bool {
        if round == 0 {
            return true;
        }
        if self.duration_ms == 0 {
            round < self.rounds
        } else {
            started.elapsed() < Duration::from_millis(self.duration_ms)
        }
    }
}

fn emit(value: serde_json::Value) -> Result<()> {
    let mut out = std::io::stdout().lock();
    serde_json::to_writer(&mut out, &value)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(usize::from(args.worker_threads))
        .enable_all()
        .build()?;
    runtime.block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    ensure!(
        args.source.is_ipv4() == args.target.is_ipv4(),
        "address families differ"
    );
    ensure!(
        args.connections > 0 && args.connections <= 4096,
        "connections must be in 1..=4096"
    );
    ensure!(
        args.bytes > 0 && args.bytes <= 64 * 1024 * 1024,
        "bytes must be in 1..=64 MiB"
    );
    ensure!(
        args.datagram_bytes >= 16 && args.datagram_bytes <= 60000,
        "datagram bytes must be in 16..=60000"
    );
    ensure!(
        args.rounds > 0 && args.timeout > 0,
        "rounds and timeout must be positive"
    );
    ensure!((1280..=65535).contains(&args.mtu), "unsupported MTU");
    ensure!(args.interval_ms > 0, "interval must be positive");
    ensure!(
        !args.allow_loss || args.rate > 0,
        "allow-loss requires paced UDP"
    );
    ensure!(
        args.rate <= 1_000_000,
        "per-flow rate exceeds one million datagrams/s"
    );

    if let Some(ports) = args.udp_source_ports {
        ensure!(
            args.protocol == Protocol::Udp && args.workload == Workload::Churn && args.rate == 0,
            "udp-source-ports requires strict UDP churn"
        );
        ports.validate(args.connections)?;
    }

    let args = Arc::new(args);
    let (stop, stopping) = oneshot::channel();
    let server = tcp::serve(args.clone(), stopping).await?;
    let (udp_stop, udp_stopping) = oneshot::channel();
    let (udp_server, receive_buffer) = udp::serve(args.clone(), udp_stopping).await?;
    let settings = json!({
        "worker_threads": args.worker_threads,
        "udp_echo_batch": args.udp_echo_batch,
        "udp_server_receive_buffer_requested": args.udp_server_receive_buffer,
        "udp_server_receive_buffer_actual": receive_buffer,
    });
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    emit(json!({"event": "ready", "settings": settings}))?;
    if args.controlled {
        ensure!(
            input.next_line().await?.as_deref() == Some("start"),
            "expected start command"
        );
    }

    let started = Instant::now();
    let barrier = Arc::new(Barrier::new(args.connections));
    let mut tasks = JoinSet::new();
    for flow in 0..args.connections {
        let mut spec = (*args).clone();
        if spec.workload == Workload::Mixed {
            (spec.protocol, spec.workload) = match flow % 4 {
                0 => (Protocol::Tcp, Workload::Bulk),
                1 => (Protocol::Tcp, Workload::Sparse),
                2 => (Protocol::Tcp, Workload::Churn),
                _ => (
                    Protocol::Udp,
                    if spec.rate > 0 {
                        Workload::Bulk
                    } else {
                        Workload::Boundaries
                    },
                ),
            };
        }
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            let kind = format!("{:?}-{:?}", spec.protocol, spec.workload).to_lowercase();
            let run = async {
                match spec.protocol {
                    Protocol::Tcp => tcp::run(&spec, flow as u64).await,
                    Protocol::Udp => udp::run(&spec, flow as u64).await,
                }
            };
            let stats = tokio::time::timeout(
                Duration::from_secs(spec.timeout + 5) + Duration::from_millis(spec.duration_ms),
                run,
            )
            .await
            .with_context(|| format!("flow={flow} {kind} socket deadline"))?
            .with_context(|| format!("flow={flow} {kind}"))?;
            ensure!(stats.operations > 0, "flow={flow} {kind} made no progress");
            Ok::<_, anyhow::Error>(stats.json(flow, &kind, started.elapsed().as_secs_f64()))
        });
    }
    let mut flows = Vec::new();
    while let Some(result) = tasks.join_next().await {
        flows.push(result??);
    }
    let _ = stop.send(());
    let _ = udp_stop.send(());
    server.await??;
    let positive_controls = udp_server.await??;
    flows.sort_by_key(|v| v["flow"].as_u64());
    emit(
        json!({"event": "complete", "schema_version": 1, "seconds": started.elapsed().as_secs_f64(), "flows": flows, "positive_controls": positive_controls, "settings": settings}),
    )?;
    if args.controlled {
        ensure!(
            input.next_line().await?.as_deref() == Some("finish"),
            "expected finish command"
        );
    }
    Ok(())
}
