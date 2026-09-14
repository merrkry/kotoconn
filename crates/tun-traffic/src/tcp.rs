use crate::{
    Args, CloseMode, Direction, Workload, payload,
    stats::{Stats, record},
};
use anyhow::{Result, ensure};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::oneshot,
    task::{JoinHandle, JoinSet},
    time::Instant,
};

const BANNER: &[u8] = b"tun-traffic-ready";
const TRAILER: &[u8] = b"tun-traffic-eof";

pub async fn serve(
    args: Arc<Args>,
    mut stopping: oneshot::Receiver<()>,
) -> Result<JoinHandle<Result<()>>> {
    let listener = TcpListener::bind(SocketAddr::new(args.target, args.port)).await?;
    Ok(tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                _ = &mut stopping => break,
                accepted = listener.accept() => {
                    let (stream, _) = accepted?;
                    stream.set_nodelay(true)?;
                    let seed = args.seed;
                    tasks.spawn(async move { respond(stream, seed).await });
                }
                Some(done) = tasks.join_next() => { done??; }
            }
        }
        while let Some(done) = tasks.join_next().await {
            done??;
        }
        Ok(())
    }))
}

async fn send(
    writer: &mut (impl AsyncWrite + Unpin),
    size: usize,
    key: (u64, u64, u64, u64),
) -> Result<()> {
    let mut bytes = vec![0; 65536];
    for offset in (0..size).step_by(bytes.len()) {
        let count = bytes.len().min(size - offset);
        payload::fill(&mut bytes[..count], key.0, key.1, key.2, key.3, offset);
        writer.write_all(&bytes[..count]).await?;
    }
    Ok(())
}

async fn receive(
    reader: &mut (impl AsyncRead + Unpin),
    size: usize,
    key: (u64, u64, u64, u64),
) -> Result<()> {
    let mut bytes = vec![0; 65536];
    let mut expected = vec![0; 65536];
    for offset in (0..size).step_by(bytes.len()) {
        let count = bytes.len().min(size - offset);
        reader.read_exact(&mut bytes[..count]).await?;
        payload::fill(&mut expected[..count], key.0, key.1, key.2, key.3, offset);
        ensure!(
            bytes[..count] == expected[..count],
            "TCP corrupt payload: flow={} sequence={} direction={} offset={offset}",
            key.1,
            key.2,
            key.3
        );
    }
    Ok(())
}

async fn respond(stream: TcpStream, seed: u64) -> Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    writer.write_all(BANNER).await?;
    loop {
        let mut opcode = [0];
        if reader.read(&mut opcode).await? == 0 {
            break;
        }
        if opcode[0] == 3 {
            break;
        }
        ensure!(opcode[0] <= 2, "unknown TCP operation");
        let flow = reader.read_u64().await?;
        let sequence = reader.read_u64().await?;
        let size = usize::try_from(reader.read_u64().await?)?;
        ensure!(size <= 64 * 1024 * 1024, "oversized TCP operation");
        let upload = if opcode[0] == 1 { 0 } else { size };
        let download = if opcode[0] == 0 { 0 } else { size };
        tokio::try_join!(
            receive(&mut reader, upload, (seed, flow, sequence, 0)),
            send(&mut writer, download, (seed, flow, sequence, 1)),
        )?;
        writer.write_u8(0xa5).await?;
    }
    // The client half-close must preserve all data and the server's write half.
    writer.write_all(TRAILER).await?;
    writer.shutdown().await?;
    Ok(())
}

async fn connect(args: &Args, stats: &mut Stats) -> Result<TcpStream> {
    let socket = if args.source.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.bind(SocketAddr::new(args.source, 0))?;
    let started = Instant::now();
    let mut stream = socket
        .connect(SocketAddr::new(args.target, args.port))
        .await?;
    record(&mut stats.connect, started.elapsed())?;
    stats.connections += 1;
    stream.set_nodelay(true)?;
    let mut banner = vec![0; BANNER.len()];
    stream.read_exact(&mut banner).await?;
    ensure!(banner == BANNER, "server-first banner differs");
    record(&mut stats.first_response, started.elapsed())?;
    Ok(stream)
}

pub async fn run(args: &Args, flow: u64) -> Result<Stats> {
    let mut stats = Stats::new()?;
    let started = Instant::now();
    let mut sequence = 0;
    let size = match args.workload {
        Workload::Churn | Workload::Sparse => 64,
        _ => args.bytes,
    };
    loop {
        let stream = connect(args, &mut stats).await?;
        let (mut reader, mut writer) = stream.into_split();
        while args.more(sequence, started) {
            let planned =
                started + Duration::from_millis(args.interval_ms).mul_f64(sequence as f64);
            if args.workload == Workload::Sparse {
                // Pacing models application inactivity, never readiness or completion.
                tokio::time::sleep_until(
                    started + Duration::from_millis(args.interval_ms).mul_f64(sequence as f64),
                )
                .await;
            }
            let sent = Instant::now();
            let code = match args.direction {
                Direction::Upload => 0,
                Direction::Download => 1,
                Direction::Duplex => 2,
            };
            writer.write_u8(code).await?;
            writer.write_u64(flow).await?;
            writer.write_u64(sequence).await?;
            writer.write_u64(size as u64).await?;
            let upload = if args.direction == Direction::Download {
                0
            } else {
                size
            };
            let download = if args.direction == Direction::Upload {
                0
            } else {
                size
            };
            tokio::try_join!(
                send(&mut writer, upload, (args.seed, flow, sequence, 0)),
                receive(&mut reader, download, (args.seed, flow, sequence, 1)),
            )?;
            ensure!(
                reader.read_u8().await? == 0xa5,
                "missing transaction acknowledgment"
            );
            record(&mut stats.latency, sent.elapsed())?;
            if args.workload == Workload::Sparse {
                record(&mut stats.scheduled, planned.elapsed())?;
            }
            stats.sent_bytes += upload as u64;
            stats.received_bytes += download as u64;
            stats.operations += 1;
            sequence += 1;
            if args.workload == Workload::Churn {
                break;
            }
        }
        if args.close_mode == CloseMode::HalfClose {
            writer.shutdown().await?;
        } else {
            writer.write_u8(3).await?;
        }
        let mut trailer = Vec::new();
        reader.read_to_end(&mut trailer).await?;
        ensure!(trailer == TRAILER, "half-close lost trailer or added bytes");
        writer.shutdown().await?;
        if !args.more(sequence, started) {
            break;
        }
    }
    Ok(stats)
}
