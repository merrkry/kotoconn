use crate::{
    Args, Workload, payload,
    stats::{Stats, record},
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::{HashMap, hash_map::Entry},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::UdpSocket,
    sync::{oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

pub async fn serve(
    args: Arc<Args>,
    mut stopping: oneshot::Receiver<()>,
) -> Result<JoinHandle<Result<u64>>> {
    let socket = UdpSocket::bind(SocketAddr::new(args.target, args.port)).await?;
    Ok(tokio::spawn(async move {
        let mut bytes = vec![0; 65536];
        let mut controls = 0;
        loop {
            tokio::select! {
                _ = &mut stopping => return Ok(controls),
                received = socket.recv_from(&mut bytes) => {
                    let (size, peer) = received?;
                    ensure!(!bytes[..size].starts_with(b"INVALID"), "malformed datagram reached outbound");
                    controls += u64::from(bytes[..size].starts_with(b"CONTROL"));
                    ensure!(socket.send_to(&bytes[..size], peer).await? == size, "partial UDP reply");
                }
            }
        }
    }))
}

fn packet(args: &Args, flow: u64, sequence: u64, size: usize) -> Vec<u8> {
    let mut bytes = vec![0; size];
    payload::fill(&mut bytes, args.seed, flow, sequence, 0, 0);
    if size >= 16 {
        bytes[..8].copy_from_slice(&flow.to_be_bytes());
        bytes[8..16].copy_from_slice(&sequence.to_be_bytes());
    }
    bytes
}

pub async fn run(args: &Args, flow: u64) -> Result<Stats> {
    if args.rate > 0 {
        return paced(args, flow).await;
    }
    let mut stats = Stats::new()?;
    stats.connections = 1;
    let mut socket = UdpSocket::bind(SocketAddr::new(args.source, 0)).await?;
    socket
        .connect(SocketAddr::new(args.target, args.port))
        .await?;
    let limit = args.mtu - if args.source.is_ipv4() { 28 } else { 48 };
    let sizes = if args.workload == Workload::Boundaries {
        vec![
            0,
            1,
            limit - 1,
            limit,
            (limit + 1).min(if args.source.is_ipv4() { 65507 } else { 65527 }),
            8192,
            60000,
        ]
    } else {
        vec![args.datagram_bytes]
    };
    let mut received = vec![0; 65536];
    let started = Instant::now();
    let mut sequence = 0;
    while args.more(sequence, started) {
        if args.workload == Workload::Sparse {
            tokio::time::sleep_until(
                started + Duration::from_millis(args.interval_ms).mul_f64(sequence as f64),
            )
            .await;
        }
        if args.workload == Workload::Churn && sequence > 0 {
            socket = UdpSocket::bind(SocketAddr::new(args.source, 0)).await?;
            socket
                .connect(SocketAddr::new(args.target, args.port))
                .await?;
            stats.connections += 1;
        }
        for &size in &sizes {
            let bytes = packet(args, flow, sequence, size);
            let sent = Instant::now();
            ensure!(socket.send(&bytes).await? == size, "partial UDP send");
            let count = tokio::time::timeout(
                Duration::from_secs(args.timeout),
                socket.recv(&mut received),
            )
            .await
            .with_context(|| {
                format!("UDP reply deadline: flow={flow} sequence={sequence} size={size}")
            })??;
            ensure!(
                received[..count] == bytes,
                "UDP payload differs: flow={flow} sequence={sequence} size={size}"
            );
            record(&mut stats.latency, sent.elapsed())?;
            stats.sent_bytes += size as u64;
            stats.received_bytes += size as u64;
            stats.sent_datagrams += 1;
            stats.received_datagrams += 1;
            stats.operations += 1;
        }
        sequence += 1;
    }
    Ok(stats)
}

async fn receive_paced(
    args: &Args,
    flow: u64,
    socket: &UdpSocket,
    mut finishing: watch::Receiver<Option<u64>>,
) -> Result<(HashMap<u64, Instant>, Stats)> {
    let mut seen = HashMap::new();
    let mut stats = Stats::new()?;
    let mut bytes = vec![0; 65536];
    let mut deadline = None;
    let mut last = 0;
    loop {
        let total = *finishing.borrow();
        if total.is_some_and(|n| seen.len() as u64 == n) {
            break;
        }
        // Observe completion even when it happened before this receiver was polled.
        // Lost UDP replies must not leave the receiver waiting without a deadline.
        if total.is_some() && deadline.is_none() {
            deadline = Some(Instant::now() + Duration::from_secs(2));
        }
        tokio::select! {
            changed = finishing.changed(), if total.is_none() => { changed?; }
            _ = async {
                match deadline {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            } => break,
            received = socket.recv(&mut bytes) => {
                let size = received?;
                let at = Instant::now();
                ensure!(size == args.datagram_bytes, "paced UDP length differs");
                let sequence = u64::from_be_bytes(bytes[8..16].try_into()?);
                ensure!(
                    bytes[..size] == packet(args, flow, sequence, size),
                    "paced UDP payload differs: flow={flow} sequence={sequence}"
                );
                match seen.entry(sequence) {
                    Entry::Occupied(_) => stats.duplicates += 1,
                    Entry::Vacant(entry) => {
                        // Latency ends at the first reply, even if another arrives later.
                        entry.insert(at);
                        stats.reordered += u64::from(sequence < last);
                    }
                }
                last = last.max(sequence);
            }
        }
    }
    Ok::<_, anyhow::Error>((seen, stats))
}

async fn paced(args: &Args, flow: u64) -> Result<Stats> {
    let socket = UdpSocket::bind(SocketAddr::new(args.source, 0)).await?;
    socket
        .connect(SocketAddr::new(args.target, args.port))
        .await?;
    let started = Instant::now();
    let (finished, finishing) = watch::channel(None);
    let send = async {
        let mut timestamps = Vec::new();
        let mut sequence = 0;
        while args.more(sequence, started) {
            let planned = started + Duration::from_secs_f64(sequence as f64 / args.rate as f64);
            tokio::time::sleep_until(planned).await;
            let bytes = packet(args, flow, sequence, args.datagram_bytes);
            let actual = Instant::now();
            ensure!(
                socket.send(&bytes).await? == bytes.len(),
                "partial paced UDP send"
            );
            timestamps.push((planned, actual));
            sequence += 1;
        }
        finished.send(Some(sequence))?;
        Ok::<_, anyhow::Error>(timestamps)
    };
    let receive = receive_paced(args, flow, &socket, finishing);
    let (timestamps, (received, mut stats)) = tokio::try_join!(send, receive)?;
    stats.connections = 1;
    for (sequence, at) in &received {
        let &(planned, actual) = timestamps
            .get(usize::try_from(*sequence)?)
            .ok_or_else(|| anyhow::anyhow!("received unsent sequence {sequence}"))?;
        record(&mut stats.latency, at.duration_since(actual))?;
        record(&mut stats.scheduled, at.duration_since(planned))?;
    }
    stats.sent_datagrams = timestamps.len() as u64;
    stats.missing_sequence_sample = (0..stats.sent_datagrams)
        .filter(|sequence| !received.contains_key(sequence))
        .take(64)
        .collect();
    stats.received_datagrams = received.len() as u64;
    stats.sent_bytes = stats.sent_datagrams * args.datagram_bytes as u64;
    stats.received_bytes = stats.received_datagrams * args.datagram_bytes as u64;
    stats.operations = stats.received_datagrams;
    ensure!(
        args.allow_loss || stats.sent_datagrams == stats.received_datagrams,
        "UDP loss: sent={} received={}",
        stats.sent_datagrams,
        stats.received_datagrams
    );
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[tokio::test]
    async fn completed_sender_with_missing_replies_still_finishes_receive_drain() {
        let args =
            Args::try_parse_from(["traffic", "--source", "127.0.0.1", "--target", "127.0.0.1"])
                .unwrap();
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for sequence in [2, 0, 0] {
            sender
                .send_to(
                    &packet(&args, 7, sequence, args.datagram_bytes),
                    receiver.local_addr().unwrap(),
                )
                .await
                .unwrap();
        }
        // Completion predates the first receive poll. Missing sequences 1 and 3
        // still require a bounded drain despite duplicate and reordered replies.
        let (_finished, finishing) = watch::channel(Some(4));
        let (received, stats) = tokio::time::timeout(
            Duration::from_secs(5),
            receive_paced(&args, 7, &receiver, finishing),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(received.len(), 2);
        assert!(received.contains_key(&0));
        assert!(received.contains_key(&2));
        assert_eq!(stats.duplicates, 1);
        assert_eq!(stats.reordered, 1);
    }
}
