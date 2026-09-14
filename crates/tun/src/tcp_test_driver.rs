//! Single-connection packet fixture using the same Connection as worker dispatch.
use super::*;

pub(crate) struct QueuedPacket {
    pub bytes: Vec<u8>,
}

pub(crate) struct TestConnection {
    pub packets: queue::Sender<QueuedPacket>,
    pub accepted: Accept,
    pub driver: Pin<Box<dyn Future<Output = io::Result<()>> + Send>>,
}

pub(crate) fn connection(
    flow: Flow,
    mtu: usize,
    output: queue::Sender<Transmit>,
    stopping: tokio_util::sync::CancellationToken,
) -> TestConnection {
    let (ready, mut runnable) = mpsc::unbounded_channel();
    let (mut conn, accepted) =
        Connection::new(flow, Link { mtu, gso: false }, ready, 1, Pool::default()).unwrap();
    let (packets, mut input) = queue::channel(INITIAL, |packet: &QueuedPacket| packet.bytes.len());
    let driver = Box::pin(async move {
        let mut arena = PacketArena::default();
        let mut stopped = stopping.is_cancelled();
        let mut progress = conn.poll(Instant::now(), stopped, &output, &mut arena);
        loop {
            let deadline = match progress {
                Progress::Idle(at) => at,
                _ => None,
            };
            let blocked = match progress {
                Progress::Blocked(n) => Some(n),
                _ => None,
            };
            match progress {
                Progress::Closed => {
                    return if conn.closed_normally {
                        Ok(())
                    } else {
                        Err(reset_error())
                    };
                }
                Progress::Again => {
                    tokio::task::yield_now().await;
                }
                _ => tokio::select! {
                    _ = stopping.cancelled(), if !stopped => { stopped = true; },
                    _ = runnable.recv() => {},
                    _ = async { match deadline { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {},
                    permit = output.reserve(blocked.unwrap_or(0)), if blocked.is_some() => { drop(permit.map_err(io::Error::from)?); },
                    packet = input.recv() => {
                        let Some(packet) = packet else { return Err(reset_error()); };
                        if let Some(packet) = crate::packet::Decoder::default().decode(&packet.bytes, Instant::now()) && let Some((_, repr)) = packet.tcp() {
                            conn.input(&packet.ip, &repr, Instant::now(), &output, &mut arena);
                        }
                    }
                },
            }
            conn.begin_turn();
            stopped |= stopping.is_cancelled();
            progress = conn.poll(Instant::now(), stopped, &output, &mut arena);
        }
    });
    TestConnection {
        packets,
        accepted,
        driver,
    }
}
