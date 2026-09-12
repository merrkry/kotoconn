use super::*;
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};

struct Entry {
    tx: mpsc::Sender<Packet>,
    scope: Scope,
    generation: u64,
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.scope.close();
    }
}

pub(super) async fn association(handler: SessionHandler, mut packets: Datagram) -> Result<()> {
    let mut sessions = HashMap::<Target, Entry>::new();
    let (completed, mut completions) = mpsc::unbounded_channel();
    let mut generation = 0;

    loop {
        tokio::select! {
            biased;
            _ = packets.scope.cancelled() => return Ok(()),
            Some((target, generation)) = completions.recv() => {
                if sessions.get(&target).is_some_and(|entry| entry.generation == generation) { sessions.remove(&target); }
            }
            packet = packets.rx.recv() => {
                let Some(packet) = packet else { return Ok(()); };
                let target = packet.target.clone();

                if sessions
                    .get(&target)
                    .is_some_and(|entry| entry.scope.is_closed())
                {
                    sessions.remove(&target);
                }

                if !sessions.contains_key(&target) {
                    if sessions.len() >= 1024 {
                        continue;
                    }

                    generation += 1;
                    let scope = packets.scope.child();
                    let (tx, rx) = mpsc::channel(64);
                    let instance = handler.clone();
                    let replies = packets.tx.clone();
                    let done = completed.clone();
                    let destination = target.clone();
                    let control = scope.clone();

                    scope.spawn(async move {
                        let result =
                            control
                                .run(session(
                                    instance,
                                    destination.clone(),
                                    rx,
                                    replies,
                                    control.clone(),
                                ))
                                .await;
                        control.close();
                        let _ = done.send((destination, generation));

                        result
                    })?;

                    sessions.insert(target.clone(), Entry { tx, scope, generation });
                }

                let _ = sessions[&target].tx.try_send(packet);
            }
        }
    }
}

async fn session(
    handler: SessionHandler,
    destination: Target,
    mut incoming: mpsc::Receiver<Packet>,
    replies: mpsc::Sender<Packet>,
    scope: Scope,
) -> Result<()> {
    let _registration = handler
        .sessions
        .register(destination.clone(), TransportProtocol::Udp, scope.clone())
        .await?;
    let (activity, last_activity) = watch::channel(Instant::now());
    let work = async {
        let decision = handler
            .policy
            .route(
                handler.routing,
                Flow {
                    protocol: TransportProtocol::Udp,
                    dest: destination.clone(),
                },
            )
            .await?;

        let RouteDecision::Udp { dialer } = decision else {
            bail!("UDP session rejected");
        };

        let client = handler.clients.get(&dialer).context("unknown dialer")?;

        client
            .control(TransportProtocol::Udp)
            .run(async {
                let mut outgoing = client.udp_scoped(destination, scope.clone()).await?;
                let forward = async {
                    while let Some(packet) = incoming.recv().await {
                        outgoing.tx.send(packet).await?;
                        activity.send_replace(Instant::now());
                    }
                    Ok::<(), anyhow::Error>(())
                };

                let backward = async {
                    while let Some(packet) = outgoing.rx.recv().await {
                        // Reply address is protocol metadata; there is no target rewrite.
                        let _ = replies.try_send(packet);
                        activity.send_replace(Instant::now());
                    }
                    Ok::<(), anyhow::Error>(())
                };
                tokio::select! { result = forward => result, result = backward => result }
            })
            .await
    };
    tokio::select! {
        biased;
        _ = until_idle(handler.idle, last_activity) => Ok(()),
        result = work => result,
    }
}

async fn until_idle(timeout: std::time::Duration, mut activity: watch::Receiver<Instant>) {
    loop {
        let deadline = *activity.borrow_and_update() + timeout;
        tokio::select! {
            biased;
            changed = activity.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            _ = tokio::time::sleep_until(deadline) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn activity_refreshes_only_its_session_and_idle_work_expires() {
        let timeout = Duration::from_secs(10);
        let (a, a_rx) = watch::channel(Instant::now());
        let (_b, b_rx) = watch::channel(Instant::now());
        let first = until_idle(timeout, a_rx);
        let second = until_idle(timeout, b_rx);
        tokio::pin!(first, second);
        tokio::select! {
            biased;
            _ = &mut first => panic!("expired before idle interval"),
            _ = &mut second => panic!("expired before idle interval"),
            _ = std::future::ready(()) => {},
        }
        tokio::time::advance(Duration::from_secs(9)).await;
        a.send_replace(Instant::now());
        tokio::time::advance(Duration::from_secs(1)).await;
        second.await;
        tokio::select! {
            biased;
            _ = &mut first => panic!("another session's deadline expired this session"),
            _ = std::future::ready(()) => {},
        }
        tokio::time::advance(Duration::from_secs(9)).await;
        first.await;
    }
}
