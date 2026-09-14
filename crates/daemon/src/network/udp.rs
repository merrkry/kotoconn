use super::*;
use tokio::sync::mpsc;

struct Entry {
    tx: kotoconn_protocol::queue::Sender<Packet>,
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

    let mut batch = Vec::with_capacity(32);
    loop {
        tokio::select! {
            biased;
            _ = packets.scope.cancelled() => return Ok(()),
            Some((target, generation)) = completions.recv() => {
                if sessions
                    .get(&target)
                    .is_some_and(|entry| entry.generation == generation)
                {
                    sessions.remove(&target);
                }
            }
            count = packets.rx.recv_many(&mut batch, 32) => {
                if count == 0 { return Ok(()); }
                for packet in batch.drain(..) {
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
                    let (tx, rx) = kotoconn_protocol::queue::channel(kotoconn_protocol::queue::INITIAL_BYTES, |packet: &Packet| packet.payload.len());
                    let instance = handler.clone();
                    let replies = packets.tx.clone();
                    let done = completed.clone();
                    let destination = target.clone();
                    let control = scope.clone();

                    let span = tracing::info_span!("session", protocol = "udp", ?destination, session_id = tracing::field::Empty);
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
                        if let Err(error) = &result
                            && !control.is_closed()
                        {
                            tracing::warn!(error = %format_args!("{error:#}"), "UDP session failed");
                        }
                        tracing::debug!("session finished");
                        control.close();
                        let _ = done.send((destination, generation));

                        result
                    }.instrument(span))?;

                    sessions.insert(target.clone(), Entry { tx, scope, generation });
                }

                let _ = sessions[&target].tx.try_send(packet);
                }
            }
        }
    }
}

async fn session(
    handler: SessionHandler,
    destination: Target,
    mut incoming: kotoconn_protocol::queue::Receiver<Packet>,
    replies: kotoconn_protocol::queue::Sender<Packet>,
    scope: Scope,
) -> Result<()> {
    let registration = handler
        .sessions
        .register(destination.clone(), TransportProtocol::Udp, scope.clone())
        .await?;
    tracing::Span::current().record("session_id", registration.id.0);
    tracing::debug!("session started");

    let activity = p::Activity::default();
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

        tracing::debug!(dialer_id = dialer.0.get(), "UDP route selected");

        let client = handler.clients.get(&dialer).context("unknown dialer")?;

        client
            .control(TransportProtocol::Udp)
            .run(async {
                let mut outgoing = client.udp_scoped(destination, scope.clone()).await?;
                let forward = async {
                    let mut batch = Vec::with_capacity(32);
                    while incoming.recv_many(&mut batch, 32).await != 0 {
                        for packet in batch.drain(..) {
                            outgoing.tx.send(packet).await?;
                        }
                        activity.record();
                    }
                    Ok::<(), anyhow::Error>(())
                };

                let backward = async {
                    let mut batch = Vec::with_capacity(32);
                    while outgoing.rx.recv_many(&mut batch, 32).await != 0 {
                        for packet in batch.drain(..) {
                            let _ = replies.try_send(packet);
                        }
                        activity.record();
                    }
                    Ok::<(), anyhow::Error>(())
                };
                tokio::select! { result = forward => result, result = backward => result }
            })
            .await
    };
    tokio::select! {
        biased;
        _ = activity.until_idle(handler.idle) => {
            tracing::debug!("UDP session idle timeout");
            Ok(())
        },
        result = work => result,
    }
}
