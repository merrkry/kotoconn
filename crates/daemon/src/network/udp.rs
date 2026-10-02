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

struct Completion {
    target: Target,
    generation: u64,
    sender: mpsc::UnboundedSender<(Target, u64)>,
}

impl Drop for Completion {
    fn drop(&mut self) {
        let _ = self.sender.send((self.target.clone(), self.generation));
    }
}

pub(super) fn association(
    handler: SessionHandler,
    mut packets: Datagram,
) -> BoxFuture<'static, Result<()>> {
    debug_assert!(packets.session_activity.is_none() || packets.single_target.is_some());

    if let Some(target) = packets.single_target.take() {
        // Select the driver before constructing its future. Otherwise every TUN
        // association retains the larger multi-target dispatch state while idle.
        Box::pin(async move {
            let incoming = packets.take_receiver();
            let worker = packets.worker.take();
            let activity = packets.session_activity.take();
            packets
                .scope
                .run(session(
                    handler,
                    target,
                    incoming,
                    packets.tx.clone(),
                    packets.scope.clone(),
                    worker,
                    activity,
                ))
                .await
        })
    } else {
        Box::pin(route_packets(handler, packets))
    }
}

async fn route_packets(handler: SessionHandler, mut packets: Datagram) -> Result<()> {
    let mut sessions = HashMap::<Target, Entry>::new();
    let (completed, mut completions) = mpsc::unbounded_channel();
    let mut generation = 0u64;

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
                if count == 0 {
                    return Ok(());
                }

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

                        generation = generation.checked_add(1)
                            .context("UDP session generation exhausted")?;
                        let scope = packets.scope.child();
                        let (tx, rx) = p::queue::channel(
                            p::queue::INITIAL_BYTES,
                            |packet: &Packet| packet.payload.len(),
                        );
                        let instance = handler.clone();
                        let replies = packets.tx.clone();
                        let completion = Completion {
                            target: target.clone(),
                            generation,
                            sender: completed.clone(),
                        };
                        let destination = target.clone();
                        let control = scope.clone();

                        let span = tracing::info_span!("session", protocol = "udp", ?destination, session_id = tracing::field::Empty);
                        scope.spawn(async move {
                            let _completion = completion;
                            let result = session(
                                instance,
                                destination,
                                rx,
                                replies,
                                control,
                                None,
                                None,
                            ).await;
                            tracing::debug!("session finished");

                            result
                        }.instrument(span))?;

                        sessions.insert(target.clone(), Entry { tx, scope, generation });
                    }

                    // SAFETY: this loop found or inserted the target; completions
                    // cannot mutate the map until the next select iteration.
                    debug_assert!(sessions.contains_key(&target));
                    let entry = sessions.get(&target).expect("admitted UDP session");
                    let _ = entry.tx.try_send(packet);
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
    worker: Option<Arc<dyn p::DatagramWorker>>,
    shared_activity: Option<p::Activity>,
) -> Result<()> {
    let (lifetime, activity) = match shared_activity {
        Some(activity) => (None, activity),
        None => {
            let lifetime = p::UdpSession::new(scope.clone(), handler.idle);
            let activity = lifetime.activity();
            (Some(lifetime), activity)
        }
    };

    // Keep the relay state in one allocation instead of embedding it in each
    // enclosing cancellation and idle-timeout future.
    let work = Box::pin(async {
        let registration = handler
            .sessions
            .register(destination.clone(), TransportProtocol::Udp, scope.clone())
            .await?;
        tracing::Span::current().record("session_id", registration.id.0);
        tracing::debug!("session started");

        let sniff = if let Some(config) = &handler.sniff {
            kotoconn_inbounds::sniff::udp(&mut incoming, config, &scope).await
        } else {
            None
        };

        let decision = handler
            .policy
            .route(
                handler.routing,
                Flow {
                    protocol: TransportProtocol::Udp,
                    dest: destination.clone(),
                    sniff,
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
                if let Some(worker) = &worker
                    && let Some(native) = client
                        .udp_native_scoped(destination.clone(), scope.clone())
                        .await?
                {
                    worker
                        .transfer(native.io, incoming, activity.clone())
                        .await?;
                    return Ok(());
                }
                let mut outgoing = client.udp_scoped(destination, scope.clone()).await?;
                if let Some(worker) = worker
                    && let Some(native) = outgoing.take_native().await?
                {
                    tracing::debug!("UDP transport transferred to TUN worker");
                    worker.transfer(native, incoming, activity.clone()).await?;
                    return Ok(());
                }
                let forward = async {
                    let mut batch = Vec::with_capacity(32);
                    while incoming.recv_many(&mut batch, 32).await != 0 {
                        for packet in batch.drain(..) {
                            outgoing.tx.send(packet).await?;
                            activity.record();
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                };

                let backward = async {
                    let mut batch = Vec::with_capacity(32);
                    while outgoing.rx.recv_many(&mut batch, 32).await != 0 {
                        let mut forwarded = false;
                        for packet in batch.drain(..) {
                            forwarded |= replies.try_send(packet).is_ok();
                        }
                        if forwarded {
                            activity.record();
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                };
                tokio::select! { result = forward => result, result = backward => result }
            })
            .await
    });
    match lifetime {
        Some(lifetime) => lifetime.run(work).await,
        None => work.await,
    }
}
