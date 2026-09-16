use crate::{
    runtime::CloseScope,
    wire::{self, Fragment, Reassembly},
};
use anyhow::{Result, ensure};
use kotoconn_protocol::{Datagram, Packet, Scope, ServerContext, packet_pair, queue};
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

pub struct Registration {
    pub scope: Scope,
    pub reply: oneshot::Sender<Result<Datagram>>,
}

struct Association {
    tx: queue::Sender<Packet>,
    close: CloseScope,
    active: Instant,
}

pub fn send(
    connection: &quinn::Connection,
    session: u32,
    counter: &mut u16,
    packet: &Packet,
) -> Result<()> {
    let mtu = connection
        .max_datagram_size()
        .ok_or_else(|| anyhow::anyhow!("peer disabled QUIC datagrams"))?;
    let fragments = match wire::fragments(session, *counter, packet, mtu) {
        Ok(fragments) => fragments,
        // The protocol permits discarding a datagram that cannot be represented.
        Err(_) => return Ok(()),
    };
    *counter = counter.wrapping_add(1);
    for bytes in fragments {
        match connection.send_datagram(bytes) {
            Ok(()) | Err(quinn::SendDatagramError::TooLarge) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

pub async fn client(
    connection: quinn::Connection,
    mut registrations: mpsc::Receiver<Registration>,
) -> Result<()> {
    let mut associations = HashMap::<u32, Association>::new();
    let mut next = 0u64;
    let mut reassembly = Reassembly::default();
    loop {
        tokio::select! {
            registration = registrations.recv() => {
                let Some(registration) = registration else { return Ok(()); };
                associations.retain(|_, value| !value.close.0.is_closed());
                if associations.len() >= 4096 || next > u32::MAX as u64 {
                    let _ = registration.reply.send(Err(anyhow::anyhow!("Hysteria UDP association limit")));
                    continue;
                }
                if registration.reply.is_closed() || registration.scope.is_closed() { continue; }
                let id = next as u32;
                next += 1;
                let scope = registration.scope;
                let (user, mut driver) = packet_pair(scope.clone());
                let tx = driver.tx.clone();
                let connection = connection.clone();
                scope.spawn(async move {
                    let mut counter = 0;
                    while let Some(packet) = driver.rx.recv().await {
                        send(&connection, id, &mut counter, &packet)?;
                    }
                    Ok(())
                })?;
                associations.insert(id, Association { tx, close: CloseScope(scope), active: Instant::now() });
                let _ = registration.reply.send(Ok(user));
            }
            received = connection.read_datagram() => {
                let bytes = received?;
                let Ok(part) = Fragment::decode(bytes) else { continue; };
                if !associations.contains_key(&part.session) { continue; }
                let Ok(Some((session, packet))) = reassembly.receive(part) else { continue; };
                if let Some(association) = associations.get(&session) {
                    let _ = association.tx.try_send(packet);
                }
            }
        }
    }
}

pub async fn server(
    connection: quinn::Connection,
    context: ServerContext,
    authenticated: std::sync::Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    let mut associations = HashMap::<u32, Association>::new();
    let (responses, mut replies) = queue::channel(queue::INITIAL_BYTES, |value: &(u32, Packet)| {
        value.1.payload.len()
    });
    let mut reassembly = Reassembly::default();
    let mut counter = 0;
    // This expires network associations. Destination-specific policy sessions
    // retain their own idle clocks in the daemon.
    let retention = context.udp_idle_timeout.max(Duration::from_secs(1));
    loop {
        let expiry = associations
            .values()
            .map(|value| value.active + retention)
            .min();
        tokio::select! {
            _ = context.stopping.cancelled() => return Ok(()),
            _ = async { match expiry { Some(at) => tokio::time::sleep_until(at).await, None => std::future::pending().await } } => {
                associations.retain(|_, value| value.active + retention > Instant::now() && !value.close.0.is_closed());
            }
            received = connection.read_datagram() => {
                let bytes = received?;
                if !authenticated.load(std::sync::atomic::Ordering::Acquire) { continue; }
                let Ok(part) = Fragment::decode(bytes) else { continue; };
                let Ok(Some((id, packet))) = reassembly.receive(part) else { continue; };
                if associations.get(&id).is_some_and(|value| value.close.0.is_closed()) { associations.remove(&id); }
                if !associations.contains_key(&id) {
                    if associations.len() >= 4096 { continue; }
                    let scope = context.scope.child();
                    let (user, mut driver) = packet_pair(scope.clone());
                    let tx = driver.tx.clone();
                    let handler = context.handler.clone();
                    let responses = responses.clone();
                    scope.spawn(async move {
                        let work = handler.udp(user);
                        tokio::pin!(work);
                        loop {
                            tokio::select! {
                                result = &mut work => return result,
                                packet = driver.rx.recv() => {
                                    let Some(packet) = packet else { return Ok(()); };
                                    let _ = responses.try_send((id, packet));
                                }
                            }
                        }
                    })?;
                    associations.insert(id, Association { tx, close: CloseScope(scope), active: Instant::now() });
                }
                // SAFETY: The single owner inserted or found this ID immediately above.
                let association = associations.get_mut(&id).expect("UDP association");
                if association.tx.try_send(packet).is_ok() { association.active = Instant::now(); }
            }
            reply = replies.recv() => {
                let Some((id, packet)) = reply else { return Ok(()); };
                if let Some(association) = associations.get_mut(&id) {
                    send(&connection, id, &mut counter, &packet)?;
                    association.active = Instant::now();
                }
            }
        }
    }
}

pub async fn register(
    registrations: &mpsc::Sender<Registration>,
    scope: Scope,
) -> Result<Datagram> {
    ensure!(!scope.is_closed(), "UDP client closed");
    let (reply, receive) = oneshot::channel();
    registrations.send(Registration { scope, reply }).await?;
    receive.await?
}
