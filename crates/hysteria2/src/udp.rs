use crate::{
    runtime::CloseScope,
    wire::{self, Fragment, Reassembly},
};
use anyhow::{Result, ensure};
use kotoconn_protocol::{Datagram, Packet, Scope, ServerContext, packet_pair, queue};
use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

const MAX_ASSOCIATIONS: usize = 4096;

pub struct Registration {
    pub scope: Scope,
    pub reply: oneshot::Sender<Result<Datagram>>,
}

struct Association {
    tx: queue::Sender<Packet>,
    close: CloseScope,
    active: Instant,
}

/// One activity entry per association avoids scanning every association for each packet.
#[derive(Default)]
struct ServerAssociations {
    entries: HashMap<u32, Association>,
    activity: BTreeSet<(Instant, u32)>,
}

impl ServerAssociations {
    fn has_capacity(&mut self) -> bool {
        if self.entries.len() < MAX_ASSOCIATIONS {
            return true;
        }

        // Reclaim closed scopes before rejecting a new ID. Scan only under
        // admission pressure; ordinary packet handling uses the activity index.
        self.entries.retain(|id, association| {
            if association.close.0.is_closed() {
                let removed = self.activity.remove(&(association.active, *id));
                debug_assert!(removed);
                false
            } else {
                true
            }
        });
        debug_assert_eq!(self.entries.len(), self.activity.len());

        self.entries.len() < MAX_ASSOCIATIONS
    }

    fn insert(&mut self, id: u32, association: Association) {
        self.remove(id);
        self.activity.insert((association.active, id));
        self.entries.insert(id, association);
    }

    fn remove(&mut self, id: u32) {
        if let Some(association) = self.entries.remove(&id) {
            let removed = self.activity.remove(&(association.active, id));
            debug_assert!(removed);
        }
    }

    fn touch(&mut self, id: u32) {
        // SAFETY: Only the owner task touches an association after finding it in entries.
        let association = self.entries.get_mut(&id).expect("active UDP association");
        let removed = self.activity.remove(&(association.active, id));
        debug_assert!(removed);
        association.active = Instant::now();
        self.activity.insert((association.active, id));
    }

    fn next_expiry(&self, retention: Duration) -> Option<Instant> {
        self.activity.first().map(|(active, _)| *active + retention)
    }

    fn expire(&mut self, retention: Duration) {
        let now = Instant::now();
        while let Some(&(active, id)) = self.activity.first() {
            if active + retention > now {
                break;
            }
            self.remove(id);
        }
        debug_assert_eq!(self.entries.len(), self.activity.len());
    }
}

struct Reply {
    session: u32,
    association: Scope,
    packet: Packet,
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
                let Some(registration) = registration else {
                    return Ok(());
                };
                associations.retain(|_, value| !value.close.0.is_closed());
                if associations.len() >= MAX_ASSOCIATIONS || next > u32::MAX as u64 {
                    let _ = registration.reply.send(Err(anyhow::anyhow!("Hysteria UDP association limit")));
                    continue;
                }
                if registration.reply.is_closed() || registration.scope.is_closed() {
                    continue;
                }

                let id = next as u32;
                next += 1;
                let scope = registration.scope;
                let (user, mut driver) = packet_pair(scope.clone());
                let tx = driver.tx.clone();
                let connection = connection.clone();
                if let Err(error) = scope.spawn(async move {
                    let mut counter = 0;
                    while let Some(packet) = driver.rx.recv().await {
                        send(&connection, id, &mut counter, &packet)?;
                    }
                    Ok(())
                }) {
                    // A caller can close after the admission check. Its failed
                    // registration must not end the shared QUIC connection.
                    let _ = registration.reply.send(Err(error));
                    continue;
                }

                associations.insert(id, Association {
                    tx,
                    close: CloseScope(scope),
                    active: Instant::now(),
                });
                let _ = registration.reply.send(Ok(user));
            }
            received = connection.read_datagram() => {
                let bytes = received?;
                let Ok(part) = Fragment::decode(bytes) else {
                    continue;
                };
                if !associations.contains_key(&part.session) {
                    continue;
                }
                let Ok(Some((session, packet))) = reassembly.receive(part) else {
                    continue;
                };

                if let Some(association) = associations.get(&session) {
                    let _ = association.tx.try_send(packet);
                }
            }
        }
    }
}

fn associate(
    context: &ServerContext,
    id: u32,
    responses: queue::Sender<Reply>,
) -> Result<Association> {
    let scope = context.scope.child();
    let (user, mut driver) = packet_pair(scope.clone());
    let tx = driver.tx.clone();
    let handler = context.handler.clone();
    let association = scope.clone();
    scope.spawn(async move {
        let work = handler.udp(user);
        tokio::pin!(work);

        loop {
            tokio::select! {
                result = &mut work => return result,
                packet = driver.rx.recv() => {
                    let Some(packet) = packet else {
                        return Ok(());
                    };
                    let _ = responses.try_send(Reply {
                        session: id,
                        association: association.clone(),
                        packet,
                    });
                }
            }
        }
    })?;

    Ok(Association {
        tx,
        close: CloseScope(scope),
        active: Instant::now(),
    })
}

pub async fn server(
    connection: quinn::Connection,
    context: ServerContext,
    authenticated: Arc<AtomicBool>,
) -> Result<()> {
    let mut associations = ServerAssociations::default();
    let (responses, mut replies) = queue::channel(queue::INITIAL_BYTES, |reply: &Reply| {
        reply.packet.payload.len()
    });
    let mut reassembly = Reassembly::default();
    let mut counter = 0;

    // This expires network associations. Destination-specific policy sessions
    // retain their own idle clocks in the daemon.
    let retention = context.udp_idle_timeout.max(Duration::from_secs(1));
    loop {
        let expiry = associations.next_expiry(retention);
        let expire = async {
            match expiry {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            _ = context.stopping.cancelled() => return Ok(()),
            _ = expire => {
                associations.expire(retention);
            }
            received = connection.read_datagram() => {
                let bytes = received?;
                if !authenticated.load(Ordering::Acquire) {
                    continue;
                }
                let Ok(part) = Fragment::decode(bytes) else {
                    continue;
                };
                let Ok(Some((id, packet))) = reassembly.receive(part) else {
                    continue;
                };

                if associations.entries.get(&id).is_some_and(|value| value.close.0.is_closed()) {
                    associations.remove(id);
                }
                if !associations.entries.contains_key(&id) {
                    if !associations.has_capacity() {
                        continue;
                    }
                    associations.insert(id, associate(&context, id, responses.clone())?);
                }

                // SAFETY: The single owner inserted or found this ID immediately above.
                let association = associations.entries.get(&id).expect("UDP association");
                if association.tx.try_send(packet).is_ok() {
                    associations.touch(id);
                }
            }
            reply = replies.recv() => {
                let Some(reply) = reply else {
                    return Ok(());
                };
                // A queued response from an expired association must not be sent
                // through a later association reusing the same wire session ID.
                if reply.association.is_closed() {
                    continue;
                }

                if associations.entries.contains_key(&reply.session) {
                    send(&connection, reply.session, &mut counter, &reply.packet)?;
                    associations.touch(reply.session);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn association() -> (Association, Scope) {
        let scope = Scope::new();
        let (tx, _) = queue::channel(1024, |packet: &Packet| packet.payload.len());
        (
            Association {
                tx,
                close: CloseScope(scope.clone()),
                active: Instant::now(),
            },
            scope,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn closed_association_frees_capacity_before_its_idle_deadline() {
        let retention = Duration::from_secs(10);
        let mut associations = ServerAssociations::default();
        for id in 0..MAX_ASSOCIATIONS as u32 {
            let (association, _) = association();
            associations.insert(id, association);
        }
        let deadline = associations.next_expiry(retention).unwrap();
        assert!(Instant::now() < deadline);
        assert!(!associations.has_capacity());

        associations.entries[&0].close.0.close();
        assert!(associations.has_capacity());
        assert!(!associations.entries.contains_key(&0));
        assert!(!associations.activity.iter().any(|(_, id)| *id == 0));
        assert_eq!(associations.entries.len(), MAX_ASSOCIATIONS - 1);
        assert_eq!(associations.activity.len(), MAX_ASSOCIATIONS - 1);
        assert_eq!(associations.next_expiry(retention), Some(deadline));
        assert!(
            associations
                .entries
                .values()
                .all(|value| !value.close.0.is_closed())
        );

        let (replacement, replacement_scope) = association();
        associations.insert(MAX_ASSOCIATIONS as u32, replacement);
        assert!(!associations.has_capacity());
        associations.expire(retention);
        assert_eq!(associations.entries.len(), MAX_ASSOCIATIONS);
        assert!(!replacement_scope.is_closed());

        tokio::time::advance(retention).await;
        associations.expire(retention);
        assert!(replacement_scope.is_closed());
        assert!(associations.entries.is_empty());
        assert!(associations.activity.is_empty());
        assert!(associations.has_capacity());
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_tracks_refreshes_replacements_and_equal_deadlines() {
        let retention = Duration::from_secs(10);
        let mut associations = ServerAssociations::default();
        let (first, first_scope) = association();
        let (second, second_scope) = association();
        associations.insert(1, first);
        associations.insert(2, second);
        let original = associations.next_expiry(retention).unwrap();

        tokio::time::advance(Duration::from_secs(5)).await;
        associations.touch(1);
        assert_eq!(associations.next_expiry(retention), Some(original));
        tokio::time::advance(Duration::from_secs(5)).await;
        associations.expire(retention);
        assert!(second_scope.is_closed());
        assert!(!first_scope.is_closed());
        assert_eq!(associations.entries.len(), 1);

        let (replacement, replacement_scope) = association();
        associations.insert(1, replacement);
        assert!(first_scope.is_closed());
        assert_eq!(associations.activity.len(), 1);
        tokio::time::advance(Duration::from_secs(5)).await;
        associations.expire(retention);
        assert!(!replacement_scope.is_closed());
        tokio::time::advance(Duration::from_secs(5)).await;
        associations.expire(retention);
        assert!(replacement_scope.is_closed());
        assert!(associations.next_expiry(retention).is_none());
        assert!(associations.entries.is_empty());
    }
}
