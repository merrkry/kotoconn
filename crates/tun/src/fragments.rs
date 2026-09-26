//! Bind IP datagrams to their transport worker when the offset-zero fragment arrives.
//! Earlier fragments retain their receive blocks under the shared reassembly budget.
use crate::{
    Received,
    packet::{FragmentKey, REASSEMBLY_LIFETIME},
    reassembly::{Lease, Limits},
};
use ahash::AHashMap as HashMap;
use std::{
    mem::size_of,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{sync::Notify, time::Instant};

pub(crate) struct Binding {
    pub key: FragmentKey,
    pub expires: Instant,
    shard: usize,
    active: AtomicBool,
    _lease: Lease,
}

impl Binding {
    pub fn active(&self, now: Instant) -> bool {
        now < self.expires && self.active.load(Ordering::Acquire)
    }
}

pub(crate) struct Pending {
    pub frames: Vec<Received>,
    retained: usize,
    // This lease follows the batch through forwarding and decoder consumption.
    lease: Lease,
}

impl Pending {
    fn push(&mut self, frame: Received, now: Instant) -> bool {
        debug_assert!(frame.allocation_size >= frame.bytes.len());
        let retained = self.retained + frame.allocation_size;
        let capacity = self.frames.capacity().max(self.frames.len() + 1);
        if !self
            .lease
            .resize(retained + capacity * size_of::<Received>(), now)
        {
            return false;
        }

        self.frames.reserve_exact(1);
        if !self.lease.resize(
            retained + self.frames.capacity() * size_of::<Received>(),
            now,
        ) {
            return false;
        }
        self.retained = retained;
        self.frames.push(frame);
        true
    }

    fn size(&self) -> usize {
        self.frames
            .iter()
            .map(|frame| frame.bytes.len())
            .sum::<usize>()
            + self.frames.capacity() * size_of::<Received>()
    }
}

pub(crate) struct Delivery {
    pub owner: usize,
    pub binding: Arc<Binding>,
    pub frame: Received,
    pub pending: Option<Pending>,
}

impl Delivery {
    pub fn size(&self) -> usize {
        self.frame.bytes.len() + self.pending.as_ref().map_or(0, Pending::size)
    }
}

struct Entry {
    binding: Arc<Binding>,
    owner: Option<usize>,
    pending: Option<Pending>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        // Queued fragments must not resurrect this datagram after completion,
        // expiry or admission failure, even if its fragment ID is reused.
        self.binding.active.store(false, Ordering::Release);
    }
}

const ENTRY_BYTES: usize =
    size_of::<(FragmentKey, Entry)>() + size_of::<Binding>() + 2 * size_of::<usize>();

#[derive(Default)]
struct Shard {
    entries: HashMap<FragmentKey, Entry>,
    next_expiry: Option<Instant>,
}

impl Shard {
    fn expire(&mut self, now: Instant) {
        if self.next_expiry.is_some_and(|at| at <= now) {
            self.entries.retain(|_, entry| entry.binding.expires > now);
            self.next_expiry = self
                .entries
                .values()
                .map(|entry| entry.binding.expires)
                .min();
        }
    }
}

pub(crate) struct Routes {
    shards: Vec<Mutex<Shard>>,
    limits: Limits,
    changed: Notify,
}

impl Routes {
    pub fn new(count: usize, limits: Limits) -> Self {
        debug_assert!(count > 0);
        Self {
            shards: (0..count).map(|_| Mutex::new(Shard::default())).collect(),
            limits,
            changed: Notify::new(),
        }
    }

    fn lock(&self, shard: usize) -> MutexGuard<'_, Shard> {
        // SAFETY: The stable endpoint hash supplies an in-range shard. Only
        // routing and accounting run under the lock; no user code can poison it.
        debug_assert!(shard < self.shards.len());
        self.shards[shard]
            .lock()
            .expect("fragment routing poisoned")
    }

    /// Determining the owner and taking pending chunks share one lock. Later
    /// fragments may overtake this delivery; the local decoder accepts disorder.
    pub fn route(
        &self,
        shard: usize,
        key: FragmentKey,
        first_owner: Option<usize>,
        frame: Received,
        now: Instant,
    ) -> Option<Delivery> {
        let mut state = self.lock(shard);
        state.expire(now);

        if let std::collections::hash_map::Entry::Vacant(entry) = state.entries.entry(key) {
            let lease = self.limits.reserve(ENTRY_BYTES, now)?;
            let expires = now + REASSEMBLY_LIFETIME;
            entry.insert(Entry {
                binding: Arc::new(Binding {
                    key,
                    expires,
                    shard,
                    active: AtomicBool::new(true),
                    _lease: lease,
                }),
                owner: None,
                pending: None,
            });
            if state.next_expiry.is_none_or(|at| expires < at) {
                state.next_expiry = Some(expires);
                // Notify stores a permit if the endpoint has not started waiting.
                self.changed.notify_one();
            }
        }

        // SAFETY: This call found or inserted the key while holding the shard lock.
        let entry = state.entries.get_mut(&key).expect("fragment route entry");
        if entry.owner.is_none() {
            entry.owner = first_owner;
        }
        if let Some(owner) = entry.owner {
            return Some(Delivery {
                owner,
                binding: entry.binding.clone(),
                frame,
                pending: entry.pending.take(),
            });
        }

        if entry.pending.is_none() {
            entry.pending = Some(Pending {
                frames: Vec::new(),
                retained: 0,
                lease: self.limits.reserve(0, now)?,
            });
        }
        // SAFETY: The pending batch was installed above under the same lock.
        if !entry
            .pending
            .as_mut()
            .expect("pending fragments")
            .push(frame, now)
        {
            state.entries.remove(&key);
        }
        None
    }

    pub fn finish(&self, binding: &Arc<Binding>) {
        let mut state = self.lock(binding.shard);
        if state
            .entries
            .get(&binding.key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.binding, binding))
        {
            state.entries.remove(&binding.key);
            // Keep the scheduled deadline until the next expiry scan. Clearing
            // it here would wake the supervisor for every completed datagram.
        }
    }

    /// Orphans have no decoder owner yet. Reclaim their oldest retained batch
    /// separately when the shared budget signals pressure.
    pub fn discard_oldest_pending(&self, shard: usize) -> bool {
        let mut state = self.lock(shard);
        let key = state
            .entries
            .iter()
            .filter(|(_, entry)| entry.pending.is_some())
            .min_by_key(|(_, entry)| entry.binding.expires)
            .map(|(key, _)| *key);
        if let Some(key) = key {
            state.entries.remove(&key);
            true
        } else {
            false
        }
    }

    /// Polled by the endpoint supervisor so orphan fragments expire even when
    /// every receive worker is idle. No periodic scan or extra runtime is needed.
    pub async fn expire(&self) {
        loop {
            let next = self
                .shards
                .iter()
                .enumerate()
                .filter_map(|(index, _)| self.lock(index).next_expiry)
                .min();
            tokio::select! {
                _ = self.changed.notified() => {},
                _ = async {
                    match next {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                } => {
                    let now = Instant::now();
                    for index in 0..self.shards.len() {
                        self.lock(index).expire(now);
                    }
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        packet::{self, Decoder, Packet, RouteKey},
        tests::{flow, segment},
        udp,
    };
    use bytes::Bytes;
    use smoltcp::wire::*;
    use std::{sync::Barrier, time::Duration};

    fn received(bytes: impl Into<Bytes>) -> Received {
        let bytes = bytes.into();
        Received {
            allocation_size: bytes.len(),
            bytes,
            checksum_verified: false,
            udp_segment_size: None,
        }
    }

    fn frames(ipv6: bool, tcp: bool) -> Vec<Bytes> {
        let flow = flow(ipv6);
        if !tcp {
            return udp::Encoder::new(1280)
                .encode(flow.source, flow.destination, &[7; 2500])
                .unwrap();
        }
        let packet = segment(flow, 100, None, TcpControl::Syn, &[7; 2500]).bytes;
        let header = if ipv6 { 40 } else { 20 };
        let parts: Vec<_> = packet[header..].chunks(1024).collect();
        parts
            .iter()
            .enumerate()
            .map(|(index, part)| {
                let mut bytes = packet[..header].to_vec();
                if ipv6 {
                    bytes.extend_from_slice(&[6, 0]);
                    bytes.extend_from_slice(
                        &(((index * 1024) | usize::from(index + 1 < parts.len())) as u16)
                            .to_be_bytes(),
                    );
                    bytes.extend_from_slice(&42u32.to_be_bytes());
                    bytes.extend_from_slice(part);
                    let mut ip = Ipv6Packet::new_unchecked(&mut bytes);
                    ip.set_next_header(IpProtocol::Ipv6Frag);
                    ip.set_payload_len((8 + part.len()) as u16);
                } else {
                    bytes.extend_from_slice(part);
                    let mut ip = Ipv4Packet::new_unchecked(&mut bytes);
                    ip.set_total_len((header + part.len()) as u16);
                    ip.set_ident(42);
                    ip.set_dont_frag(false);
                    ip.set_more_frags(index + 1 < parts.len());
                    ip.set_frag_offset((index * 1024) as u16);
                    ip.fill_checksum();
                }
                Bytes::from(bytes)
            })
            .collect()
    }

    fn route(routes: &Routes, frame: Received, now: Instant) -> Option<Delivery> {
        let parsed = packet::parse(&frame.bytes).unwrap();
        let RouteKey::Fragment(key) = parsed.route().unwrap() else {
            panic!("fragment required")
        };
        let owner = parsed.first_route().map(|_| 1);
        routes.route(0, key, owner, frame, now)
    }

    fn decode(
        routes: &Routes,
        decoder: &mut Decoder,
        mut delivery: Delivery,
        now: Instant,
    ) -> Vec<Packet<'static>> {
        assert_eq!(delivery.owner, 1);
        let mut result = Vec::new();
        let mut consume = |frame: Received| {
            if let Some(packet) = decoder.decode_fragment(&frame.bytes, now, &delivery.binding) {
                result.push(packet.into_owned());
            }
            if !decoder.retains(&delivery.binding) {
                routes.finish(&delivery.binding);
            }
        };
        if let Some(pending) = &mut delivery.pending {
            for frame in pending.frames.drain(..) {
                consume(frame);
            }
        }
        drop(delivery.pending);
        consume(delivery.frame);
        result
    }

    #[test]
    fn first_fragment_binds_tcp_and_udp_with_concurrent_out_of_order_arrivals() {
        for ipv6 in [false, true] {
            for tcp in [false, true] {
                for _ in 0..16 {
                    let limits = Limits::default();
                    let routes = Routes::new(2, limits.clone());
                    let frames = frames(ipv6, tcp);
                    let barrier = Barrier::new(frames.len());
                    let now = Instant::now();
                    let deliveries = std::thread::scope(|scope| {
                        let handles: Vec<_> = frames
                            .into_iter()
                            .map(|frame| {
                                let (routes, barrier) = (&routes, &barrier);
                                scope.spawn(move || {
                                    barrier.wait();
                                    route(routes, received(frame.to_vec()), now)
                                })
                            })
                            .collect();
                        handles
                            .into_iter()
                            .filter_map(|thread| thread.join().unwrap())
                            .collect::<Vec<_>>()
                    });
                    let mut decoder = Decoder::new(limits);
                    let packets: Vec<_> = deliveries
                        .into_iter()
                        .rev()
                        .flat_map(|delivery| decode(&routes, &mut decoder, delivery, now))
                        .collect();
                    assert_eq!(packets.len(), 1);
                    if tcp {
                        let (actual, repr) = packets[0].tcp().unwrap();
                        assert_eq!(actual, flow(ipv6));
                        assert_eq!(repr.control, TcpControl::Syn);
                        assert_eq!(repr.payload, [7; 2500]);
                    } else {
                        assert_eq!(packets[0].udp().unwrap(), (flow(ipv6), &[7; 2500][..]));
                    }
                }
            }
        }
    }

    #[test]
    fn orphan_chunks_move_without_copy_and_reassembly_keeps_the_original_deadline() {
        let limits = Limits::default();
        let routes = Routes::new(2, limits.clone());
        let frames = frames(true, false);
        let now = Instant::now();
        let tail = received(frames[2].to_vec());
        let pointer = tail.bytes.as_ptr();
        assert!(route(&routes, tail, now).is_none());
        let delivery = route(
            &routes,
            received(frames[0].to_vec()),
            now + Duration::from_secs(59),
        )
        .unwrap();
        assert_eq!(
            delivery.pending.as_ref().unwrap().frames[0].bytes.as_ptr(),
            pointer
        );
        assert_eq!(delivery.binding.expires, now + REASSEMBLY_LIFETIME);
        let mut decoder = Decoder::new(limits);
        assert!(
            decode(
                &routes,
                &mut decoder,
                delivery,
                now + Duration::from_secs(59)
            )
            .is_empty()
        );
        assert_eq!(decoder.deadline(), Some(now + REASSEMBLY_LIFETIME));
        decoder.expire(now + REASSEMBLY_LIFETIME);
        assert_eq!(decoder.deadline(), None);
        // An ID reused after expiry cannot complete with the old tail.
        let delivery = route(
            &routes,
            received(frames[0].to_vec()),
            now + REASSEMBLY_LIFETIME,
        )
        .unwrap();
        assert!(decode(&routes, &mut decoder, delivery, now + REASSEMBLY_LIFETIME).is_empty());
        let delivery = route(
            &routes,
            received(frames[1].to_vec()),
            now + REASSEMBLY_LIFETIME,
        )
        .unwrap();
        assert!(decode(&routes, &mut decoder, delivery, now + REASSEMBLY_LIFETIME).is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn orphan_admission_charges_storage_and_releases_on_expiry_or_pressure() {
        for reclaim in [false, true] {
            let allowance = 4096;
            let limits = Limits::new(allowance);
            let routes = Routes::new(2, limits.clone());
            let frames = frames(false, false);
            let now = Instant::now();
            // A short view can retain a much larger allocation.
            let mut block = vec![0; allowance];
            block[..frames[1].len()].copy_from_slice(&frames[1]);
            let frame = Received {
                bytes: Bytes::from(block).slice(..frames[1].len()),
                allocation_size: allowance,
                checksum_verified: false,
                udp_segment_size: None,
            };
            assert!(route(&routes, frame, now).is_none());
            assert!(limits.reserve(allowance, now).is_some());

            assert!(route(&routes, received(frames[1].to_vec()), now).is_none());
            assert!(limits.reserve(allowance, now).is_none());
            if reclaim {
                for shard in 0..2 {
                    routes.discard_oldest_pending(shard);
                }
                assert_eq!(Instant::now(), now);
            } else {
                routes.expire().await;
                assert_eq!(Instant::now(), now + REASSEMBLY_LIFETIME);
            }
            assert!(limits.reserve(allowance, Instant::now()).is_some());
        }
    }

    #[test]
    fn queued_old_fragments_cannot_poison_a_reused_id() {
        let limits = Limits::default();
        let routes = Routes::new(2, limits.clone());
        let mut decoder = Decoder::new(limits);
        let frames = frames(true, false);
        let now = Instant::now();
        let first = route(&routes, received(frames[0].to_vec()), now).unwrap();
        let stale = route(&routes, received(frames[0].to_vec()), now).unwrap();
        assert!(decode(&routes, &mut decoder, first, now).is_empty());
        let mut packets = Vec::new();
        for frame in &frames[1..] {
            let delivery = route(&routes, received(frame.to_vec()), now).unwrap();
            packets.extend(decode(&routes, &mut decoder, delivery, now));
        }
        assert_eq!(packets.len(), 1);
        let first = route(&routes, received(frames[0].to_vec()), now).unwrap();
        assert!(!Arc::ptr_eq(&stale.binding, &first.binding));
        assert!(decode(&routes, &mut decoder, first, now).is_empty());
        assert!(decode(&routes, &mut decoder, stale, now).is_empty());
        let mut packets = Vec::new();
        for frame in &frames[1..] {
            let delivery = route(&routes, received(frame.to_vec()), now).unwrap();
            packets.extend(decode(&routes, &mut decoder, delivery, now));
        }
        assert_eq!(packets.len(), 1);
    }

    #[test]
    fn conflicting_ipv6_first_headers_share_one_poisoned_binding() {
        let limits = Limits::default();
        let routes = Routes::new(2, limits.clone());
        let mut decoder = Decoder::new(limits);
        let frames = frames(true, false);
        let now = Instant::now();
        let first = route(&routes, received(frames[0].to_vec()), now).unwrap();
        let binding = first.binding.clone();
        assert!(decode(&routes, &mut decoder, first, now).is_empty());
        let mut conflicting = frames[0].to_vec();
        conflicting[40] = 6;
        let parsed = packet::parse(&conflicting).unwrap();
        let RouteKey::Fragment(key) = parsed.route().unwrap() else {
            panic!("fragment required")
        };
        let conflicting = routes
            .route(0, key, Some(0), received(conflicting), now)
            .unwrap();
        assert_eq!(conflicting.owner, 1);
        assert!(Arc::ptr_eq(&binding, &conflicting.binding));
        assert!(decode(&routes, &mut decoder, conflicting, now).is_empty());
        for frame in &frames[1..] {
            let delivery = route(&routes, received(frame.to_vec()), now).unwrap();
            assert!(decode(&routes, &mut decoder, delivery, now).is_empty());
        }
        assert!(decoder.retains(&binding));
        assert!(binding.active(now + Duration::from_secs(59)));
        assert!(!binding.active(now + REASSEMBLY_LIFETIME));
    }
}
