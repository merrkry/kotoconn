//! Shared accounting for incomplete IP datagrams. Successful assemblies teach
//! the working set; expiry and malformed traffic cannot inflate its allowance.
use kotoconn_protocol::queue::{Capacity, INITIAL_BYTES};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::{sync::watch, time::Instant};

#[derive(Clone)]
pub(crate) struct Limits(Arc<Shared>);

struct Shared {
    state: Mutex<State>,
    pressure: watch::Sender<()>,
}

struct State {
    used: usize,
    capacity: Capacity,
}

impl Default for Limits {
    fn default() -> Self {
        Self::new(4 * INITIAL_BYTES)
    }
}

impl Limits {
    pub fn new(initial: usize) -> Self {
        Self(Arc::new(Shared {
            state: Mutex::new(State {
                used: 0,
                capacity: Capacity::new(initial),
            }),
            pressure: watch::channel(()).0,
        }))
    }

    pub fn pressure(&self) -> watch::Receiver<()> {
        self.0.pressure.subscribe()
    }

    pub fn needs_headroom(&self, now: Instant) -> bool {
        let mut state = self.lock();
        let target = state.capacity.target(now);
        state.used > target - target / 4
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // SAFETY: Only accounting operations run under this lock. Poisoning
        // makes the shared ownership totals unreliable and must stop processing.
        self.0.state.lock().expect("reassembly accounting poisoned")
    }

    pub fn reserve(&self, bytes: usize, now: Instant) -> Option<Lease> {
        let mut lease = Lease {
            limits: self.clone(),
            bytes: 0,
        };
        lease.resize(bytes, now).then_some(lease)
    }
}

pub(crate) struct Lease {
    limits: Limits,
    bytes: usize,
}

impl Lease {
    pub fn resize(&mut self, bytes: usize, now: Instant) -> bool {
        let mut state = self.limits.lock();
        debug_assert!(state.used >= self.bytes);
        let rest = state.used - self.bytes;
        let Some(total) = rest.checked_add(bytes) else {
            return false;
        };
        if bytes > self.bytes && total > state.capacity.target(now) {
            drop(state);
            // Wake even idle owners. Their incomplete datagrams must not hold
            // the shared allowance until timeout while other workers drop input.
            self.limits.0.pressure.send_replace(());
            return false;
        }
        state.used = total;
        self.bytes = bytes;
        true
    }

    pub fn complete(&self, now: Instant) {
        self.limits.lock().capacity.complete(self.bytes, now);
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.limits.lock();
        debug_assert!(state.used >= self.bytes);
        state.used -= self.bytes;
    }
}
