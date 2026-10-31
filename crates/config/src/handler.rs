use std::{any::Any, fmt, marker::PhantomData, num::NonZeroU64, sync::Arc};

/// A policy function that native code calls. Policy registers the function when a
/// resource or decision references it, and the registration ends when the last clone
/// of every reference drops.
pub struct Handler<K> {
    id: NonZeroU64,
    registration: Arc<dyn Any + Send + Sync>,
    kind: PhantomData<fn() -> K>,
}

/// Selects a dialer for a flow, or rejects it.
pub enum Route {}

/// Resolves a dialer's server name.
pub enum Resolve {}

/// Answers a DNS request.
pub enum Dns {}

pub type RoutingHandler = Handler<Route>;

pub type ResolveHandler = Handler<Resolve>;

pub type DnsHandler = Handler<Dns>;

impl<K> Handler<K> {
    /// `registration` keeps the function registered until it drops.
    pub fn new(id: NonZeroU64, registration: Arc<dyn Any + Send + Sync>) -> Self {
        Self {
            id,
            registration,
            kind: PhantomData,
        }
    }

    pub fn id(&self) -> NonZeroU64 {
        self.id
    }
}

impl<K> Clone for Handler<K> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            registration: self.registration.clone(),
            kind: PhantomData,
        }
    }
}

impl<K> PartialEq for Handler<K> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<K> Eq for Handler<K> {}

impl<K> fmt::Debug for Handler<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Handler").field(&self.id).finish()
    }
}
