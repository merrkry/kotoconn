//! Reusable initialized blocks shared by payload and packet producers.
use bytes::Bytes;
use crossbeam_queue::SegQueue;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const CLASSES: usize = 17;
const CACHED_PER_CLASS: usize = 32;

#[derive(Default)]
struct Class {
    blocks: SegQueue<Vec<u8>>,
    cached: AtomicUsize,
}

/// Free blocks are a bounded cache, never an admission limit for live data.
#[derive(Clone)]
pub(crate) struct Pool(Arc<[Class; CLASSES]>);

impl Default for Pool {
    fn default() -> Self {
        Self(Arc::new(std::array::from_fn(|_| Class::default())))
    }
}

struct Allocation {
    bytes: Vec<u8>,
    pool: Pool,
    class: usize,
}

impl AsRef<[u8]> for Allocation {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Allocation {
    fn drop(&mut self) {
        let class = &self.pool.0[self.class];
        if class
            .cached
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < CACHED_PER_CLASS).then_some(n + 1)
            })
            .is_ok()
        {
            class.blocks.push(std::mem::take(&mut self.bytes));
        }
    }
}

impl Pool {
    pub fn copy(&self, bytes: &[u8]) -> Bytes {
        self.fill(bytes.len(), |out| out.copy_from_slice(bytes))
    }

    pub fn fill(&self, length: usize, fill: impl FnOnce(&mut [u8])) -> Bytes {
        self.encode(length, fill).1
    }

    pub fn encode<R>(&self, length: usize, fill: impl FnOnce(&mut [u8]) -> R) -> (R, Bytes) {
        // SAFETY: Callers pass at most one IP-sized segment or one application chunk.
        assert!(length <= 65536);
        let capacity = length.max(256).next_power_of_two();
        let index = capacity.trailing_zeros() as usize;
        let class = &self.0[index];
        let mut bytes = match class.blocks.pop() {
            Some(bytes) => {
                class.cached.fetch_sub(1, Ordering::Relaxed);
                bytes
            }
            None => Vec::with_capacity(capacity),
        };
        bytes.resize(length, 0);
        let result = fill(&mut bytes);
        (
            result,
            Bytes::from_owner(Allocation {
                bytes,
                pool: self.clone(),
                class: index,
            }),
        )
    }

    pub fn trim(&self) {
        for class in self.0.iter() {
            while class.blocks.pop().is_some() {
                class.cached.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_reuses_storage_only_after_last_view_is_dropped() {
        let pool = Pool::default();
        let bytes = pool.copy(&[7; 1000]);
        let view = bytes.slice(123..456);
        let ptr = bytes.as_ptr();
        drop(bytes);
        assert_eq!(pool.0[10].cached.load(Ordering::Relaxed), 0);
        assert_eq!(view, [7; 333][..]);
        drop(view);
        let reused = pool.copy(&[9; 1000]);
        assert_eq!(reused.as_ptr(), ptr);
        assert_eq!(reused, [9; 1000][..]);
        drop(reused);
        pool.trim();
        assert_eq!(pool.0[10].cached.load(Ordering::Relaxed), 0);
    }
}
