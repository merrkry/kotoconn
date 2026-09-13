//! TCP payload ownership. The worker owns the index; immutable blocks can move
//! to/from the application without copying their payload or sharing the socket.
use bytes::Bytes;
use crossbeam_queue::SegQueue;
use smoltcp::socket::tcp::Buffer;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
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
        fill(&mut bytes);
        Bytes::from_owner(Allocation {
            bytes,
            pool: self.clone(),
            class: index,
        })
    }

    pub fn trim(&self) {
        for class in self.0.iter() {
            while class.blocks.pop().is_some() {
                class.cached.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

/// Sparse logical offsets preserve out-of-order data without allocating holes.
/// A block is immutable after insertion; replacing overlaps splits its views.
pub(crate) struct Storage {
    blocks: BTreeMap<usize, Bytes>,
    head: usize,
    length: usize,
    pub target: Arc<AtomicUsize>,
    pub outstanding: Arc<AtomicUsize>,
    pub released: Arc<AtomicUsize>,
    tx: bool,
    pool: Pool,
}

impl Storage {
    pub fn new(
        pool: Pool,
        target: Arc<AtomicUsize>,
        tx: bool,
        outstanding: Arc<AtomicUsize>,
        released: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            blocks: BTreeMap::new(),
            head: 0,
            length: 0,
            target,
            outstanding,
            released,
            tx,
            pool,
        }
    }

    pub fn push(&mut self, bytes: Bytes) -> usize {
        let len = bytes.len();
        if len != 0 {
            self.blocks.insert(self.head + self.length, bytes);
            self.length += len;
        }
        len
    }

    pub fn take(&mut self) -> Option<Bytes> {
        if self.length == 0 {
            return None;
        }
        // SAFETY: enqueue_unallocated only publishes ranges proven contiguous by TCP's assembler.
        let (start, mut bytes) = self
            .blocks
            .pop_first()
            .expect("TCP contiguous storage missing");
        debug_assert_eq!(start, self.head);
        let count = bytes.len().min(self.length);
        if count < bytes.len() {
            let suffix = bytes.split_off(count);
            self.blocks.insert(start + count, suffix);
        }
        self.head += count;
        self.length -= count;
        Some(bytes)
    }

    fn insert(&mut self, start: usize, bytes: Bytes) {
        let end = start + bytes.len();
        if let Some((&key, old)) = self.blocks.range(..start).next_back()
            && key + old.len() > start
        {
            let old = self.blocks.remove(&key);
            // SAFETY: The preceding lookup found this key; no other writer owns the map.
            let old = old.expect("TCP overlapping block disappeared");
            if key + old.len() > end {
                self.blocks.insert(end, old.slice(end - key..));
            }
            self.blocks.insert(key, old.slice(..start - key));
        }
        while let Some((&key, _)) = self.blocks.range(start..end).next() {
            // SAFETY: The lookup above found the key and this worker owns the map.
            let old = self
                .blocks
                .remove(&key)
                .expect("TCP overlapping block disappeared");
            if key + old.len() > end {
                self.blocks.insert(end, old.slice(end - key..));
            }
        }
        self.blocks.insert(start, bytes);
    }
}

impl Buffer for Storage {
    fn capacity(&self) -> usize {
        self.target.load(Ordering::Acquire)
    }

    fn len(&self) -> usize {
        self.length
    }

    fn clear(&mut self) {
        if self.tx {
            self.dequeue_allocated(self.length);
        }
        self.blocks.clear();
        self.head = 0;
        self.length = 0;
    }

    fn window(&self) -> usize {
        self.capacity().saturating_sub(if self.tx {
            self.outstanding.load(Ordering::Acquire)
        } else {
            self.length + self.outstanding.load(Ordering::Acquire)
        })
    }

    fn window_shift(&self) -> Option<u8> {
        Some(7)
    }

    fn enqueue_slice(&mut self, data: &[u8]) -> usize {
        let n = data.len().min(self.window()).min(65536);
        if n > 0 {
            self.push(self.pool.copy(&data[..n]));
        }
        n
    }

    fn dequeue_slice(&mut self, out: &mut [u8]) -> usize {
        let n = self.read_allocated(0, out);
        self.dequeue_allocated(n);
        n
    }

    fn get_allocated(&self, offset: usize, size: usize) -> &[u8] {
        if offset >= self.length {
            return &[];
        }
        let pos = self.head + offset;
        // SAFETY: Offsets below length belong to the contiguous published prefix.
        let (&start, bytes) = self
            .blocks
            .range(..=pos)
            .next_back()
            .expect("TCP block missing");
        let local = pos - start;
        debug_assert!(local < bytes.len());
        &bytes[local..bytes.len().min(local + size.min(self.length - offset))]
    }

    fn read_allocated(&mut self, offset: usize, out: &mut [u8]) -> usize {
        let mut n = 0;
        while n < out.len() {
            let part = self.get_allocated(offset + n, out.len() - n);
            if part.is_empty() {
                break;
            }
            out[n..n + part.len()].copy_from_slice(part);
            n += part.len();
        }
        n
    }

    fn write_unallocated(&mut self, offset: usize, data: &[u8]) -> usize {
        // TCP already checked the advertised window. A lowered target must still
        // accept in-flight data covered by a previous window advertisement.
        if !data.is_empty() {
            self.insert(self.head + self.length + offset, self.pool.copy(data));
        }
        data.len()
    }

    fn enqueue_unallocated(&mut self, count: usize) {
        self.length += count;
    }

    fn dequeue_allocated(&mut self, count: usize) {
        debug_assert!(count <= self.length);
        let end = self.head + count;
        while let Some((&start, _)) = self.blocks.first_key_value() {
            if start >= end {
                break;
            }
            // SAFETY: first_key_value found this entry; this worker exclusively owns it.
            let bytes = self.blocks.remove(&start).expect("TCP block missing");
            if start + bytes.len() > end {
                self.blocks.insert(end, bytes.slice(end - start..));
            }
        }
        self.head = end;
        self.length -= count;
        if self.tx && count != 0 {
            let old = self.outstanding.fetch_sub(count, Ordering::AcqRel);
            debug_assert!(old >= count);
            self.released.fetch_add(count, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(tx: bool) -> Storage {
        Storage::new(
            Pool::default(),
            Arc::new(AtomicUsize::new(128 * 1024)),
            tx,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        )
    }

    #[test]
    fn sparse_receive_preserves_overlaps_and_holes_after_consumption() {
        let mut rx = storage(false);
        rx.write_unallocated(10, b"klmnop");
        rx.write_unallocated(0, b"abcdefghij");
        rx.write_unallocated(8, b"IJKLM");
        rx.enqueue_unallocated(16);
        let mut result = Vec::new();
        while let Some(bytes) = rx.take() {
            result.extend_from_slice(&bytes);
        }
        assert_eq!(result, b"abcdefghIJKLMnop");

        // A far-away fragment allocates only its own payload, not the hole.
        rx.write_unallocated(65536, b"tail");
        assert_eq!(rx.blocks.values().map(Bytes::len).sum::<usize>(), 4);
        rx.write_unallocated(0, b"next");
        rx.enqueue_unallocated(4);
        assert_eq!(rx.take().unwrap(), b"next"[..]);
        assert_eq!(rx.blocks.first_key_value().unwrap().0 - rx.head, 65532);
    }

    #[test]
    fn immutable_transmit_blocks_survive_partial_ack_without_copying() {
        let mut tx = storage(true);
        let bytes = tx.pool.copy(b"abcdefgh");
        let pointer = bytes.as_ptr();
        tx.outstanding.store(bytes.len(), Ordering::Release);
        tx.push(bytes);
        assert_eq!(tx.get_allocated(0, 8).as_ptr(), pointer);
        assert_eq!(tx.get_allocated(2, 4), b"cdef");
        assert_eq!(tx.outstanding.load(Ordering::Acquire), 8);
        tx.dequeue_allocated(3);
        assert_eq!(tx.get_allocated(0, 8), b"defgh");
        assert_eq!(tx.outstanding.load(Ordering::Acquire), 5);
        tx.dequeue_allocated(5);
        assert!(tx.blocks.is_empty());
        assert_eq!(tx.released.load(Ordering::Acquire), 8);
    }

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
