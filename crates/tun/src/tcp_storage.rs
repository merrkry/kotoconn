//! TCP payload ownership. The worker owns the index; immutable blocks can move
//! to/from the application without copying their payload or sharing the socket.
use crate::pool::Pool;
use bytes::Bytes;
use smoltcp::socket::tcp::Buffer;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

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
    scratch: Bytes,
    pub source: Option<Bytes>,
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
            scratch: Bytes::new(),
            source: None,
        }
    }

    pub fn segments(&self, range: std::ops::Range<usize>) -> Vec<Bytes> {
        let mut position = self.head + range.start;
        let end = self.head + range.end;
        let mut result = Vec::new();
        while position < end {
            // SAFETY: dispatch selects a range inside the contiguous TX prefix.
            let (&start, bytes) = self
                .blocks
                .range(..=position)
                .next_back()
                .expect("TX segment");
            let count = (end - position).min(start + bytes.len() - position);
            debug_assert!(count != 0);
            result.push(bytes.slice(position - start..position - start + count));
            position += count;
        }
        result
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

    fn segment_len(&self, offset: usize, size: usize) -> usize {
        let mut position = offset;
        let end = offset + size.min(self.length.saturating_sub(offset));
        // Bound descriptor work and the number of vectors in one syscall.
        for _ in 0..64 {
            if position == end {
                break;
            }
            let part = self.get_allocated(position, end - position);
            debug_assert!(!part.is_empty());
            position += part.len();
        }
        position - offset
    }

    fn get_segment(&mut self, offset: usize, size: usize) -> &[u8] {
        let wanted = size.min(self.length.saturating_sub(offset));
        if self.get_allocated(offset, wanted).len() == wanted {
            return self.get_allocated(offset, wanted);
        }
        // Only a segment crossing a block boundary needs gathering. Full MSS
        // packets let the Linux writer keep coalescing across application writes.
        self.scratch = self.pool.fill(wanted, |out| {
            let mut n = 0;
            while n < wanted {
                let bytes = self.get_allocated(offset + n, wanted - n);
                debug_assert!(!bytes.is_empty());
                out[n..n + bytes.len()].copy_from_slice(bytes);
                n += bytes.len();
            }
        });
        &self.scratch
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
            let bytes = self
                .source
                .as_ref()
                .and_then(|source| crate::storage::view(source, data))
                .unwrap_or_else(|| self.pool.copy(data));
            self.insert(self.head + self.length + offset, bytes);
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
        if self.length == 0 {
            self.scratch = Bytes::new();
        }
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
    fn receive_views_keep_the_frame_alive_through_overlaps_and_consumption() {
        let mut rx = storage(false);
        let source = rx.pool.copy(b"headerabcdefghij");
        let pointer = source.as_ptr();
        rx.source = Some(source.clone());
        rx.write_unallocated(0, &source[6..]);
        rx.source = None;
        drop(source);
        rx.enqueue_unallocated(10);
        let bytes = rx.take().unwrap();
        assert_eq!(bytes.as_ptr() as usize, pointer as usize + 6);
        assert_eq!(bytes, b"abcdefghij"[..]);
        rx.write_unallocated(0, b"next");
        rx.enqueue_unallocated(4);
        assert_eq!(rx.take().unwrap(), b"next"[..]);
        assert_eq!(bytes, b"abcdefghij"[..]);
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
    fn wire_segments_cross_application_blocks_without_shortening() {
        let mut tx = storage(true);
        tx.push(tx.pool.copy(&[1; 8192]));
        tx.push(tx.pool.copy(&[2; 8192]));
        let segment = tx.get_segment(8000, 1460);
        assert_eq!(segment.len(), 1460);
        assert_eq!(&segment[..192], &[1; 192]);
        assert_eq!(&segment[192..], &[2; 1268]);
        assert_eq!(tx.get_allocated(8000, 1460), &[1; 192]);
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
}
