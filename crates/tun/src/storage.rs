//! Packet boundaries over shared blocks. Each producer keeps its own arena so
//! FIFO consumption releases old blocks without unrelated flows pinning them.
//! Reserve queue allowance before encoding on paths that can drop packets.
use bytes::{Bytes, BytesMut};

const BLOCK: usize = 16 * 1024;

#[derive(Default)]
pub(crate) struct PacketArena {
    tail: BytesMut,
    pool: crate::pool::Pool,
}

impl PacketArena {
    pub fn with_pool(pool: crate::pool::Pool) -> Self {
        Self {
            tail: BytesMut::new(),
            pool,
        }
    }

    /// Encode directly into a contiguous slice, then publish an immutable view.
    /// Small packets share a block; large packets reuse a power-of-two pool block.
    pub fn encode<R>(&mut self, len: usize, emit: impl FnOnce(&mut [u8]) -> R) -> (R, Bytes) {
        if len > BLOCK / 2 {
            return self.pool.encode(len, emit);
        }

        if self.tail.capacity() < len {
            // Replace the tail rather than reallocating a block with live views.
            self.tail = BytesMut::with_capacity(BLOCK);
        }
        debug_assert!(self.tail.is_empty());
        self.tail.resize(len, 0);
        let result = emit(&mut self.tail);
        (result, self.tail.split().freeze())
    }
}

/// Conservative packing charge, excluding descriptor metadata. A retired block
/// is over half full because packed packets are at most half a block. Large
/// packets occupy a power-of-two allocation smaller than twice their length. FIFO
/// consumers can additionally retain a partial head block and the producer's
/// current tail. This assumes every published view is consumed in order;
/// repeated admission drops must not advance the arena. This is queue
/// accounting, not a process memory quota.
pub(crate) fn charge(len: usize) -> usize {
    2 * len
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_packets_share_blocks_and_survive_growth_and_producer_drop() {
        let mut arena = PacketArena::default();
        let packets: Vec<_> = (0..1024)
            .map(|index| arena.encode(40, |bytes| bytes.fill((index % 251) as u8)).1)
            .collect();
        let blocks = 1 + packets
            .windows(2)
            .filter(|pair| pair[1].as_ptr() as usize != pair[0].as_ptr() as usize + pair[0].len())
            .count();
        assert_eq!(blocks, (1024usize).div_ceil(BLOCK / 40));
        let large = arena.encode(65535, |bytes| bytes.fill(255)).1;
        drop(arena);
        for (index, packet) in packets.into_iter().enumerate() {
            assert_eq!(packet.as_ref(), &[((index % 251) as u8); 40]);
        }
        assert!(large.iter().all(|byte| *byte == 255));
    }
}
