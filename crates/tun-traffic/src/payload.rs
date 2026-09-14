//! Position-dependent data catches corruption, cross-flow delivery and block reuse.

fn word(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

pub fn fill(bytes: &mut [u8], seed: u64, flow: u64, sequence: u64, direction: u64, offset: usize) {
    // SAFETY: TCP uses 64 KiB blocks and UDP starts at offset zero.
    debug_assert!(offset.is_multiple_of(8));
    let key = word(seed ^ 0xa0761d6478bd642f)
        ^ word(flow ^ 0xe7037ed1a0b428db)
        ^ word(sequence ^ 0x8ebc6af09c88c6e3)
        ^ word(direction ^ 0x589965cc75374cc3);
    for (index, chunk) in bytes.chunks_mut(8).enumerate() {
        let value = word(key.wrapping_add((offset / 8 + index) as u64)).to_le_bytes();
        chunk.copy_from_slice(&value[..chunk.len()]);
    }
}
