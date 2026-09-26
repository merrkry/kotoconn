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
    // Fixed-size chunks let LLVM vectorize the full words. Handle the tail
    // separately so odd payload lengths retain the same wire pattern.
    let (chunks, tail) = bytes.as_chunks_mut::<8>();
    let words = chunks.len();
    for (index, chunk) in chunks.iter_mut().enumerate() {
        let value = word(key.wrapping_add((offset / 8 + index) as u64)).to_le_bytes();
        *chunk = value;
    }
    let value = word(key.wrapping_add((offset / 8 + words) as u64)).to_le_bytes();
    tail.copy_from_slice(&value[..tail.len()]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_pattern_preserves_partial_words_and_block_offsets() {
        // Known wire bytes for seed 1, flow 2, sequence 3, direction 4.
        // Both peers use fill, so round trips alone cannot detect a pattern change.
        for (offset, expected) in [
            (
                0,
                [
                    121, 61, 70, 109, 170, 120, 55, 84, 72, 213, 155, 184, 167, 0, 131, 75, 115,
                    154, 83, 53, 8, 230, 141, 56, 242, 243, 61, 237, 219, 233, 232, 118, 63,
                ],
            ),
            (
                65536,
                [
                    214, 97, 79, 166, 86, 202, 132, 165, 229, 1, 63, 118, 179, 236, 168, 115, 92,
                    167, 247, 164, 220, 204, 207, 191, 208, 4, 103, 193, 176, 85, 165, 149, 188,
                ],
            ),
        ] {
            for length in 0..=expected.len() {
                let mut bytes = vec![0; length];
                fill(&mut bytes, 1, 2, 3, 4, offset);
                assert_eq!(
                    bytes,
                    expected[..length],
                    "offset {offset}, length {length}"
                );
            }
        }
    }
}
