use exonum_sodiumoxide::crypto::hash::sha256::hash;

/// Endless iterator that can be used to generated pseudo-random bytes deterministically from the given
/// seed. It uses sha256 to generate batches of bytes.
pub struct RandIterator {
    buffer: Vec<u8>,
    offset: u64,
}

impl RandIterator {
    /// Create `RandIterator` from the given `seed`.
    pub fn new(seed: &[u8]) -> Self {
        Self { buffer: hash(seed).as_ref().to_vec(), offset: 0 }
    }
}

impl Iterator for RandIterator {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.buffer[self.offset as usize];
        self.offset += 1;
        if self.offset == self.buffer.len() as _ {
            self.buffer = hash(self.buffer.as_ref()).as_ref().to_vec();
            self.offset = 0;
        }
        Some(result)
    }
}
