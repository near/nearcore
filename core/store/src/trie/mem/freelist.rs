use std::ops::Deref;

/// A simple freelist for `Vec<u8>` that is used to avoid unnecessary
/// re-allocations.
pub struct VecU8Freelist {
    free: Vec<ReusableVecU8>,

    // There are two ways we detect that the freelist is not being used properly:
    //  1. If a ReusableVecU8 is dropped before being returned to the freelist,
    //     it will panic in debug.
    //  2. If the number of allocations exceeds the expected number of allocations,
    //     it will panic in debug and log an error in production the first time
    //     it happens.
    num_allocs: usize,
    expected_allocs: usize,
}

/// A wrapper around `Vec<u8>` that makes it harder to accidentally drop it
/// without returning it to the freelist.
pub struct ReusableVecU8 {
    vec: Vec<u8>,
}

impl Deref for ReusableVecU8 {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.vec
    }
}

impl ReusableVecU8 {
    /// Used to actually free the vector in the end.
    fn internal_free(&mut self) {
        std::mem::take(&mut self.vec);
    }

    pub fn vec_mut(&mut self) -> &mut Vec<u8> {
        &mut self.vec
    }
}

impl Drop for ReusableVecU8 {
    fn drop(&mut self) {
        // Do not drop without returning it to the freelist.
        debug_assert_eq!(self.vec.capacity(), 0);
    }
}

impl VecU8Freelist {
    /// Create a new freelist with an expected number of non-reused allocations.
    /// The expected number is used to detect incorrect usage of the freelist.
    pub fn new(expected_allocs: usize) -> Self {
        Self { free: Vec::with_capacity(expected_allocs), num_allocs: 0, expected_allocs }
    }

    /// Returns a byte array, by either reusing one or allocating a new one
    /// if there's none to reuse.
    pub fn alloc(&mut self) -> ReusableVecU8 {
        if let Some(vec) = self.free.pop() {
            vec
        } else {
            self.num_allocs += 1;
            if self.num_allocs == self.expected_allocs {
                // If this triggers, it means that we're not using the freelist properly.
                if cfg!(debug_assertions) {
                    panic!("Too many freelist allocations; expected {}", self.expected_allocs);
                } else {
                    tracing::error!(target: "memtrie", "Too many freelist allocations; expected {}", self.expected_allocs);
                }
            }
            ReusableVecU8 { vec: Vec::new() }
        }
    }

    /// Returns a byte array to the freelist.
    pub fn free(&mut self, mut vec: ReusableVecU8) {
        vec.vec.clear();
        self.free.push(vec);
    }
}

impl Drop for VecU8Freelist {
    fn drop(&mut self) {
        // This is where the returned byte arrays are actually freed.
        for mut vec in self.free.drain(..) {
            vec.internal_free();
        }
    }
}
