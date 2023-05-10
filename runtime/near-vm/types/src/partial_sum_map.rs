//! Partial sum maps
//!
//! These maps allow you to efficiently store repeating sequences of a value. An example of such
//! sequence could be the list of locals for a webassembly function.
//!
//! Considering the locals example above, it might be  represented as a `u32` partial sum of the
//! local’s index. The locals between the index of the previous element and the current element
//! have the `WpType` type. So, given
//!
//! (0, u32), (10, u64), (15, f64)
//!
//! then 0th local would be a u32, locals `1..=10` – u64 and locals `11..=15` – f64.
//!
//! The type of a given index can be quickly found with a binary search over the partial sum
//! field.

/// A Map from keys to values that is able to efficiently store repeating occurences of the value.
///
/// This map can only be appended to.
#[derive(Debug)]
pub struct PartialSumMap<K, V> {
    /// Keys between ((keys[n-1] + 1) or 0) and keys[n] (both included) have value values[n]
    keys: Vec<K>,
    values: Vec<V>,
    size: K,
}

impl<K: Clone + Ord + num_traits::Unsigned + num_traits::CheckedAdd, V> PartialSumMap<K, V> {
    /// Create a new `PartialSumMap`.
    ///
    /// Does not allocate.
    pub fn new() -> Self {
        Self { keys: vec![], values: vec![], size: K::zero() }
    }

    /// Push `count` number of `value`s.
    ///
    /// `O(1)` amortized.
    pub fn push(&mut self, count: K, value: V) -> Result<(), Error> {
        if count != K::zero() {
            self.size = self.size.checked_add(&count).ok_or(Error::Overflow)?;
            self.keys.push(self.size.clone() - K::one());
            self.values.push(value);
        }
        Ok(())
    }

    /// Get the current maximum index that can be used with `find` for this map.
    ///
    /// Will return `None` if there are no elements in this map yet.
    ///
    /// `O(1)`
    pub fn max_index(&self) -> Option<K> {
        self.keys.last().cloned()
    }

    /// Get the current (virtual) size of this map. This is the sum of all `count` arguments passed to `push` until now.
    ///
    /// Note that the result can be greater than `usize::MAX` if eg. `K` is a BigInt type. Cast at your own risk.
    ///
    /// `O(1)`
    pub fn size(&self) -> &K {
        &self.size
    }

    /// Find the value by the index.
    ///
    /// This is a `O(n log n)` operation.
    pub fn find(&self, index: K) -> Option<&V> {
        match self.keys.binary_search(&index) {
            // If this index would be inserted at the end of the list, then the
            // index is out of bounds and we return a None.
            //
            // If `Ok` is returned we found the index exactly, or if `Err` is
            // returned the position is the one which is the least index
            // greater than `idx`, which is still the type of `idx` according
            // to our "compressed" representation. In both cases we access the
            // list at index `i`.
            Ok(i) | Err(i) => self.values.get(i),
        }
    }
}

/// Errors that occur when using PartialSumMap.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The partial sum has overflowed.
    Overflow,
}

impl std::error::Error for Error {}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Overflow => "partial sum overflow",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, PartialSumMap};

    #[test]
    fn empty_partial_map() {
        let map = PartialSumMap::<u32, u32>::new();
        assert_eq!(None, map.find(0));
        assert_eq!(0, *map.size());
    }

    #[test]
    fn basic_function() {
        let mut map = PartialSumMap::<u32, u32>::new();
        assert_eq!(None, map.max_index());
        assert_eq!(0, *map.size());
        for i in 0..10 {
            map.push(1, i).unwrap();
            assert_eq!(Some(i), map.max_index());
            assert_eq!(i + 1, *map.size());
        }
        for i in 0..10 {
            assert_eq!(Some(&i), map.find(i));
        }
        assert_eq!(None, map.find(10));
        assert_eq!(None, map.find(0xFFFF_FFFF));
    }

    #[test]
    fn zero_count() {
        let mut map = PartialSumMap::<u32, u32>::new();
        assert_eq!(Ok(()), map.push(0, 0));
        assert_eq!(None, map.max_index());
        assert_eq!(0, *map.size());
        assert_eq!(Ok(()), map.push(10, 42));
        assert_eq!(Some(9), map.max_index());
        assert_eq!(10, *map.size());
        assert_eq!(Ok(()), map.push(0, 43));
        assert_eq!(Some(9), map.max_index());
        assert_eq!(10, *map.size());
    }

    #[test]
    fn close_to_limit() {
        let mut map = PartialSumMap::<u32, u32>::new();
        assert_eq!(Ok(()), map.push(0xFFFF_FFFE, 42)); // we added values 0..=0xFFFF_FFFD
        assert_eq!(Some(&42), map.find(0xFFFF_FFFD));
        assert_eq!(None, map.find(0xFFFF_FFFE));

        assert_eq!(Err(Error::Overflow), map.push(100, 93)); // overflowing does not change the map
        assert_eq!(Some(&42), map.find(0xFFFF_FFFD));
        assert_eq!(None, map.find(0xFFFF_FFFE));

        assert_eq!(Ok(()), map.push(1, 322)); // we added value at index 0xFFFF_FFFE (which is the 0xFFFF_FFFFth value)
        assert_eq!(Some(&42), map.find(0xFFFF_FFFD));
        assert_eq!(Some(&322), map.find(0xFFFF_FFFE));
        assert_eq!(None, map.find(0xFFFF_FFFF));

        assert_eq!(Err(Error::Overflow), map.push(1, 1234)); // can't add any more stuff...
        assert_eq!(Some(&42), map.find(0xFFFF_FFFD));
        assert_eq!(Some(&322), map.find(0xFFFF_FFFE));
        assert_eq!(None, map.find(0xFFFF_FFFF));
    }
}
