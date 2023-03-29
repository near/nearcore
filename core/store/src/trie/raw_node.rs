use borsh::{BorshDeserialize, BorshSerialize};

use near_primitives::hash::CryptoHash;
use near_primitives::state::ValueRef;

/// Trie node with memory cost of its subtree.
///
/// memory_usage is serialized, stored and contributes to hash.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct RawTrieNodeWithSize {
    pub node: RawTrieNode,
    pub(super) memory_usage: u64,
}

/// Trie node.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum RawTrieNode {
    /// Leaf(key, value_length, value_hash)
    Leaf(Vec<u8>, ValueRef),
    /// Branch(children)
    BranchNoValue(Children),
    /// Branch(children, value)
    BranchWithValue(ValueRef, Children),
    /// Extension(key, child)
    Extension(Vec<u8>, CryptoHash),
}

impl RawTrieNode {
    #[inline]
    pub fn branch(children: Children, value: Option<ValueRef>) -> Self {
        match value {
            Some(value) => Self::BranchWithValue(value, children),
            None => Self::BranchNoValue(children),
        }
    }
}

/// Children of a branch node.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Children<T = CryptoHash>(pub [Option<T>; 16]);

impl<T> Children<T> {
    /// Iterates over existing children; `None` entries are omitted.
    #[inline]
    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (u8, &'a T)> {
        self.0.iter().enumerate().flat_map(|(i, el)| Some(i as u8).zip(el.as_ref()))
    }
}

impl<T> Default for Children<T> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<T> std::ops::Index<u8> for Children<T> {
    type Output = Option<T>;
    fn index(&self, index: u8) -> &Option<T> {
        &self.0[usize::from(index)]
    }
}

impl<T> std::ops::IndexMut<u8> for Children<T> {
    fn index_mut(&mut self, index: u8) -> &mut Option<T> {
        &mut self.0[usize::from(index)]
    }
}

impl<T: BorshSerialize> BorshSerialize for Children<T> {
    fn serialize<W: std::io::Write>(&self, wr: &mut W) -> std::io::Result<()> {
        let mut bitmap: u16 = 0;
        let mut pos: u16 = 1;
        for child in self.0.iter() {
            if child.is_some() {
                bitmap |= pos
            }
            pos <<= 1;
        }
        bitmap.serialize(wr)?;
        self.0.iter().flat_map(Option::as_ref).map(|child| child.serialize(wr)).collect()
    }
}

impl<T: BorshDeserialize> BorshDeserialize for Children<T> {
    fn deserialize_reader<R: std::io::Read>(rd: &mut R) -> std::io::Result<Self> {
        let mut bitmap = u16::deserialize_reader(rd)?;
        let mut children = Self::default();
        while bitmap != 0 {
            let idx = bitmap.trailing_zeros() as u8;
            bitmap &= bitmap - 1;
            children[idx] = Some(T::deserialize_reader(rd)?);
        }
        Ok(children)
    }
}

mod children {
    struct Debug<'a, T>((u8, &'a T));

    impl<T: std::fmt::Debug> std::fmt::Debug for Debug<'_, T> {
        fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(fmtr, "{}: {:?}", self.0 .0, self.0 .1)
        }
    }

    impl<T: std::fmt::Debug> std::fmt::Debug for super::Children<T> {
        fn fmt(&self, fmtr: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            fmtr.debug_list().entries(self.iter().map(Debug)).finish()
        }
    }
}

#[test]
fn test_encode_decode() {
    #[track_caller]
    fn test(node: RawTrieNode, encoded: &[u8]) {
        let node = RawTrieNodeWithSize { node, memory_usage: 42 };
        let mut buf = node.try_to_vec().unwrap();
        assert_eq!(encoded, buf.as_slice());
        assert_eq!(node, RawTrieNodeWithSize::try_from_slice(&buf).unwrap());

        // Test that adding garbage at the end fails decoding.
        buf.push(b'!');
        let got = RawTrieNodeWithSize::try_from_slice(&buf);
        assert!(got.is_err(), "got: {got:?}");
    }

    let value = ValueRef { length: 3, hash: CryptoHash::hash_bytes(&[123, 245, 255]) };
    let node = RawTrieNode::Leaf(vec![1, 2, 3], value.clone());
    #[rustfmt::skip]
    let encoded = [
        /* node type: */ 0,
        /* key: */ 3, 0, 0, 0, 1, 2, 3,
        /* value: */ 3, 0, 0, 0, 194, 40, 8, 24, 64, 219, 69, 132, 86, 52, 110, 175, 57, 198, 165, 200, 83, 237, 211, 11, 194, 83, 251, 33, 145, 138, 234, 226, 7, 242, 186, 73,
        /* memory usage: */ 42, 0, 0, 0, 0, 0, 0, 0,
    ];
    test(node, &encoded);

    let mut children = Children::default();
    children[3] = Some(CryptoHash([1; 32]));
    children[5] = Some(CryptoHash([2; 32]));
    let node = RawTrieNode::BranchNoValue(children);
    #[rustfmt::skip]
    let encoded = [
        /* node type: */ 1,
        /* bitmask: */ 40, 0,
        /* child: */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        /* child: */ 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        /* memory usage: */ 42, 0, 0, 0, 0, 0, 0, 0
    ];
    test(node, &encoded);

    let mut children = Children::default();
    children[0] = Some(CryptoHash([1; 32]));
    let node = RawTrieNode::BranchNoValue(children);
    #[rustfmt::skip]
    let encoded = [
        /* node type: */ 1,
        /* bitmask: */ 1, 0,
        /* child: */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        /* memory usage: */ 42, 0, 0, 0, 0, 0, 0, 0
    ];
    test(node, &encoded);

    let mut children = Children::default();
    children[15] = Some(CryptoHash([1; 32]));
    let node = RawTrieNode::BranchNoValue(children);
    #[rustfmt::skip]
    let encoded = [
        /* node type: */ 1,
        /* bitmask: */ 0, 128,
        /* child: */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        /* memory usage: */ 42, 0, 0, 0, 0, 0, 0, 0
    ];
    test(node, &encoded);

    let mut children = Children::default();
    children[3] = Some(CryptoHash([1; 32]));
    let node = RawTrieNode::BranchWithValue(value, children);
    #[rustfmt::skip]
    let encoded = [
        /* node type: */ 2,
        /* value: */ 3, 0, 0, 0, 194, 40, 8, 24, 64, 219, 69, 132, 86, 52, 110, 175, 57, 198, 165, 200, 83, 237, 211, 11, 194, 83, 251, 33, 145, 138, 234, 226, 7, 242, 186, 73,
        /* bitmask: */ 8, 0,
        /* child: */ 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        /* memory usage: */ 42, 0, 0, 0, 0, 0, 0, 0
    ];
    test(node, &encoded);

    let node = RawTrieNode::Extension(vec![123, 245, 255], super::Trie::EMPTY_ROOT);
    #[rustfmt::skip]
    let encoded = [
        /* node type: */ 3,
        /* key: */ 3, 0, 0, 0, 123, 245, 255,
        /* node: */ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        /* memory usage: */ 42, 0, 0, 0, 0, 0, 0, 0
    ];
    test(node, &encoded);
}
