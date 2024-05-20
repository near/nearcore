use super::arena::Arena;
use super::freelist::{ReusableVecU8, VecU8Freelist};
use super::node::MemTrieNodeId;
use crate::trie::mem::node::InputMemTrieNode;
use crate::NibbleSlice;
use elastic_array::ElasticArray4;
use near_primitives::state::FlatStateValue;

/// Algorithm to construct a trie from a given stream of sorted leaf values.
///
/// This is a bottom-up algorithm that avoids constructing trie nodes until
/// they are complete. The algorithm is only intended to be used when
/// constructing the initial trie; it does not work at all for incremental
/// updates.
///
/// The algorithm maintains a list of segments, where each segment represents
/// a subpath of the last key (where path means a sequence of nibbles), and
/// the segments' subpaths join together to form the last key.
//
// To understand the algorithm, conceptually imagine a tree like this:
//
//                X
//               / \
//              O   X
//                 / \
//                /   \
//               O     \
//              / \     \
//             O   O     X
//                      / \
//                     O   X
// Nodes marked with X are `TrieConstructionSegment`s, whereas nodes marked
// with O are already constructed, `MemTrieNodeId`s. As we build out the trie,
// X's will become O's. A `TrieConstructionSegment` represents an `X` along
// with the tail segment(s) immediately below it (as opposed to above). So
// the first segment in the above drawing would represent:
//
//                X
//               / \
//              O
//
// this is a branch node (possibly with a leaf value), with a single child
// on the left (so `children` array has a single element), with the current
// `trail` being the edge sticking out to the right.
//
// Notice that the X's always form the rightmost path of the trie. This is
// because anything to the left have already been determined so they are
// immutable. The algorithm assumes that keys are ingested in order, so we
// are always constructing further to the right. Suppose we encounter a new
// key that shares a prefix with the first segment but diverges in the middle
// of the second segment, like so:
//
//                X                                 X
//               / \                               / \
//              O   X                             O   X
//                 / \                               / \
//                /   \ <--- diverges here          /   X----+
//               O     \                           O     \    \
//              / \     \                         / \     \    \
//             O   O     X         ====>         O   O     O    X
//                      / \                               / \
//                     O   X                             O   O
//
// As the bottom two segments are no longer part of the right-most path, they
// are converted to concrete TrieMemNodeId's.
pub struct TrieConstructor<'a, A: Arena> {
    arena: &'a mut A,
    segments: Vec<TrieConstructionSegment>,
    trail_freelist: VecU8Freelist,
}

/// A segment of the rightmost path of the trie under construction, as
/// described above. Ultimately, a segment is turned into a node when it's
/// no longer part of the rightmost path.
struct TrieConstructionSegment {
    /// Always determined at the beginning. If true, this is a branch node,
    /// possibly with value; if not, this is either leaf or extension node.
    is_branch: bool,
    // The trail, an edge below this node. If this is a branch node,
    // it is the rightmost child edge. It is an encoded NibbleSlice.
    // We use a freelist here because this would otherwise be frequently
    // allocated and deallocated and slow down construction significantly.
    trail: ReusableVecU8,
    // If present, it is either a Leaf node or BranchWithValue.
    value: Option<FlatStateValue>,
    // Only used if is_branch is true. The children that are already
    // constructed. The last child currently being constructed is not in here.
    children: ElasticArray4<(u8, MemTrieNodeId)>,
    // Only used for extension nodes; the child that is already constructed.
    child: Option<MemTrieNodeId>,
}

impl TrieConstructionSegment {
    /// Prepares a segment that represents a branch node, possibly with value.
    fn new_branch(initial_trail: ReusableVecU8, value: Option<FlatStateValue>) -> Self {
        Self {
            is_branch: true,
            trail: initial_trail,
            value,
            children: Default::default(),
            child: None,
        }
    }

    /// Prepares a segment that represents an extension node.
    fn new_extension(trail: ReusableVecU8) -> Self {
        let nibbles = NibbleSlice::from_encoded(&trail);
        assert!(!nibbles.1); // nibble slice is not leaf
        assert!(!nibbles.0.is_empty()); // extension nibbles cannot be empty
        Self { is_branch: false, trail, value: None, children: Default::default(), child: None }
    }

    /// Prepares a segment that represents a leaf node.
    fn new_leaf(trail: ReusableVecU8, value: FlatStateValue) -> Self {
        let nibbles = NibbleSlice::from_encoded(&trail);
        assert!(nibbles.1);
        Self {
            is_branch: false,
            trail,
            value: Some(value),
            children: Default::default(),
            child: None,
        }
    }

    fn is_leaf(&self) -> bool {
        self.value.is_some() && !self.is_branch
    }

    fn to_node(&self, arena: &mut impl Arena) -> MemTrieNodeId {
        let input_node = if self.is_branch {
            assert!(!self.children.is_empty());
            assert!(self.child.is_none());
            let mut children = [None; 16];
            for (i, child) in self.children.iter() {
                children[*i as usize] = Some(*child);
            }
            if let Some(value) = &self.value {
                InputMemTrieNode::BranchWithValue { children, value }
            } else {
                InputMemTrieNode::Branch { children }
            }
        } else if let Some(value) = &self.value {
            assert!(self.child.is_none());
            assert!(self.children.is_empty());
            InputMemTrieNode::Leaf { value, extension: &self.trail }
        } else {
            assert!(self.child.is_some());
            assert!(self.children.is_empty());
            InputMemTrieNode::Extension { extension: &self.trail, child: self.child.unwrap() }
        };
        MemTrieNodeId::new(arena, input_node)
    }
}

/// A helper trait to make the construction code more readable.
///
/// Whenever we encode nibbles to vector, we want to use a vector from the
/// freelist; otherwise allocation is quite slow.
trait NibblesHelper {
    fn encode_to_vec(&self, freelist: &mut VecU8Freelist, is_leaf: bool) -> ReusableVecU8;
    fn encode_leftmost_to_vec(
        &self,
        freelist: &mut VecU8Freelist,
        len: usize,
        is_leaf: bool,
    ) -> ReusableVecU8;
}

impl NibblesHelper for NibbleSlice<'_> {
    fn encode_to_vec(&self, freelist: &mut VecU8Freelist, is_leaf: bool) -> ReusableVecU8 {
        let mut vec = freelist.alloc();
        self.encode_to(is_leaf, vec.vec_mut());
        vec
    }

    fn encode_leftmost_to_vec(
        &self,
        freelist: &mut VecU8Freelist,
        len: usize,
        is_leaf: bool,
    ) -> ReusableVecU8 {
        let mut vec = freelist.alloc();
        self.encode_leftmost_to(len, is_leaf, vec.vec_mut());
        vec
    }
}

impl<'a, A: Arena> TrieConstructor<'a, A> {
    pub fn new(arena: &'a mut A) -> Self {
        // We should only have as many allocations as the number of segments
        // alive, which is at most the length of keys. We give a generous
        // margin on top of that. If this is exceeded in production, an error
        // is printed; if exceeded in debug, it panics.
        const EXPECTED_FREELIST_MAX_ALLOCATIONS: usize = 4096;
        Self {
            arena,
            segments: vec![],
            trail_freelist: VecU8Freelist::new(EXPECTED_FREELIST_MAX_ALLOCATIONS),
        }
    }

    fn recycle_segment(&mut self, segment: TrieConstructionSegment) {
        self.trail_freelist.free(segment.trail);
    }

    /// Encodes the bottom-most segment into a node, and pops it off the stack.
    fn pop_segment(&mut self) {
        let segment = self.segments.pop().unwrap();
        let node = segment.to_node(self.arena);
        self.recycle_segment(segment);
        let parent = self.segments.last_mut().unwrap();
        if parent.is_branch {
            parent.children.push((NibbleSlice::from_encoded(&parent.trail).0.at(0), node));
        } else {
            assert!(parent.child.is_none());
            parent.child = Some(node);
        }
    }

    /// Adds a leaf to the trie. The key must be greater than all previous keys
    /// inserted.
    pub fn add_leaf(&mut self, key: &[u8], value: FlatStateValue) {
        let mut nibbles = NibbleSlice::new(key);
        let mut i = 0;
        // We'll go down the segments to find where our nibbles deviate.
        // If the deviation happens in the middle of a segment, we would split
        // that segment. Then, we pop off any old segments and push new segments
        // past the deviation point.
        while i < self.segments.len() {
            // It's not possible to exhaust the nibbles we're inserting while
            // we're iterating through the segments, because that would mean
            // this new key is a prefix of the previous key, which violates the
            // assumed key ordering.
            assert!(!nibbles.is_empty());

            let segment = &self.segments[i];
            let (extension_nibbles, _) = NibbleSlice::from_encoded(&segment.trail);
            let common_prefix_len = nibbles.common_prefix(&extension_nibbles);
            if common_prefix_len == extension_nibbles.len() {
                // This segment entirely matches a prefix of the nibbles we have
                // so we continue matching.
                nibbles = nibbles.mid(common_prefix_len);
                i += 1;
                continue;
            }

            // At this point, we have a deviation somewhere in this segment.
            // We can already pop off all the extra segments below it, as they
            // have no chance to be relevant to the leaf we're inserting.
            while i < self.segments.len() - 1 {
                self.pop_segment();
            }

            // If the deviation happens in the middle of a segment, i.e. we have
            // a non-zero common prefix, split that first. Such a segment is
            // definitely an extension or leaf segment (since segment has more
            // than 1 nibble). To do this split, we insert an extension segment
            // on top for the common prefix, and shorten the current segment's
            // trail.
            if common_prefix_len > 0 {
                let mut segment = self.segments.pop().unwrap();
                assert!(!segment.is_branch);
                let (extension_nibbles, was_leaf) = NibbleSlice::from_encoded(&segment.trail);
                assert_eq!(was_leaf, segment.is_leaf());
                assert_eq!(was_leaf, segment.child.is_none());

                let top_segment = TrieConstructionSegment::new_extension(
                    extension_nibbles.encode_leftmost_to_vec(
                        &mut self.trail_freelist,
                        common_prefix_len,
                        false,
                    ),
                );
                let new_trail = extension_nibbles
                    .mid(common_prefix_len)
                    .encode_to_vec(&mut self.trail_freelist, was_leaf);
                self.trail_freelist.free(std::mem::replace(&mut segment.trail, new_trail));
                self.segments.push(top_segment);
                self.segments.push(segment);
                nibbles = nibbles.mid(common_prefix_len);
            }

            // At this point, we know that the last segment deviates from our
            // leaf and the deviation point is the beginning of the segment.
            if self.segments.last().unwrap().is_branch {
                // If the existing segment is a branch, we simply add another
                // case of the branch.
                let segment = self.segments.last_mut().unwrap();
                let new_trail = nibbles.encode_leftmost_to_vec(&mut self.trail_freelist, 1, false);
                self.trail_freelist.free(std::mem::replace(&mut segment.trail, new_trail));
                nibbles = nibbles.mid(1);
                break;
            } else {
                // Otherwise, the existing segment is an extension or leaf. We
                // need to split the segment so that the first nibble is
                // converted to a branch.
                let mut segment = self.segments.pop().unwrap();
                let (extension_nibbles, was_leaf) = NibbleSlice::from_encoded(&segment.trail);
                assert_eq!(was_leaf, segment.is_leaf());
                assert_eq!(was_leaf, segment.child.is_none());

                // This is the branch node that handles the deviation. The trail
                // for this branch node begins with the old trail's first nibble.
                // We'll insert the new leaf later.
                let mut top_segment = TrieConstructionSegment::new_branch(
                    extension_nibbles.encode_leftmost_to_vec(&mut self.trail_freelist, 1, false),
                    None,
                );
                if extension_nibbles.len() > 1 || was_leaf {
                    // If the old segment had more than 1 nibble, we need to
                    // keep that segment except without the first nibble.
                    // Similarly, if the old segment had just 1 nibble but was a
                    // leaf, we still need to keep the leaf but now with empty
                    // trail on the leaf.
                    let new_trail =
                        extension_nibbles.mid(1).encode_to_vec(&mut self.trail_freelist, was_leaf);
                    self.trail_freelist.free(std::mem::replace(&mut segment.trail, new_trail));
                    self.segments.push(top_segment);
                    self.segments.push(segment);
                    // The bottom segment is no longer relevant to our new leaf,
                    // so pop that off.
                    self.pop_segment();
                } else {
                    // If the old segment was an extension with just 1 nibble,
                    // then that segment is no longer needed. We can add the old
                    // extension segment's child directly to the branch node.
                    top_segment.children.push((extension_nibbles.at(0), segment.child.unwrap()));
                    self.segments.push(top_segment);
                    self.recycle_segment(segment);
                }
                // At this point we have popped the old case of the branch node,
                // so we advance the branch node to point to our new leaf
                // segment that we'll add below.
                let segment = self.segments.last_mut().unwrap();
                let new_trail = nibbles.encode_leftmost_to_vec(&mut self.trail_freelist, 1, false);
                self.trail_freelist.free(std::mem::replace(&mut segment.trail, new_trail));
                nibbles = nibbles.mid(1);
                break;
            }
        }
        // When we exit the loop, either we exited because we ran out of segments
        // (in which case this leaf contains the previous leaf as a prefix) or we
        // exited in the middle and we've just added a new branch.
        if !self.segments.is_empty() && self.segments.last().unwrap().is_leaf() {
            // This is the case where we ran out of segments. We definitely have
            // some non-empty nibbles left or else we're trying insert a
            // duplicate key.
            assert!(!nibbles.is_empty());
            // In order for a leaf node to have another leaf below it, it needs
            // to be converted to a branch node with value.
            let mut segment = self.segments.pop().unwrap();
            let (extension_nibbles, was_leaf) = NibbleSlice::from_encoded(&segment.trail);
            assert!(was_leaf);
            if !extension_nibbles.is_empty() {
                // If the original leaf node had an extension within it, we need
                // to create an extension above the branch node.
                let top_segment = TrieConstructionSegment::new_extension(
                    extension_nibbles.encode_to_vec(&mut self.trail_freelist, false),
                );
                self.segments.push(top_segment);
            }
            // Now let's construct our branch node, and add our new leaf node below it.
            let mid_segment = TrieConstructionSegment::new_branch(
                nibbles.encode_leftmost_to_vec(&mut self.trail_freelist, 1, false),
                std::mem::take(&mut segment.value),
            );
            let bottom_segment = TrieConstructionSegment::new_leaf(
                nibbles.mid(1).encode_to_vec(&mut self.trail_freelist, true),
                value,
            );
            self.segments.push(mid_segment);
            self.segments.push(bottom_segment);
            self.recycle_segment(segment);
        } else {
            // Otherwise we're at one branch of a branch node (or we're at root),
            // so just append the leaf.
            let segment = TrieConstructionSegment::new_leaf(
                nibbles.encode_to_vec(&mut self.trail_freelist, true),
                value,
            );
            self.segments.push(segment);
        }
    }

    /// Finishes the construction of the trie, returning the ID of the root
    /// node. Note that the root node has a 0 refcount; the caller is
    /// responsible for incrementing its refcount.
    ///
    /// None is returned iff add_leaf was never called.
    pub fn finalize(mut self) -> Option<MemTrieNodeId> {
        while self.segments.len() > 1 {
            self.pop_segment();
        }
        if self.segments.is_empty() {
            None
        } else {
            let segment = self.segments.pop().unwrap();
            let ret = segment.to_node(self.arena);
            self.recycle_segment(segment);
            Some(ret)
        }
    }
}

impl<'a, A: Arena> Drop for TrieConstructor<'a, A> {
    fn drop(&mut self) {
        for segment in std::mem::take(&mut self.segments) {
            self.recycle_segment(segment);
        }
    }
}
