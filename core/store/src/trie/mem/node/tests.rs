use crate::trie::Children;
use crate::trie::mem::arena::Arena;
use crate::trie::mem::arena::single_thread::STArena;
use crate::trie::mem::node::{InputMemTrieNode, MemTrieNodeId, MemTrieNodeView};
use crate::{RawTrieNode, RawTrieNodeWithSize};
use near_primitives::hash::hash;
use near_primitives::state::{FlatStateValue, ValueRef};

#[test]
fn test_basic_leaf_node_inlined() {
    let mut arena = STArena::new("".to_owned());
    let node = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Leaf {
            extension: &[0, 1, 2, 3, 4],
            value: &FlatStateValue::Inlined(vec![5, 6, 7, 8, 9]),
        },
    );
    let view = node.as_ptr(arena.memory()).view();
    assert_eq!(
        view.to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: 115,
            node: RawTrieNode::Leaf(
                vec![0, 1, 2, 3, 4],
                FlatStateValue::Inlined(vec![5, 6, 7, 8, 9]).to_value_ref()
            ),
        }
    );
    assert_eq!(view.memory_usage(), 115);
    assert_eq!(view.node_hash(), hash(&borsh::to_vec(&view.to_raw_trie_node_with_size()).unwrap()));
    match view {
        MemTrieNodeView::Leaf { extension, value } => {
            assert_eq!(extension, &[0, 1, 2, 3, 4]);
            assert_eq!(value.to_flat_value(), FlatStateValue::Inlined(vec![5, 6, 7, 8, 9]));
        }
        _ => panic!("Unexpected view type: {:?}", view),
    }
}

#[test]
fn test_basic_leaf_node_ref() {
    let mut arena = STArena::new("".to_owned());
    let test_hash = hash(&[5, 6, 7, 8, 9]);
    let node = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Leaf {
            extension: &[0, 1, 2, 3, 4],
            value: &FlatStateValue::Ref(ValueRef { hash: test_hash, length: 5 }),
        },
    );
    let view = node.as_ptr(arena.memory()).view();
    assert_eq!(
        view.to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: 115,
            node: RawTrieNode::Leaf(vec![0, 1, 2, 3, 4], ValueRef { hash: test_hash, length: 5 }),
        }
    );
    assert_eq!(view.memory_usage(), 115);
    assert_eq!(view.node_hash(), hash(&borsh::to_vec(&view.to_raw_trie_node_with_size()).unwrap()));
    match view {
        MemTrieNodeView::Leaf { extension, value } => {
            assert_eq!(extension, &[0, 1, 2, 3, 4]);
            assert_eq!(
                value.to_flat_value(),
                FlatStateValue::Ref(ValueRef { hash: test_hash, length: 5 })
            );
        }
        _ => panic!("Unexpected view type: {:?}", view),
    }
}

#[test]
fn test_basic_leaf_node_empty_extension_empty_value() {
    let mut arena = STArena::new("".to_owned());
    let node = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Leaf { extension: &[], value: &FlatStateValue::Inlined(vec![]) },
    );
    let view = node.as_ptr(arena.memory()).view();
    assert_eq!(
        view.to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: 100,
            node: RawTrieNode::Leaf(vec![], FlatStateValue::Inlined(vec![]).to_value_ref()),
        }
    );
    assert_eq!(view.memory_usage(), 100);
    assert_eq!(view.node_hash(), hash(&borsh::to_vec(&view.to_raw_trie_node_with_size()).unwrap()));
    match view {
        MemTrieNodeView::Leaf { extension, value } => {
            assert!(extension.is_empty());
            assert_eq!(value.to_flat_value(), FlatStateValue::Inlined(vec![]));
        }
        _ => panic!("Unexpected view type: {:?}", view),
    }
}

#[test]
fn test_basic_extension_node() {
    let mut arena = STArena::new("".to_owned());
    let child = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Leaf {
            extension: &[0, 1, 2, 3, 4],
            value: &FlatStateValue::Inlined(vec![5, 6, 7, 8, 9]),
        },
    );
    let node = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Extension { extension: &[5, 6, 7, 8, 9], child },
    );
    let child_ptr = child.as_ptr(arena.memory());
    let node_ptr = node.as_ptr(arena.memory());
    assert_eq!(
        node_ptr.view().to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: child_ptr.view().memory_usage() + 60,
            node: RawTrieNode::Extension(vec![5, 6, 7, 8, 9], child_ptr.view().node_hash()),
        }
    );
    assert_eq!(node_ptr.view().memory_usage(), child_ptr.view().memory_usage() + 60);
    assert_eq!(
        node_ptr.view().node_hash(),
        hash(&borsh::to_vec(&node_ptr.view().to_raw_trie_node_with_size()).unwrap())
    );
    match node_ptr.view() {
        MemTrieNodeView::Extension { hash, memory_usage, extension, child: actual_child } => {
            assert_eq!(hash, node_ptr.view().node_hash());
            assert_eq!(memory_usage, node_ptr.view().memory_usage());
            assert_eq!(extension, &[5, 6, 7, 8, 9]);
            assert_eq!(actual_child, child_ptr);
        }
        _ => panic!("Unexpected view type: {:?}", node_ptr.view()),
    }
}

fn branch_array(children: Vec<(usize, MemTrieNodeId)>) -> [Option<MemTrieNodeId>; 16] {
    let mut result = [None; 16];
    for (idx, child) in children {
        result[idx] = Some(child);
    }
    result
}

#[test]
fn test_basic_branch_node() {
    let mut arena = STArena::new("".to_owned());
    let child1 = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Leaf { extension: &[], value: &FlatStateValue::Inlined(vec![1]) },
    );
    let child2 = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Leaf { extension: &[1], value: &FlatStateValue::Inlined(vec![2]) },
    );
    let node = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Branch { children: branch_array(vec![(3, child1), (5, child2)]) },
    );
    let child1_ptr = child1.as_ptr(arena.memory());
    let child2_ptr = child2.as_ptr(arena.memory());
    let node_ptr = node.as_ptr(arena.memory());
    assert_eq!(
        node_ptr.view().to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: child1_ptr.view().memory_usage() + child2_ptr.view().memory_usage() + 50,
            node: RawTrieNode::BranchNoValue(Children([
                None,
                None,
                None,
                Some(child1_ptr.view().node_hash()),
                None,
                Some(child2_ptr.view().node_hash()),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None
            ])),
        }
    );
    assert_eq!(
        node_ptr.view().memory_usage(),
        child1_ptr.view().memory_usage() + child2_ptr.view().memory_usage() + 50
    );
    assert_eq!(
        node_ptr.view().node_hash(),
        hash(&borsh::to_vec(&node_ptr.view().to_raw_trie_node_with_size()).unwrap())
    );
    match node_ptr.view() {
        MemTrieNodeView::Branch { hash, memory_usage, children } => {
            assert_eq!(hash, node_ptr.view().node_hash());
            assert_eq!(memory_usage, node_ptr.view().memory_usage());
            assert_eq!(children.iter().collect::<Vec<_>>(), vec![child1_ptr, child2_ptr]);
            assert_eq!(children.get(3), Some(child1_ptr));
            assert_eq!(children.get(1), None);
            assert_eq!(children.get(5), Some(child2_ptr));
        }
        _ => panic!("Unexpected view type: {:?}", node_ptr.view()),
    }
}

#[test]
fn test_basic_branch_with_value_node() {
    let mut arena = STArena::new("".to_owned());
    let child1 = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Leaf { extension: &[], value: &FlatStateValue::Inlined(vec![1]) },
    );
    let child2 = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::Leaf { extension: &[1], value: &FlatStateValue::Inlined(vec![2]) },
    );
    let node = MemTrieNodeId::new(
        &mut arena,
        InputMemTrieNode::BranchWithValue {
            children: branch_array(vec![(0, child1), (15, child2)]),
            value: &FlatStateValue::Inlined(vec![3, 4, 5]),
        },
    );

    let child1_ptr = child1.as_ptr(arena.memory());
    let child2_ptr = child2.as_ptr(arena.memory());
    let node_ptr = node.as_ptr(arena.memory());
    assert_eq!(
        node_ptr.view().to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: child1_ptr.view().memory_usage() + child2_ptr.view().memory_usage() + 103,
            node: RawTrieNode::BranchWithValue(
                FlatStateValue::Inlined(vec![3, 4, 5]).to_value_ref(),
                Children([
                    Some(child1_ptr.view().node_hash()),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    Some(child2_ptr.view().node_hash()),
                ])
            ),
        }
    );
    assert_eq!(
        node_ptr.view().memory_usage(),
        child1_ptr.view().memory_usage() + child2_ptr.view().memory_usage() + 103
    );
    assert_eq!(
        node_ptr.view().node_hash(),
        hash(&borsh::to_vec(&node_ptr.view().to_raw_trie_node_with_size()).unwrap())
    );
    match node_ptr.view() {
        MemTrieNodeView::BranchWithValue { hash, memory_usage, children, value } => {
            assert_eq!(hash, node_ptr.view().node_hash());
            assert_eq!(memory_usage, node_ptr.view().memory_usage());
            assert_eq!(children.iter().collect::<Vec<_>>(), vec![child1_ptr, child2_ptr]);
            assert_eq!(children.get(0), Some(child1_ptr));
            assert_eq!(children.get(1), None);
            assert_eq!(children.get(15), Some(child2_ptr));
            assert_eq!(value.to_flat_value(), FlatStateValue::Inlined(vec![3, 4, 5]));
        }
        _ => panic!("Unexpected view type: {:?}", node_ptr.view()),
    }
}
