use borsh::BorshSerialize;
use near_primitives::hash::hash;
use near_primitives::state::{FlatStateValue, ValueRef};

use crate::trie::mem::node::MemTrieNodeView;
use crate::trie::Children;
use crate::{RawTrieNode, RawTrieNodeWithSize};

use super::{InputMemTrieNode, MemTrieNode};

#[test]
fn test_basic_leaf_node_inlined() {
    let node = MemTrieNode::new(InputMemTrieNode::Leaf {
        extension: vec![0, 1, 2, 3, 4].into_boxed_slice(),
        value: FlatStateValue::Inlined(vec![5, 6, 7, 8, 9]),
    });
    let view = node.view();
    assert_eq!(
        node.view().to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: 115,
            node: RawTrieNode::Leaf(
                vec![0, 1, 2, 3, 4],
                FlatStateValue::Inlined(vec![5, 6, 7, 8, 9]).to_value_ref()
            ),
        }
    );
    assert_eq!(node.memory_usage(), 115);
    assert_eq!(node.hash(), hash(&view.to_raw_trie_node_with_size().try_to_vec().unwrap()));
    match node.view() {
        MemTrieNodeView::Leaf { extension, value } => {
            assert_eq!(extension, &[0, 1, 2, 3, 4]);
            assert_eq!(value.to_flat_value(), FlatStateValue::Inlined(vec![5, 6, 7, 8, 9]));
        }
        _ => panic!(),
    }
}

#[test]
fn test_basic_leaf_node_ref() {
    let test_hash = hash(&[5, 6, 7, 8, 9]);
    let node = MemTrieNode::new(InputMemTrieNode::Leaf {
        extension: vec![0, 1, 2, 3, 4].into_boxed_slice(),
        value: FlatStateValue::Ref(ValueRef { hash: test_hash, length: 5 }),
    });
    let view = node.view();
    assert_eq!(
        node.view().to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: 115,
            node: RawTrieNode::Leaf(vec![0, 1, 2, 3, 4], ValueRef { hash: test_hash, length: 5 }),
        }
    );
    assert_eq!(node.memory_usage(), 115);
    assert_eq!(node.hash(), hash(&view.to_raw_trie_node_with_size().try_to_vec().unwrap()));
    match node.view() {
        MemTrieNodeView::Leaf { extension, value } => {
            assert_eq!(extension, &[0, 1, 2, 3, 4]);
            assert_eq!(
                value.to_flat_value(),
                FlatStateValue::Ref(ValueRef { hash: test_hash, length: 5 })
            );
        }
        _ => panic!(),
    }
}

#[test]
fn test_basic_leaf_node_empty_extension_empty_value() {
    let node = MemTrieNode::new(InputMemTrieNode::Leaf {
        extension: vec![].into_boxed_slice(),
        value: FlatStateValue::Inlined(vec![]),
    });
    let view = node.view();
    assert_eq!(
        node.view().to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: 100,
            node: RawTrieNode::Leaf(vec![], FlatStateValue::Inlined(vec![]).to_value_ref()),
        }
    );
    assert_eq!(node.memory_usage(), 100);
    assert_eq!(node.hash(), hash(&view.to_raw_trie_node_with_size().try_to_vec().unwrap()));
    match node.view() {
        MemTrieNodeView::Leaf { extension, value } => {
            assert!(extension.is_empty());
            assert_eq!(value.to_flat_value(), FlatStateValue::Inlined(vec![]));
        }
        _ => panic!(),
    }
}

#[test]
fn test_basic_extension_node() {
    let child = MemTrieNode::new(InputMemTrieNode::Leaf {
        extension: vec![0, 1, 2, 3, 4].into_boxed_slice(),
        value: FlatStateValue::Inlined(vec![5, 6, 7, 8, 9]),
    });
    let node = MemTrieNode::new(InputMemTrieNode::Extension {
        extension: vec![5, 6, 7, 8, 9].into_boxed_slice(),
        child: child.clone(),
    });
    assert_eq!(
        node.view().to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: child.memory_usage() + 60,
            node: RawTrieNode::Extension(vec![5, 6, 7, 8, 9], child.hash()),
        }
    );
    node.compute_hash_and_memory_usage_recursively();
    assert_eq!(node.memory_usage(), child.memory_usage() + 60);
    assert_eq!(node.hash(), hash(&node.view().to_raw_trie_node_with_size().try_to_vec().unwrap()));
    match node.view() {
        MemTrieNodeView::Extension { hash, memory_usage, extension, child: actual_child } => {
            assert_eq!(*hash, node.hash());
            assert_eq!(memory_usage, node.memory_usage());
            assert_eq!(extension, &[5, 6, 7, 8, 9]);
            assert_eq!(actual_child, &child);
        }
        _ => panic!(),
    }
}

fn branch_vec(children: Vec<(usize, MemTrieNode)>) -> Vec<Option<MemTrieNode>> {
    let mut result: Vec<Option<MemTrieNode>> = std::iter::repeat_with(|| None).take(16).collect();
    for (idx, child) in children {
        result[idx] = Some(child);
    }
    result
}

#[test]
fn test_basic_branch_node() {
    let child1 = MemTrieNode::new(InputMemTrieNode::Leaf {
        extension: vec![].into_boxed_slice(),
        value: FlatStateValue::Inlined(vec![1]),
    });
    let child2 = MemTrieNode::new(InputMemTrieNode::Leaf {
        extension: vec![1].into_boxed_slice(),
        value: FlatStateValue::Inlined(vec![2]),
    });
    let node = MemTrieNode::new(InputMemTrieNode::Branch {
        children: branch_vec(vec![(3, child1.clone()), (5, child2.clone())]),
    });
    assert_eq!(
        node.view().to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: child1.memory_usage() + child2.memory_usage() + 50,
            node: RawTrieNode::BranchNoValue(Children([
                None,
                None,
                None,
                Some(child1.hash()),
                None,
                Some(child2.hash()),
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
    node.compute_hash_and_memory_usage_recursively();
    assert_eq!(node.memory_usage(), child1.memory_usage() + child2.memory_usage() + 50);
    assert_eq!(node.hash(), hash(&node.view().to_raw_trie_node_with_size().try_to_vec().unwrap()));
    match node.view() {
        MemTrieNodeView::Branch { hash, memory_usage, children } => {
            assert_eq!(*hash, node.hash());
            assert_eq!(memory_usage, node.memory_usage());
            assert_eq!(
                children.iter().cloned().collect::<Vec<_>>(),
                vec![child1.clone(), child2.clone()]
            );
            assert_eq!(children.get(3).cloned(), Some(child1));
            assert_eq!(children.get(1), None);
            assert_eq!(children.get(5).cloned(), Some(child2));
        }
        _ => panic!(),
    }
}

#[test]
fn test_basic_branch_with_value_node() {
    let child1 = MemTrieNode::new(InputMemTrieNode::Leaf {
        extension: vec![].into_boxed_slice(),
        value: FlatStateValue::Inlined(vec![1]),
    });
    let child2 = MemTrieNode::new(InputMemTrieNode::Leaf {
        extension: vec![1].into_boxed_slice(),
        value: FlatStateValue::Inlined(vec![2]),
    });
    let node = MemTrieNode::new(InputMemTrieNode::BranchWithValue {
        children: branch_vec(vec![(0, child1.clone()), (15, child2.clone())]),
        value: FlatStateValue::Inlined(vec![3, 4, 5]),
    });
    assert_eq!(
        node.view().to_raw_trie_node_with_size(),
        RawTrieNodeWithSize {
            memory_usage: child1.memory_usage() + child2.memory_usage() + 103,
            node: RawTrieNode::BranchWithValue(
                FlatStateValue::Inlined(vec![3, 4, 5]).to_value_ref(),
                Children([
                    Some(child1.hash()),
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
                    Some(child2.hash()),
                ])
            ),
        }
    );
    node.compute_hash_and_memory_usage_recursively();
    assert_eq!(node.memory_usage(), child1.memory_usage() + child2.memory_usage() + 103);
    assert_eq!(node.hash(), hash(&node.view().to_raw_trie_node_with_size().try_to_vec().unwrap()));
    match node.view() {
        MemTrieNodeView::BranchWithValue { hash, memory_usage, children, value } => {
            assert_eq!(*hash, node.hash());
            assert_eq!(memory_usage, node.memory_usage());
            assert_eq!(
                children.iter().cloned().collect::<Vec<_>>(),
                vec![child1.clone(), child2.clone()]
            );
            assert_eq!(children.get(0).cloned(), Some(child1));
            assert_eq!(children.get(1), None);
            assert_eq!(children.get(15).cloned(), Some(child2));
            assert_eq!(value.to_flat_value(), FlatStateValue::Inlined(vec![3, 4, 5]));
        }
        _ => panic!(),
    }
}
