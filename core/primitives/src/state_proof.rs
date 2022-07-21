use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Eq)]
pub struct TrieProofLeaf {
    pub key: Vec<u8>,
    pub value_length: u32,
    pub value_hash: CryptoHash,
    pub memory_usage: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Eq)]
pub struct TrieProofExtension {
    pub key: Vec<u8>,
    pub child_hash: CryptoHash,
    pub memory_usage: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Eq)]
pub struct TrieProofBranch {
    pub children: [Option<CryptoHash>; 16],
    pub value: Option<(u32, CryptoHash)>,
    pub memory_usage: u64,
    pub index: u8,
}

/// Trie Merkle Proof Item is an element of a merkle proof
///
/// Can be either a Leaf, an Extension or a Branch.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Eq)]

pub enum TrieProofItem {
    Leaf(Box<TrieProofLeaf>),
    Extension(Box<TrieProofExtension>),
    Branch(Box<TrieProofBranch>),
}
