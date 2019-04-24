use primitives::types::{AccountId, MerkleHash};
use primitives::transaction::{ReceiptTransaction};
use primitives::chain::{SignedShardBlockHeader, ReceiptBlock, ShardBlockHeader};
use primitives::crypto::group_signature::GroupSignature;
use primitives::merkle::merklize;
use primitives::hash::{CryptoHash, hash};
use node_runtime::chain_spec::{DefaultIdType, AuthorityRotation, ChainSpec};
use storage::{Trie, TrieUpdate};
use storage::test_utils::create_trie;
use node_runtime::{Runtime, state_viewer::TrieViewer};

use byteorder::{ByteOrder, LittleEndian};
use std::sync::Arc;

pub fn alice_account() -> AccountId {
    "alice.near".to_string()
}
pub fn bob_account() -> AccountId {
    "bob.near".to_string()
}
pub fn eve_account() -> AccountId {
    "eve.near".to_string()
}

pub fn default_code_hash() -> CryptoHash {
    let genesis_wasm = include_bytes!("../../../runtime/wasm/runtest/res/wasm_with_mem.wasm");
    hash(genesis_wasm)
}

pub fn get_runtime_and_trie_from_chain_spec(
    chain_spec: &ChainSpec,
) -> (Runtime, Arc<Trie>, MerkleHash) {
    let trie = create_trie();
    let runtime = Runtime {};
    let trie_update = TrieUpdate::new(trie.clone(), MerkleHash::default());
    let (genesis_root, db_changes) = runtime.apply_genesis_state(
        trie_update,
        &chain_spec.accounts,
        &chain_spec.genesis_wasm,
        &chain_spec.initial_authorities,
    );
    trie.apply_changes(db_changes).unwrap();
    (runtime, trie, genesis_root)
}

pub fn get_runtime_and_trie() -> (Runtime, Arc<Trie>, MerkleHash) {
    let (chain_spec, _) =
        ChainSpec::testing_spec(DefaultIdType::Named, 3, 3, AuthorityRotation::ProofOfAuthority);
    get_runtime_and_trie_from_chain_spec(&chain_spec)
}

pub fn get_test_trie_viewer() -> (TrieViewer, TrieUpdate) {
    let (_, trie, root) = get_runtime_and_trie();
    let trie_viewer = TrieViewer {};
    let state_update = TrieUpdate::new(trie, root);
    (trie_viewer, state_update)
}

pub fn encode_int(val: i32) -> [u8; 4] {
    let mut tmp = [0u8; 4];
    LittleEndian::write_i32(&mut tmp, val);
    tmp
}

pub fn to_receipt_block(receipts: Vec<ReceiptTransaction>) -> ReceiptBlock {
    let (receipt_merkle_root, path) = merklize(&[&receipts]);
    let header = SignedShardBlockHeader {
        body: ShardBlockHeader {
            parent_hash: CryptoHash::default(),
            shard_id: 0,
            index: 0,
            merkle_root_state: CryptoHash::default(),
            receipt_merkle_root,
        },
        hash: CryptoHash::default(),
        signature: GroupSignature::default(),
    };
    ReceiptBlock::new(header, path[0].clone(), receipts, 0)
}
