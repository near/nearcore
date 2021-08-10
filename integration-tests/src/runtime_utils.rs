use near_chain_configs::Genesis;
use near_primitives::types::StateRoot;
use near_store::{ShardTries, TrieUpdate};
use nearcore::config::GenesisExt;
use node_runtime::{state_viewer::TrieViewer, Runtime};
use testlib::runtime_utils::{
    add_test_contract, alice_account, bob_account, get_runtime_and_trie_from_genesis,
};

pub fn get_runtime_and_trie() -> (Runtime, ShardTries, StateRoot) {
    let mut genesis =
        Genesis::test(vec![alice_account(), bob_account(), "carol.near".parse().unwrap()], 3);
    add_test_contract(&mut genesis, &"test.contract".parse().unwrap());
    get_runtime_and_trie_from_genesis(&genesis)
}

pub fn get_test_trie_viewer() -> (TrieViewer, TrieUpdate) {
    let (_, tries, root) = get_runtime_and_trie();
    let trie_viewer = TrieViewer::default();
    let state_update = tries.new_trie_update(0, root);
    (trie_viewer, state_update)
}
