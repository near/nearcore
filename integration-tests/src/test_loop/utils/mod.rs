use near_async::test_loop::TestLoopV2;

use super::env::TestData;

pub mod contract_distribution;
pub mod network;
pub mod setups;
pub mod transactions;
pub mod validators;

pub(crate) const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;
pub(crate) const TGAS: u64 = 1_000_000_000_000;

/// Returns the height of the chain head, by querying node at index 0.
pub(crate) fn get_head_height(test_loop: &mut TestLoopV2, node_datas: &Vec<TestData>) -> u64 {
    let client_handle = node_datas[0].client_sender.actor_handle();
    let client = &test_loop.data.get(&client_handle).client;
    client.chain.head().unwrap().height
}
