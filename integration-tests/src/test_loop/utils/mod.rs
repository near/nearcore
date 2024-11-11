use super::env::TestLoopEnv;

pub mod contract_distribution;
pub mod network;
pub mod setups;
pub mod transactions;
pub mod validators;

pub(crate) const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;
pub(crate) const TGAS: u64 = 1_000_000_000_000;

/// Returns the height of the chain head, by querying node at index 0.
pub(crate) fn get_head_height(env: &mut TestLoopEnv) -> u64 {
    let client_handle = env.datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&client_handle).client;
    client.chain.head().unwrap().height
}
