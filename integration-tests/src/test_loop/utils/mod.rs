use super::env::{TestData, TestLoopEnv};
use near_async::test_loop::data::{TestLoopData, TestLoopDataHandle};
use near_client::client_actor::ClientActorInner;

pub(crate) mod contract_distribution;
pub(crate) mod network;
pub(crate) mod receipts;
pub(crate) mod setups;
pub(crate) mod sharding;
pub(crate) mod transactions;
pub(crate) mod trie_sanity;
pub(crate) mod validators;

pub(crate) const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;
pub(crate) const TGAS: u64 = 1_000_000_000_000;

/// Returns the height of the chain head, by querying node at index 0.
pub(crate) fn get_head_height(env: &mut TestLoopEnv) -> u64 {
    let client_handle = env.datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&client_handle).client;
    client.chain.head().unwrap().height
}

/// Signature of functions callable from inside the inner loop of a test loop test.
pub(crate) type LoopActionFn =
    Box<dyn Fn(&[TestData], &mut TestLoopData, TestLoopDataHandle<ClientActorInner>)>;
