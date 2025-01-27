use super::env::{TestData, TestLoopEnv};
use near_async::test_loop::data::TestLoopData;
use near_client::client_actor::ClientActorInner;
use near_primitives::types::AccountId;

pub(crate) mod contract_distribution;
pub(crate) mod loop_action;
pub(crate) mod network;
pub(crate) mod receipts;
pub(crate) mod resharding;
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

/// Returns the test data of for the node with the given account id.
pub(crate) fn get_node_data<'a>(
    node_datas: &'a [TestData],
    account_id: &AccountId,
) -> &'a TestData {
    for node_data in node_datas {
        if &node_data.account_id == account_id {
            return node_data;
        }
    }
    panic!("client not found");
}

/// Retrieves the client actor of the node having account_id equal to `client_account_id`.
pub(crate) fn retrieve_client_actor<'a>(
    node_datas: &'a [TestData],
    test_loop_data: &'a mut TestLoopData,
    client_account_id: &AccountId,
) -> &'a mut ClientActorInner {
    let client_handle = get_node_data(node_datas, client_account_id).client_sender.actor_handle();
    test_loop_data.get_mut(&client_handle)
}
