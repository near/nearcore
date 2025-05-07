use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_client::Client;
use near_client::client_actor::ClientActorInner;
use near_primitives::types::{AccountId, BlockHeight};

use crate::setup::env::TestLoopEnv;
use crate::setup::state::NodeExecutionData;

pub(crate) mod client_queries;
pub(crate) mod contract_distribution;
pub(crate) mod loop_action;
pub(crate) mod network;
pub(crate) mod peer_manager_actor;
pub(crate) mod receipts;
pub(crate) mod resharding;
pub(crate) mod rotating_validators_runner;
pub(crate) mod setups;
pub(crate) mod sharding;
pub(crate) mod transactions;
pub(crate) mod trie_sanity;
pub(crate) mod validators;

pub(crate) const ONE_NEAR: u128 = 1_000_000_000_000_000_000_000_000;
pub(crate) const TGAS: u64 = 1_000_000_000_000;

pub(crate) fn get_node_client<'a>(
    env: &'a TestLoopEnv,
    client_account_id: &AccountId,
) -> &'a Client {
    let client_handle =
        get_node_data(&env.node_datas, client_account_id).client_sender.actor_handle();
    &env.test_loop.data.get(&client_handle).client
}

pub(crate) fn get_node_head_height(
    env: &TestLoopEnv,
    client_account_id: &AccountId,
) -> BlockHeight {
    get_node_client(env, client_account_id).chain.head().unwrap().height
}

#[allow(dead_code)]
pub(crate) fn run_until_node_head_height(
    env: &mut TestLoopEnv,
    client_account_id: &AccountId,
    height: BlockHeight,
    maximum_duration: Duration,
) {
    env.test_loop.run_until(
        |test_loop_data| {
            let client_actor =
                retrieve_client_actor(&env.node_datas, test_loop_data, client_account_id);
            client_actor.client.chain.head().unwrap().height >= height
        },
        maximum_duration,
    );
}

/// Returns the test data of for the node with the given account id.
pub(crate) fn get_node_data<'a>(
    node_datas: &'a [NodeExecutionData],
    account_id: &AccountId,
) -> &'a NodeExecutionData {
    for node_data in node_datas {
        if &node_data.account_id == account_id {
            return node_data;
        }
    }
    panic!("client not found");
}

/// Retrieves the client actor of the node having account_id equal to `client_account_id`.
pub(crate) fn retrieve_client_actor<'a>(
    node_datas: &'a [NodeExecutionData],
    test_loop_data: &'a mut TestLoopData,
    client_account_id: &AccountId,
) -> &'a mut ClientActorInner {
    let client_handle = get_node_data(node_datas, client_account_id).client_sender.actor_handle();
    test_loop_data.get_mut(&client_handle)
}
