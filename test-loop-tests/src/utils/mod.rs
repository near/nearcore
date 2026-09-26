use crate::setup::state::NodeExecutionData;
use near_async::test_loop::data::TestLoopData;
use near_client::client_actor::ClientActor;
use near_primitives::types::AccountId;

pub(crate) mod account;
pub(crate) mod cloud_archival;
pub(crate) mod contract_distribution;
pub(crate) mod loop_action;
pub(crate) mod network;
pub(crate) mod node;
pub(crate) mod receipts;
pub(crate) mod resharding;
pub(crate) mod rotating_validators_runner;
pub(crate) mod setups;
pub(crate) mod sharded_rpc;
pub(crate) mod sharding;
pub(crate) mod transactions;
pub(crate) mod trie_sanity;
pub(crate) mod validators;

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
) -> &'a mut ClientActor {
    let client_handle = get_node_data(node_datas, client_account_id).client_sender.actor_handle();
    test_loop_data.get_mut(&client_handle)
}
