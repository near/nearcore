use crate::setup::state::NodeExecutionData;
use near_async::test_loop::data::TestLoopData;
use near_client::Client;
use near_client::client_actor::ClientActor;
use near_primitives::types::{AccountId, EpochId};
use near_store::adapter::StoreAdapter;

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

/// Epoch ids from the first epoch after genesis up to `client`'s current epoch, in order.
/// Follows `EpochSummary`, which GC keeps: the summary of epoch T stores the last block of
/// T-1, whose hash is the id of T+1. `client` must have finalized every epoch since
/// genesis, so an epoch-synced node does not qualify.
pub(crate) fn epoch_ids_since_genesis(client: &Client) -> Vec<EpochId> {
    let epoch_store = client.chain.chain_store.store().epoch_store();
    let current_epoch_id = client.chain.head().unwrap().epoch_id;
    let mut epoch_id = EpochId::default();
    let mut epoch_ids = vec![epoch_id];
    while epoch_id != current_epoch_id {
        let summary = epoch_store
            .get_epoch_validator_info(&epoch_id)
            .expect("every epoch before the current one must have an EpochSummary");
        // The id of the next epoch is the hash of the last block of the previous epoch.
        epoch_id = EpochId(summary.prev_epoch_last_block_hash);
        epoch_ids.push(epoch_id);
    }
    epoch_ids
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
