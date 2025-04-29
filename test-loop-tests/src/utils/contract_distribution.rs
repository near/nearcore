use crate::setup::env::TestLoopEnv;
use itertools::Itertools;
use near_async::{test_loop::data::TestLoopData, time::Duration};
use near_chain::ChainStoreAccess;
use near_primitives::{hash::CryptoHash, version::PROTOCOL_VERSION};
use near_vm_runner::get_contract_cache_key;

/// Runs the network until all the nodes contain the given code hash in their compiled-contracts cache.
/// This is used, for example, to make sure that a deploy action took effect in the network and code was distributed to all nodes.
pub(crate) fn run_until_caches_contain_contract(env: &mut TestLoopEnv, code_hash: &CryptoHash) {
    env.test_loop.run_until(
        |test_loop_data: &mut TestLoopData| -> bool {
            for i in 0..env.node_datas.len() {
                let client_handle = env.node_datas[i].client_sender.actor_handle();
                let client = &test_loop_data.get(&client_handle).client;
                let runtime_config = client.runtime_adapter.get_runtime_config(PROTOCOL_VERSION);
                let cache_key = get_contract_cache_key(*code_hash, &runtime_config.wasm_config);

                let contract_cache = client.runtime_adapter.compiled_contract_cache();
                if !contract_cache.has(&cache_key).unwrap() {
                    return false;
                }
            }
            true
        },
        Duration::seconds(5),
    );
}

/// Asserts that no chunk validation error happened between `start_height` and `end_height` (inclusive), by querying node 0.
/// In other words, asserts that all chunks are included and endorsements are received from all the chunk validators assigned.
pub(crate) fn assert_all_chunk_endorsements_received(
    env: &TestLoopEnv,
    start_height: u64,
    end_height: u64,
) {
    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let client = &env.test_loop.data.get(&client_handle).client;
    let chain_store = client.chain.chain_store();
    let epoch_manager = &client.epoch_manager;

    for height in start_height..=end_height {
        let header = chain_store.get_block_header_by_height(height).unwrap();

        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(header.prev_hash()).unwrap();
        let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_ids =
            epoch_manager.get_shard_layout(&epoch_id).unwrap().shard_ids().collect_vec();
        let chunk_mask = header.chunk_mask();
        let endorsements = header.chunk_endorsements().unwrap();
        for shard_id in shard_ids {
            let shard_index = shard_layout.get_shard_index(shard_id).unwrap();
            let num_validator_assignments = epoch_manager
                .get_chunk_validator_assignments(&epoch_id, shard_id, height)
                .unwrap()
                .ordered_chunk_validators()
                .len();
            let num_received_endorsements =
                endorsements.iter(shard_index).filter(|endorsement| *endorsement).count();

            assert!(chunk_mask[shard_index], "Missed chunk at shard index {}", shard_index);
            assert_eq!(
                num_received_endorsements, num_validator_assignments,
                "Not all endorsements received for shard index {}",
                shard_index
            );
        }
    }
}

/// Clears the compiled contract caches for all the clients.
pub(crate) fn clear_compiled_contract_caches(env: &TestLoopEnv) {
    #[cfg(feature = "test_features")]
    for i in 0..env.node_datas.len() {
        let client_handle = env.node_datas[i].client_sender.actor_handle();
        let contract_cache_handle =
            env.test_loop.data.get(&client_handle).client.runtime_adapter.compiled_contract_cache();
        contract_cache_handle.test_only_clear().unwrap();
    }
    #[cfg(not(feature = "test_features"))]
    let _ignore = env;
}
