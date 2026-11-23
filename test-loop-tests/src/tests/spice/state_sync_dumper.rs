use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::rpc_account_id;
use near_async::time::Duration;
use near_chain_configs::{DumpConfig, ExternalStorageLocation};
use near_o11y::testonly::init_test_logger;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The state sync dumper does not start an epoch whose sync block final blocks certify but this
/// node has not applied yet, and dumps the epoch once execution catches up.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn test_spice_state_sync_dumper_waits_for_execution() {
    init_test_logger();
    const NUM_VALIDATORS: usize = 2;
    const EPOCH_LENGTH: u64 = 30;
    const SLOW_APPLY_DELAY: Duration = Duration::seconds(3);
    const TIMEOUT: Duration = Duration::seconds(60);

    // Slows the RPC node's next chunk application, once.
    let slow_next_apply = Arc::new(AtomicBool::new(false));
    let dump_dir = tempfile::tempdir().unwrap();
    let mut env = TestLoopBuilder::new()
        .validators(NUM_VALIDATORS, 0)
        .epoch_length(EPOCH_LENGTH)
        .enable_rpc()
        .config_modifier({
            let root_dir = dump_dir.path().to_path_buf();
            move |config, client_index| {
                // The RPC node comes after the validators.
                if client_index == NUM_VALIDATORS {
                    config.state_sync.dump = Some(DumpConfig {
                        location: ExternalStorageLocation::Filesystem {
                            root_dir: root_dir.clone(),
                        },
                        restart_dump_for_shards: None,
                        iteration_delay: Some(Duration::seconds(1)),
                        credentials_file: None,
                    });
                }
            }
        })
        .task_delay_fn({
            let slow_next_apply = slow_next_apply.clone();
            move |account_id, task_name| {
                if account_id == &rpc_account_id()
                    && task_name == "apply_chunks"
                    && slow_next_apply.swap(false, Ordering::Relaxed)
                {
                    Some(SLOW_APPLY_DELAY)
                } else {
                    None
                }
            }
        })
        .build();

    // Slow the RPC node's application of the next sync block.
    env.rpc_runner().run_until_new_epoch();
    env.rpc_runner().run_until(
        |node| {
            let head = node.head();
            node.last_executed().height == head.height
                && node
                    .client()
                    .epoch_manager
                    .is_next_block_epoch_start(&head.last_block_hash)
                    .unwrap()
        },
        TIMEOUT,
    );
    slow_next_apply.store(true, Ordering::Relaxed);
    env.rpc_runner().run_until_new_epoch();

    let rpc_node = env.rpc_node();
    let epoch_id = env.rpc_node().head().epoch_id;
    let sync_hash = *env
        .rpc_node()
        .client()
        .epoch_manager
        .get_block_info(&rpc_node.head().last_block_hash)
        .unwrap()
        .epoch_first_block();
    let shard_layout = rpc_node.client().epoch_manager.get_shard_layout(&epoch_id).unwrap();

    env.rpc_runner().run_until(
        |node| {
            node.client()
                .chain
                .chain_store()
                .are_chunks_certified(&sync_hash, shard_layout.shard_ids())
        },
        SLOW_APPLY_DELAY,
    );
    assert!(
        !env.rpc_node()
            .client()
            .chain
            .chain_store()
            .are_chunks_applied(&sync_hash, shard_layout.shard_uids())
            .unwrap(),
        "the RPC node should not apply the sync block before final blocks certified it",
    );

    env.rpc_runner().run_until(
        |node| {
            let chain_store = node.client().chain.chain_store();
            if chain_store.are_chunks_applied(&sync_hash, shard_layout.shard_uids()).unwrap() {
                return true;
            }
            for progress in chain_store.iter_state_sync_dump_progress() {
                let (_, (dump_epoch_id, _)) = progress.unwrap();
                assert_ne!(
                    dump_epoch_id, epoch_id,
                    "the dumper should not start an epoch whose sync block is not yet applied"
                );
            }
            false
        },
        TIMEOUT,
    );

    env.rpc_runner().run_until(
        |node| {
            let progress: Vec<_> = node
                .client()
                .chain
                .chain_store()
                .iter_state_sync_dump_progress()
                .map(Result::unwrap)
                .collect();
            shard_layout
                .shard_ids()
                .all(|shard_id| progress.contains(&(shard_id, (epoch_id, true))))
        },
        TIMEOUT,
    );
}
