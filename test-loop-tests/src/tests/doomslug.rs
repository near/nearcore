use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_chain_configs::UpdatableClientConfig;
use near_client::client_actor::AdvProduceBlockHeightSelection;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::{Approval, ApprovalInner, ApprovalType};
use near_primitives::hash::CryptoHash;
use near_primitives::test_utils::create_test_signer;

/// Regression test ensuring that `collect_block_approval`, when multiple blocks
/// exist at a skip approval's `parent_height`, prefers a parent hash under
/// which the current node is the block producer for `target_height`, so the
/// approval is not silently dropped.
///
/// This test runs in multiple trials because the pre-fix behavior was
/// non-deterministic: an arbitrary hash was picked from `HashMap` iteration
/// order, which accidentally matched the current node's producer schedule in
/// roughly half the cases. Each trial:
///   - advance to the first block of a new epoch (fork_height),
///   - fork: produce a skip block at fork_height whose parent is at
///     fork_height - 2, so the skip block stays in the *previous* epoch while
///     the canonical block at fork_height is in the new epoch,
///   - scan for a target_height > fork_height where the two epochs disagree
///     on the block producer, choosing the producer under the skip epoch as
///     fork_producer (the only node that has both sibling blocks in its
///     store — others reject the adversarial skip for NotEnoughApprovals),
///   - inject a self-Skip(fork_height) approval into fork_producer and
///     assert it reaches doomslug.
#[test]
fn test_skip_approval_prefers_producer_matching_parent() {
    init_test_logger();

    let epoch_length = 10;
    let num_validators = 2;
    let mut env =
        TestLoopBuilder::new().validators(num_validators, 0).epoch_length(epoch_length).build();

    // Advance past the first couple of epochs — they can share rng seeds and
    // produce the same block-producer schedule, making "different epoch"
    // forks indistinguishable.
    env.validator_runner().run_until_new_epoch();
    env.validator_runner().run_until_new_epoch();

    // 4 trials leave a ~6% false-pass probability of the prior ~50%
    //     non-deterministic behavior (HashMap iteration order).
    let mut need_success = 4;
    let mut iter = 0;
    while need_success > 0 {
        iter += 1;
        assert!(iter <= 20, "ran out of iterations without finding enough testable boundaries");

        env.validator_runner().run_until_new_epoch();
        let fork_height = env.validator().head().height;
        let canonical_epoch = env.validator().head().epoch_id;

        let prev_block_hash = env
            .validator()
            .client()
            .chain
            .chain_store()
            .get_block_hash_by_height(fork_height - 2)
            .unwrap();
        let epoch_manager = env.validator().client().epoch_manager.clone();
        let skip_epoch = epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash).unwrap();
        if skip_epoch == canonical_epoch {
            tracing::warn!(fork_height, "no epoch boundary, skipping trial");
            continue;
        }

        let fork_producer = epoch_manager.get_block_producer(&skip_epoch, fork_height).unwrap();
        let Some(target_height) = (fork_height + 2..fork_height + 50).find_map(|target_height| {
            let p_canonical =
                epoch_manager.get_block_producer(&canonical_epoch, target_height).ok()?;
            let p_skip = epoch_manager.get_block_producer(&skip_epoch, target_height).ok()?;
            (p_canonical != p_skip && p_skip == fork_producer).then_some(target_height)
        }) else {
            tracing::warn!(fork_height, "no usable target_height, skipping trial");
            continue;
        };

        // Produce a skip block at fork_height whose parent is at fork_height - 2,
        // creating a sibling of the canonical block at fork_height in a
        // different epoch.
        env.node_for_account_mut(&fork_producer).client_actor().adv_produce_blocks_on(
            1,
            true,
            AdvProduceBlockHeightSelection::SelectedHeightOnSelectedBlock {
                produced_block_height: fork_height,
                base_block_height: fork_height - 2,
            },
        );

        // Wait for the fork block to land in fork_producer's chain store.
        // `adv_produce_blocks_on` schedules async block processing, so the
        // fork is not visible immediately. `get_all_block_hashes_by_height`
        // returns a map keyed by epoch id, so `keys().len() >= 2` confirms
        // the two siblings are in two different epochs.
        env.runner_for_account(&fork_producer).run_until(
            |node| {
                let blocks =
                    node.client().chain.chain_store().get_all_block_hashes_by_height(fork_height);
                blocks.keys().len() >= 2
            },
            Duration::seconds(10),
        );

        let signer = create_test_signer(fork_producer.as_str());
        let approval = Approval::new(CryptoHash::default(), fork_height, target_height, &signer);
        assert!(matches!(approval.inner, ApprovalInner::Skip(_)));

        let mut node = env.node_for_account_mut(&fork_producer);
        let client = &mut node.client_actor().client;
        client.collect_block_approval(&approval, ApprovalType::SelfApproval);
        assert!(
            !client.doomslug.approval_status_at_height(&target_height).approvals.is_empty(),
            "approval should reach doomslug at fork_height {fork_height}",
        );

        need_success -= 1;
    }
}

/// A SIGHUP config reload calls `update_client_config` to push new values
/// into a running node. Check that a changed consensus field reaches the live
/// `ClientConfig` and that the node keeps producing blocks.
#[test]
fn test_update_client_config_applies_consensus_values() {
    init_test_logger();

    let mut env = TestLoopBuilder::new().build();
    env.validator_runner().run_until_head_height(5);

    let client = env.validator().client();
    let old_doomslug_step_period = client.config.doomslug_step_period.get();
    let new_doomslug_step_period = old_doomslug_step_period + Duration::milliseconds(7);

    let updatable_config = |doomslug_step_period| UpdatableClientConfig {
        doomslug_step_period,
        ..UpdatableClientConfig::from(&client.config)
    };

    assert!(client.apply_updatable_client_config(updatable_config(new_doomslug_step_period)));
    assert_eq!(client.config.doomslug_step_period.get(), new_doomslug_step_period);

    // Re-applying identical values reports no change.
    assert!(!client.apply_updatable_client_config(updatable_config(new_doomslug_step_period)));

    let target_height = env.validator().head().height + 5;
    env.validator_runner().run_until_head_height(target_height);
}
