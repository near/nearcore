use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::event_type::ReshardingSplitShardParams;
use super::flat_storage_resharder::FlatStorageResharder;
use super::trie_state_resharder::TrieStateResharder;
use super::types::ScheduleResharding;
use crate::resharding::trie_state_resharder::ResumeAllowed;
use crate::types::RuntimeAdapter;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt};
use near_async::messaging::{self, HandlerWithContext};
use near_chain_configs::{MutableConfigValue, ReshardingConfig, ReshardingHandle};
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::shard_layout::ShardUId;
#[cfg(feature = "test_features")]
use near_primitives::types::BlockHeightDelta;
use near_store::adapter::StoreAdapter;
use near_store::adapter::chain_store::ChainStoreAdapter;
use time::Duration;

/// Dedicated actor for resharding V3.
pub struct ReshardingActor {
    chain_store: ChainStoreAdapter,
    /// HashMap storing all scheduled resharding events. Typically there will be only
    /// one event per parent shard, but we keep it as a HashMap to allow for
    /// handling forks in the chain.
    /// We start resharding when one of the resharding block becomes final.
    resharding_events: HashMap<ShardUId, Vec<ReshardingSplitShardParams>>,
    /// Indicates whether resharding has started for a given parent shard.
    /// This is used to prevent resharding from being started multiple times for the same parent shard.
    resharding_started: HashSet<ShardUId>,
    /// Takes care of performing resharding on the flat storage.
    flat_storage_resharder: FlatStorageResharder,
    /// Takes care of performing resharding on the trie state.
    trie_state_resharder: TrieStateResharder,
    /// TEST ONLY. If non zero, the start of scheduled tasks (such as split parent)
    /// will be postponed by the specified number of blocks.
    #[cfg(feature = "test_features")]
    pub adv_task_delay_by_blocks: BlockHeightDelta,
    /// TEST ONLY. Tracks parent shards whose child shard flat storage statuses have been
    /// pre-set to `CreatingChild` during the artificial delay. This ensures the
    /// `StateSnapshotActor` can detect pending resharding before it actually starts.
    #[cfg(feature = "test_features")]
    child_shard_status_prepared: HashSet<ShardUId>,
}

enum ReshardingSchedulingStatus {
    StartResharding(ReshardingSplitShardParams),
    WaitForFinalBlock,
    /// Resharding block is final but start is artificially delayed for testing.
    /// Contains the child shard UIDs needed to pre-set flat storage statuses.
    #[cfg(feature = "test_features")]
    WaitForDelay {
        left_child_shard: ShardUId,
        right_child_shard: ShardUId,
    },
    AlreadyStarted,
}

impl messaging::Actor for ReshardingActor {}

impl HandlerWithContext<ScheduleResharding> for ReshardingActor {
    fn handle(&mut self, msg: ScheduleResharding, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.handle_schedule_resharding(msg.split_shard_event, ctx);
    }
}

impl ReshardingActor {
    pub fn new(
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        resharding_handle: ReshardingHandle,
        resharding_config: MutableConfigValue<ReshardingConfig>,
    ) -> Self {
        let chain_store = runtime_adapter.store().chain_store();
        let flat_storage_resharder = FlatStorageResharder::new(
            epoch_manager,
            runtime_adapter.clone(),
            resharding_handle.clone(),
            resharding_config.clone(),
        );
        let trie_state_resharder = TrieStateResharder::new(
            runtime_adapter,
            resharding_handle,
            resharding_config,
            ResumeAllowed::No,
        );
        Self {
            chain_store,
            resharding_events: HashMap::new(),
            resharding_started: HashSet::new(),
            flat_storage_resharder,
            trie_state_resharder,
            #[cfg(feature = "test_features")]
            adv_task_delay_by_blocks: 0,
            #[cfg(feature = "test_features")]
            child_shard_status_prepared: HashSet::new(),
        }
    }

    fn handle_schedule_resharding(
        &mut self,
        split_shard_event: ReshardingSplitShardParams,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        tracing::info!(target: "resharding", ?split_shard_event, "handle_schedule_resharding");

        let parent_shard = split_shard_event.parent_shard;
        if self.resharding_started.contains(&parent_shard) {
            // The event is already in progress, no need to reschedule.
            tracing::info!(target: "resharding", "resharding already in progress");
            return;
        }

        let events = self.resharding_events.entry(split_shard_event.parent_shard).or_default();

        if !events.is_empty() {
            // Validate the event parameters. We should never have two events with
            // different parameters for the same parent shard.
            assert_eq!(events[0].left_child_shard, split_shard_event.left_child_shard);
            assert_eq!(events[0].right_child_shard, split_shard_event.right_child_shard);
            assert_eq!(events[0].boundary_account, split_shard_event.boundary_account);
        }

        events.push(split_shard_event);

        // Schedule the resharding task and wait for the resharding block to become final.
        self.schedule_resharding(parent_shard, ctx);
    }

    // Wait for the resharding block to become final and then start resharding.
    fn schedule_resharding(
        &mut self,
        parent_shard_uid: ShardUId,
        ctx: &mut dyn DelayedActionRunner<Self>,
    ) {
        match self.get_resharding_scheduling_status(parent_shard_uid) {
            ReshardingSchedulingStatus::StartResharding(event) => {
                self.start_resharding_blocking(parent_shard_uid, event)
            }
            ReshardingSchedulingStatus::WaitForFinalBlock => {
                // The task must be retried later.
                ctx.run_later(
                    "ReshardingActor ScheduleResharding",
                    Duration::milliseconds(1000),
                    move |act, ctx| {
                        act.schedule_resharding(parent_shard_uid, ctx);
                    },
                );
            }
            #[cfg(feature = "test_features")]
            ReshardingSchedulingStatus::WaitForDelay { left_child_shard, right_child_shard } => {
                // Pre-set child shard flat storage statuses so that
                // `should_wait_for_resharding_split` in the `StateSnapshotActor`
                // detects pending resharding and waits before creating a snapshot.
                if !self.child_shard_status_prepared.contains(&parent_shard_uid) {
                    self.flat_storage_resharder
                        .set_child_shard_statuses_to_creating(left_child_shard, right_child_shard);
                    self.child_shard_status_prepared.insert(parent_shard_uid);
                    tracing::info!(
                        target: "resharding",
                        ?parent_shard_uid,
                        "pre-set child shard statuses to CreatingChild during artificial delay"
                    );
                }
                // The task must be retried later.
                ctx.run_later(
                    "ReshardingActor ScheduleResharding",
                    Duration::milliseconds(1000),
                    move |act, ctx| {
                        act.schedule_resharding(parent_shard_uid, ctx);
                    },
                );
            }
            // The event is already started, no need to reschedule.
            ReshardingSchedulingStatus::AlreadyStarted => {}
        }
    }

    // function to check if any one of the resharding block candidates is final
    // and part of the canonical chain.
    fn get_resharding_scheduling_status(
        &self,
        parent_shard_uid: ShardUId,
    ) -> ReshardingSchedulingStatus {
        tracing::info!(target: "resharding", ?parent_shard_uid, "get_resharding_scheduling_status");

        if self.resharding_started.contains(&parent_shard_uid) {
            // The event is already in progress, no need to reschedule.
            tracing::info!(target: "resharding", "resharding already in progress");
            return ReshardingSchedulingStatus::AlreadyStarted;
        }

        let events = self.resharding_events.get(&parent_shard_uid).unwrap();

        let chain_final_height = self.chain_store.final_head().unwrap().height;
        for event in events {
            tracing::info!(
                %chain_final_height,
                resharding_block = ?event.resharding_block,
                "get_resharding_scheduling_status: head height and resharding_block"
            );

            // To check whether we can start resharding, we need to check if the resharding block is final.
            // We check if the resharding block is behind the final block and is part of the canonical chain.
            if event.resharding_block.height > chain_final_height {
                continue;
            }

            // Get canonical block hash for the resharding block height.
            let Ok(resharding_hash) =
                self.chain_store.get_block_hash_by_height(event.resharding_block.height)
            else {
                continue;
            };

            if resharding_hash != event.resharding_block.hash {
                // The resharding block is not part of the canonical chain.
                continue;
            }

            // Check if resharding should be artificially delayed.
            // This behavior is configured through `adv_task_delay_by_blocks`
            #[cfg(feature = "test_features")]
            if event.resharding_block.height + self.adv_task_delay_by_blocks > chain_final_height {
                tracing::info!(target: "resharding", "resharding has been artificially postponed");
                return ReshardingSchedulingStatus::WaitForDelay {
                    left_child_shard: event.left_child_shard,
                    right_child_shard: event.right_child_shard,
                };
            }

            return ReshardingSchedulingStatus::StartResharding(event.clone());
        }

        ReshardingSchedulingStatus::WaitForFinalBlock
    }

    fn start_resharding_blocking(
        &mut self,
        parent_shard_uid: ShardUId,
        resharding_event: ReshardingSplitShardParams,
    ) {
        self.resharding_started.insert(parent_shard_uid);

        if let Err(err) =
            self.trie_state_resharder.initialize_trie_state_resharding_status(&resharding_event)
        {
            tracing::error!(target: "resharding", ?err, "failed to initialize trie state resharding status");
            return;
        }

        // This is a long running task and would block the actor
        if let Err(err) = self.flat_storage_resharder.start_resharding_blocking(&resharding_event) {
            tracing::error!(target: "resharding", ?err, "failed to start flat storage resharding");
            return;
        }

        tracing::info!(target: "resharding", "trie state resharder starting");
        if let Err(err) = self.trie_state_resharder.start_resharding_blocking(&resharding_event) {
            tracing::error!(target: "resharding", ?err, "failed to start trie state resharding");
            return;
        }
    }
}
