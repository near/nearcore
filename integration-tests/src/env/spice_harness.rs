//! Synchronous driver for the SPICE per-shard chunk-execution subsystem inside
//! the (synchronous) `TestEnv` integration-test framework.
//!
//! Production and test-loop run the `ChunkExecutorCoordinator` + per-shard
//! `PerShardExecutor`s as async actors. `TestEnv` has no running actor system, so
//! this module drives the *real* actors synchronously: every inter-actor message
//! is routed into a single FIFO queue of closures, and [`SpiceNode::pump`] drains
//! it (plus deferred per-shard spawns) to quiescence after each step. This mirrors
//! the deleted `TestonlySyncChunkExecutorActor` harness, generalized to the
//! coordinator + per-shard split. See `notes/17-testenv-spice-integration.md`.
//!
//! Only the transport is test-specific; the executor/coordinator/core-writer logic
//! is the real production code.

use near_async::messaging::{Actor, Handler, IntoMultiSender, IntoSender, Sender, noop};
use near_async::test_utils::FakeDelayedActionRunner;
use near_chain::ChainGenesis;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::{
    ExecutionResultEndorsed, ProcessedBlock, SpiceCoreWriterActor,
};
use near_chain::types::RuntimeAdapter;
use near_chain_configs::MutableValidatorSigner;
use near_client::spice::chunk_executor_coordinator::{
    ChunkExecutorCoordinator, PerShardChunkApplied,
};
use near_client::spice::data_distributor_actor::{
    SpiceDataDistributorAdapter, SpiceDistributorOutgoingReceipts,
};
use near_client::spice::executor_shared::ExecutorIncomingUnverifiedReceipts;
use near_client::spice::per_shard_executor::{
    IncomingReceipt, PerShardExecutor, PerShardExecutorSender,
};
use near_client::spice::per_shard_spawner::{PerShardDeps, PerShardSpawner};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_primitives::hash::CryptoHash;
use near_primitives::types::ShardId;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// A unit of synchronous work: dispatch one message to one actor in a [`SpiceNode`].
type Event = Box<dyn FnOnce(&mut SpiceNode) + Send>;

/// Shared FIFO message queue for one node's spice actors.
#[derive(Clone)]
struct EventQueue(Arc<Mutex<VecDeque<Event>>>);

impl EventQueue {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(VecDeque::new())))
    }
    fn push(&self, event: Event) {
        self.0.lock().push_back(event);
    }
    fn pop(&self) -> Option<Event> {
        self.0.lock().pop_front()
    }
}

/// A per-shard executor the coordinator asked to spawn but which hasn't been built
/// yet (built + bootstrapped by the pump, to avoid a re-entrant borrow of the node
/// while the coordinator is mid-handler — mirrors `TestLoopPerShardSpawner`).
struct DeferredSpawn {
    shard_id: ShardId,
    coordinator_sender: Sender<PerShardChunkApplied>,
}

/// `TestEnv` spawner: records the spawn for the pump to build, and hands back a
/// mailbox that routes messages to `node.per_shard[shard_id]` at drain time.
struct TestEnvPerShardSpawner {
    queue: EventQueue,
    deferred: Arc<Mutex<Vec<DeferredSpawn>>>,
}

impl PerShardSpawner for TestEnvPerShardSpawner {
    fn spawn(
        &self,
        shard_id: ShardId,
        coordinator_sender: Sender<PerShardChunkApplied>,
    ) -> PerShardExecutorSender {
        self.deferred.lock().push(DeferredSpawn { shard_id, coordinator_sender });
        per_shard_mailbox(shard_id, self.queue.clone())
    }

    fn retire(&self, _shard_id: ShardId) {
        // No-op: an untracked shard self-drops in `try_apply` (same as test-loop).
    }
}

/// Build a coordinator→per-shard mailbox whose sends enqueue a routing closure
/// into `queue`; the closure dispatches to `node.per_shard[shard_id]` at drain time
/// (by which point the deferred spawn has created the executor).
fn per_shard_mailbox(shard_id: ShardId, queue: EventQueue) -> PerShardExecutorSender {
    let q_block = queue.clone();
    let q_receipt = queue.clone();
    let q_endorsed = queue;
    PerShardExecutorSender {
        processed_block: Sender::from_fn(move |msg: ProcessedBlock| {
            q_block.push(Box::new(move |node| {
                if let Some(executor) = node.per_shard.get_mut(&shard_id) {
                    Handler::<ProcessedBlock>::handle(executor, msg);
                }
            }));
        }),
        incoming_receipt: Sender::from_fn(move |msg: IncomingReceipt| {
            q_receipt.push(Box::new(move |node| {
                if let Some(executor) = node.per_shard.get_mut(&shard_id) {
                    Handler::<IncomingReceipt>::handle(executor, msg);
                }
            }));
        }),
        execution_result_endorsed: Sender::from_fn(move |msg: ExecutionResultEndorsed| {
            q_endorsed.push(Box::new(move |node| {
                if let Some(executor) = node.per_shard.get_mut(&shard_id) {
                    Handler::<ExecutionResultEndorsed>::handle(executor, msg);
                }
            }));
        }),
    }
}

/// One node's spice subsystem, driven synchronously. Owns the real coordinator,
/// per-shard executors, and core-writer; outgoing endorsements / receipts are
/// captured for the `TestEnv`-level cross-node feeding (see [`crate::env::test_env`]).
pub struct SpiceNode {
    coordinator: ChunkExecutorCoordinator,
    per_shard: HashMap<ShardId, PerShardExecutor>,
    core_writer: SpiceCoreWriterActor,
    queue: EventQueue,
    deferred: Arc<Mutex<Vec<DeferredSpawn>>>,
    deps: PerShardDeps,
    produced_endorsements: Arc<Mutex<VecDeque<SpiceChunkEndorsementMessage>>>,
    produced_receipts: Arc<Mutex<VecDeque<SpiceDistributorOutgoingReceipts>>>,
}

impl SpiceNode {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Store,
        chain_genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        validator_signer: MutableValidatorSigner,
        save_trie_changes: bool,
        save_tx_outcomes: bool,
        save_receipt_to_tx: bool,
    ) -> Self {
        let queue = EventQueue::new();
        let deferred = Arc::new(Mutex::new(Vec::new()));
        let produced_endorsements = Arc::new(Mutex::new(VecDeque::new()));
        let produced_receipts = Arc::new(Mutex::new(VecDeque::new()));

        let core_reader = SpiceCoreReader::new(
            store.chain_store(),
            epoch_manager.clone(),
            chain_genesis.gas_limit,
        );

        // Per-shard executors' outbound senders: endorsements + receipts are captured
        // (fed cross-node by the harness); witness/network are dropped (the harness
        // short-circuits distribution, like the old TestonlySync harness).
        let deps = PerShardDeps {
            store: store.clone(),
            transaction_validity_period: chain_genesis.transaction_validity_period,
            save_trie_changes,
            save_tx_outcomes,
            save_receipt_to_tx,
            runtime_adapter: runtime_adapter.clone(),
            epoch_manager: epoch_manager.clone(),
            core_reader: core_reader.clone(),
            validator_signer,
            network_adapter: noop().into_multi_sender(),
            core_writer_sender: capture_sender(produced_endorsements.clone()),
            data_distributor_adapter: SpiceDataDistributorAdapter {
                receipts: capture_sender(produced_receipts.clone()),
                witness: noop().into_sender(),
            },
        };

        let spawner = TestEnvPerShardSpawner { queue: queue.clone(), deferred: deferred.clone() };
        let coordinator = ChunkExecutorCoordinator::new(
            store.clone(),
            runtime_adapter,
            epoch_manager.clone(),
            shard_tracker,
            Box::new(spawner),
            to_coordinator_sender(queue.clone()),
        );

        // The core-writer emits ExecutionResultEndorsed to the coordinator once a
        // block's CER reaches quorum; the chunk-validator path is unused here.
        let core_writer = SpiceCoreWriterActor::new(
            store.chain_store(),
            epoch_manager,
            core_reader,
            to_coordinator_sender(queue.clone()),
            noop().into_sender(),
        );

        let mut node = Self {
            coordinator,
            per_shard: HashMap::new(),
            core_writer,
            queue,
            deferred,
            deps,
            produced_endorsements,
            produced_receipts,
        };
        // Bootstrap the coordinator (spawns executors for genesis-epoch shards) and
        // drain to quiescence.
        let mut runner = FakeDelayedActionRunner::<ChunkExecutorCoordinator>::default();
        node.coordinator.start_actor(&mut runner);
        node.pump();
        node
    }

    /// Drive the subsystem to quiescence: build any deferred per-shard executors
    /// (running their disk self-bootstrap), then dispatch queued messages until the
    /// queue and the deferred-spawn list are both empty.
    fn pump(&mut self) {
        loop {
            let spawns = std::mem::take(&mut *self.deferred.lock());
            if !spawns.is_empty() {
                for DeferredSpawn { shard_id, coordinator_sender } in spawns {
                    let mut executor = self.deps.build(shard_id, coordinator_sender);
                    let mut runner = FakeDelayedActionRunner::<PerShardExecutor>::default();
                    executor.start_actor(&mut runner);
                    self.per_shard.insert(shard_id, executor);
                }
                continue;
            }
            match self.queue.pop() {
                Some(event) => event(self),
                None => break,
            }
        }
    }

    /// Announce a block to the coordinator and the core-writer, then drain.
    pub fn process_block(&mut self, block_hash: CryptoHash) {
        self.queue.push(Box::new(move |node| {
            Handler::<ProcessedBlock>::handle(&mut node.coordinator, ProcessedBlock { block_hash });
        }));
        self.queue.push(Box::new(move |node| {
            Handler::<ProcessedBlock>::handle(&mut node.core_writer, ProcessedBlock { block_hash });
        }));
        self.pump();
    }

    pub fn pop_endorsement(&self) -> Option<SpiceChunkEndorsementMessage> {
        self.produced_endorsements.lock().pop_front()
    }

    /// Feed an endorsement to this node's core-writer (which, on quorum, persists the
    /// CER and notifies the coordinator), then drain.
    pub fn record_endorsement(&mut self, endorsement: SpiceChunkEndorsementMessage) {
        self.queue.push(Box::new(move |node| {
            Handler::<SpiceChunkEndorsementMessage>::handle(&mut node.core_writer, endorsement);
        }));
        self.pump();
    }

    pub fn pop_produced_receipts(&self) -> Option<SpiceDistributorOutgoingReceipts> {
        self.produced_receipts.lock().pop_front()
    }

    /// Deliver a receipt proof reconstructed "from the network" to the coordinator
    /// (which routes it to the destination shard for verification), then drain.
    pub fn feed_incoming_receipts(&mut self, msg: ExecutorIncomingUnverifiedReceipts) {
        self.queue.push(Box::new(move |node| {
            Handler::<ExecutorIncomingUnverifiedReceipts>::handle(&mut node.coordinator, msg);
        }));
        self.pump();
    }
}

/// A `Sender<M>` that routes `M` to the node's coordinator at drain time.
fn to_coordinator_sender<M>(queue: EventQueue) -> Sender<M>
where
    M: Send + 'static,
    ChunkExecutorCoordinator: Handler<M>,
{
    Sender::from_fn(move |msg: M| {
        queue.push(Box::new(move |node| {
            Handler::<M>::handle(&mut node.coordinator, msg);
        }));
    })
}

/// A `Sender<M>` that captures `M` into a buffer for later cross-node feeding.
fn capture_sender<M: Send + 'static>(buffer: Arc<Mutex<VecDeque<M>>>) -> Sender<M> {
    Sender::from_fn(move |msg: M| buffer.lock().push_back(msg))
}
