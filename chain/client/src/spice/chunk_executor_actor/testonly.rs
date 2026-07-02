//! Synchronous test harness around the chunk-executor coordinator. Needed for
//! integration tests; not for production use.

use super::{ChunkExecutorActor, ExecutorApplyChunksDone, ExecutorIncomingUnverifiedReceipts};
use crate::spice::data_distributor_actor::{
    SpiceDataDistributorAdapter, SpiceDistributorOutgoingReceipts,
};
use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use near_async::futures::AsyncComputationSpawner;
use near_async::messaging::{Handler, IntoSender, Sender, noop};
use near_chain::ChainGenesis;
use near_chain::spice::chunk_application::ChunkPersistenceConfig;
use near_chain::spice::core::SpiceCoreReader;
use near_chain::spice::core_writer_actor::{ProcessedBlock, SpiceCoreWriterActor};
use near_chain::types::RuntimeAdapter;
use near_chain_configs::MutableValidatorSigner;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::SpiceChunkEndorsementMessage;
use near_network::types::PeerManagerAdapter;
use near_store::Store;
use near_store::adapter::StoreAdapter;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;

struct FakeSpawner {
    sc: UnboundedSender<Box<dyn FnOnce() + Send>>,
}

impl FakeSpawner {
    fn new() -> (FakeSpawner, UnboundedReceiver<Box<dyn FnOnce() + Send>>) {
        let (sc, rc) = unbounded();
        (Self { sc }, rc)
    }
}

impl AsyncComputationSpawner for FakeSpawner {
    fn spawn_boxed(&self, _name: &str, f: Box<dyn FnOnce() + Send>) {
        self.sc.unbounded_send(f).unwrap();
    }
}

/// This struct is needed for integration tests, otherwise it should not be used.
pub struct TestonlySyncChunkExecutorActor {
    actor: ChunkExecutorActor,
    actor_rc: UnboundedReceiver<ExecutorApplyChunksDone>,
    tasks_rc: UnboundedReceiver<Box<dyn FnOnce() + Send>>,
    core_writer_actor: SpiceCoreWriterActor,
    produced_endorsements: Arc<RwLock<VecDeque<SpiceChunkEndorsementMessage>>>,
    produced_receipt_proofs: Arc<RwLock<VecDeque<SpiceDistributorOutgoingReceipts>>>,
}

impl TestonlySyncChunkExecutorActor {
    pub fn new(
        store: Store,
        genesis: &ChainGenesis,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: ShardTracker,
        network_adapter: PeerManagerAdapter,
        validator_signer: MutableValidatorSigner,
        chunk_persistence_config: ChunkPersistenceConfig,
    ) -> Self {
        let (actor_sc, actor_rc) = unbounded();
        let myself_sender = Sender::from_fn(move |event: ExecutorApplyChunksDone| {
            actor_sc.unbounded_send(event).unwrap();
        });
        let (spawner, tasks_rc) = FakeSpawner::new();

        let core_reader = SpiceCoreReader::new(
            runtime_adapter.store().chain_store(),
            epoch_manager.clone(),
            genesis.gas_limit,
        );
        let core_writer_actor = SpiceCoreWriterActor::new(
            runtime_adapter.store().chain_store(),
            epoch_manager.clone(),
            core_reader,
            noop().into_sender(),
            noop().into_sender(),
        );
        let produced_endorsements = Arc::new(RwLock::new(VecDeque::new()));
        let core_writer_sender = Sender::from_fn({
            let endorsements = produced_endorsements.clone();
            move |message| endorsements.write().push_back(message)
        });
        let produced_receipt_proofs = Arc::new(RwLock::new(VecDeque::new()));
        let data_distributor_adapter = SpiceDataDistributorAdapter {
            receipts: Sender::from_fn({
                let produced_receipt_proofs = produced_receipt_proofs.clone();
                move |message| produced_receipt_proofs.write().push_back(message)
            }),
            witness: noop().into_sender(),
        };
        Self {
            actor: ChunkExecutorActor::new(
                store,
                genesis,
                runtime_adapter,
                epoch_manager,
                shard_tracker,
                network_adapter,
                validator_signer,
                Arc::new(spawner),
                myself_sender,
                core_writer_sender,
                data_distributor_adapter,
                chunk_persistence_config,
            ),
            actor_rc,
            tasks_rc,
            core_writer_actor,
            produced_endorsements,
            produced_receipt_proofs,
        }
    }

    pub fn pop_endorsement(&self) -> Option<SpiceChunkEndorsementMessage> {
        self.produced_endorsements.write().pop_front()
    }

    pub fn record_endorsement(&mut self, endorsement: SpiceChunkEndorsementMessage) {
        self.core_writer_actor.handle(endorsement);
    }

    pub fn pop_produced_receipts(&self) -> Option<SpiceDistributorOutgoingReceipts> {
        self.produced_receipt_proofs.write().pop_front()
    }

    pub fn handle_incoming_receipts(&mut self, receipts: ExecutorIncomingUnverifiedReceipts) {
        self.actor.handle(receipts);
        self.run_internal_events();
    }

    pub fn handle_processed_block(&mut self, ProcessedBlock { block_hash }: ProcessedBlock) {
        self.actor.handle_processed_block(&block_hash).unwrap();
        self.run_internal_events();
        self.core_writer_actor.handle(ProcessedBlock { block_hash });
    }

    fn run_internal_events(&mut self) {
        loop {
            let mut events_processed = 0;
            while let Ok(Some(task)) = self.tasks_rc.try_next() {
                events_processed += 1;
                task();
            }
            while let Ok(Some(event)) = self.actor_rc.try_next() {
                events_processed += 1;
                let block_hash = event.block_hash;
                let outgoing_proofs = self.actor.per_shard_apply_done(event);
                self.actor.coordinator_post_apply(&block_hash, &outgoing_proofs).unwrap();
            }
            if events_processed == 0 {
                break;
            }
        }
    }
}
