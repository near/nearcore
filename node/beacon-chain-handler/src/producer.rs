//! ConsensusHandler consumes consensuses, retrieves the most recent state, computes the new
//! state, signs it and puts in on the BeaconChain.
use beacon::types::BeaconBlock;
use chain::BlockChain;
use node_runtime::{ApplyState, Runtime};
use primitives::traits::{Block, Header};
use primitives::traits::Signer;
use primitives::types::ConsensusBlockBody;
use primitives::types::{SignedTransaction, ReceiptTransaction};
use std::sync::Arc;
use storage::StateDb;
use parking_lot::RwLock;
use futures::sync::mpsc::Receiver;
use futures::{Future, Stream, future};

pub fn create_beacon_block_producer_task(
    beacon_chain: Arc<BlockChain<BeaconBlock>>,
    runtime: Arc<RwLock<Runtime>>,
    signer: Arc<Signer>,
    state_db: Arc<StateDb>,
    receiver: Receiver<BeaconChainConsensusBlockBody>
) -> impl Future<Item = (), Error = ()> {
    let beacon_block_producer = BeaconBlockProducer::new(
        beacon_chain,
        runtime,
        signer,
        state_db,
    );
    receiver.fold(beacon_block_producer, |beacon_block_producer, body| {
        beacon_block_producer.produce_block(body);
        future::ok(beacon_block_producer)
    }).and_then(|_| Ok(()))
}

pub trait ConsensusHandler<B: Block, P>: Send + Sync {
    fn produce_block(&self, body: ConsensusBlockBody<P>) -> B;
}

pub struct BeaconBlockProducer {
    beacon_chain: Arc<BlockChain<BeaconBlock>>,
    runtime: Arc<RwLock<Runtime>>,
    signer: Arc<Signer>,
    state_db: Arc<StateDb>,
}

impl BeaconBlockProducer {
    pub fn new(
        beacon_chain: Arc<BlockChain<BeaconBlock>>,
        runtime: Arc<RwLock<Runtime>>,
        signer: Arc<Signer>,
        state_db: Arc<StateDb>,
    ) -> Self {
        Self {
            beacon_chain,
            runtime,
            signer,
            state_db,
        }
    }
}

pub type BeaconChainPayload = (Vec<SignedTransaction>, Vec<ReceiptTransaction>);
pub type BeaconChainConsensusBlockBody = ConsensusBlockBody<BeaconChainPayload>;

impl ConsensusHandler<BeaconBlock, BeaconChainPayload> for BeaconBlockProducer {
    fn produce_block(&self, body: BeaconChainConsensusBlockBody) -> BeaconBlock {
        // TODO: verify signature
        let transactions = body.messages.iter()
            .flat_map(|message| message.body.payload.0.clone())
            .collect();
        let mut receipts = body.messages.iter()
            .flat_map(|message| message.body.payload.1.clone())
            .collect();

        // TODO: compute actual merkle root and state, as well as signature, and
        // use some reasonable fork-choice rule
        let last_block = self.beacon_chain.best_block();
        let apply_state = ApplyState {
            root: last_block.header().body.merkle_root_state,
            parent_block_hash: last_block.header_hash(),
            block_index: last_block.header().index() + 1,
        };
        let (filtered_transactions, filtered_receipts, mut apply_result) =
            self.runtime.write().apply(&apply_state, transactions, &mut receipts);
        self.state_db.commit(&mut apply_result.transaction).ok();
        let mut block = BeaconBlock::new(
            last_block.header().index() + 1,
            last_block.header_hash(),
            apply_result.root,
            filtered_transactions,
            filtered_receipts,
        );
        block.sign(&self.signer);
        self.beacon_chain.insert_block(block.clone());
        info!(target: "block_producer", "Block body: {:?}", block.body);
        block
    }
}
