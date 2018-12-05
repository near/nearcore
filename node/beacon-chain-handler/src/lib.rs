extern crate beacon;
extern crate chain;
extern crate node_runtime;
extern crate primitives;
extern crate storage;

use beacon::types::BeaconBlock;
use chain::BlockChain;
use node_runtime::{ApplyState, Runtime};
use primitives::traits::{Block, Header};
use primitives::traits::Signer;
use primitives::types::ConsensusBlockBody;
use primitives::types::SignedTransaction;
use std::sync::Arc;
use storage::StateDb;

pub trait ConsensusHandler<B: Block, P>: Send + Sync {
    fn produce_block(&self, body: ConsensusBlockBody<P>) -> B;
}

pub struct BeaconBlockProducer {
    beacon_chain: Arc<BlockChain<BeaconBlock>>,
    runtime: Runtime,
    signer: Arc<Signer>,
    state_db: Arc<StateDb>,
}

impl BeaconBlockProducer {
    pub fn new(
        beacon_chain: Arc<BlockChain<BeaconBlock>>,
        runtime: Runtime,
        signer: Arc<Signer>,
        state_db: Arc<StateDb>,
    ) -> Self {
        BeaconBlockProducer {
            beacon_chain,
            runtime,
            signer,
            state_db,
        }
    }
}

pub type BeaconChainConsensusBlockBody = ConsensusBlockBody<Vec<SignedTransaction>>;

impl ConsensusHandler<BeaconBlock, Vec<SignedTransaction>> for BeaconBlockProducer {
    fn produce_block(&self, body: BeaconChainConsensusBlockBody) -> BeaconBlock {
        // TODO: verify signature
        let transactions = body.messages.iter()
            .flat_map(|message| message.clone().body.payload)
            .collect();

        // TODO: compute actual merkle root and state, as well as signature, and
        // use some reasonable fork-choice rule
        let last_block = self.beacon_chain.best_block();
        let apply_state = ApplyState {
            root: last_block.header().body.merkle_root_state,
            parent_block_hash: last_block.hash(),
            block_index: last_block.header().index() + 1,
        };
        let (filtered_transactions, mut apply_result) =
            self.runtime.apply(&apply_state, transactions);
        self.state_db.commit(&mut apply_result.transaction).ok();
        let mut block = BeaconBlock::new(
            last_block.header().index() + 1,
            last_block.hash(),
            apply_result.root,
            filtered_transactions,
        );
        block.sign(&self.signer);
        self.beacon_chain.insert_block(block.clone());
        block
    }
}
