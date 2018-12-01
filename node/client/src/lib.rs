extern crate beacon;
#[macro_use]
extern crate log;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;
extern crate chain as blockchain;

use beacon::authority::{Authority, AuthorityConfig};
use beacon::types::{AuthorityProposal, BeaconBlock};
use blockchain::BlockChain;
use chain_spec::ChainSpec;
use import_queue::ImportQueue;
use node_runtime::{ApplyState, Runtime};
use parking_lot::RwLock;
use primitives::hash::CryptoHash;
use primitives::signature::PublicKey;
use primitives::traits::{Block, GenericResult, Header, Signer};
use primitives::types::{BlockId, SignedTransaction, ViewCall, ViewCallResult};
use std::sync::Arc;
use storage::{StateDb, Storage};

mod import_queue;

pub mod chain;
pub mod chain_spec;
#[cfg(feature = "test-utils")]
pub mod test_utils;

#[allow(dead_code)]
pub struct Client {
    signer: Arc<Signer>,
    state_db: Arc<StateDb>,
    runtime: Runtime,
    authority: Authority,
    beacon_chain: BlockChain<BeaconBlock>,
    // transaction pool (put here temporarily)
    tx_pool: RwLock<Vec<SignedTransaction>>,
    // import queue for receiving blocks
    import_queue: RwLock<ImportQueue>,
}

impl Client {
    pub fn new(chain_spec: &ChainSpec, storage: Arc<Storage>, signer: Arc<Signer>) -> Self {
        let state_db = Arc::new(StateDb::new(storage.clone()));
        let runtime = Runtime::new(state_db.clone());
        let genesis_root =
            runtime.apply_genesis_state(&chain_spec.accounts, &chain_spec.genesis_wasm);

        let genesis = BeaconBlock::new(0, CryptoHash::default(), genesis_root, vec![]);
        let beacon_chain = BlockChain::new(genesis, storage);
        let authority_config = AuthorityConfig {
            initial_authorities: chain_spec
                .initial_authorities
                .iter()
                .map(|(public_key, amount)| AuthorityProposal {
                    public_key: PublicKey::from(public_key),
                    amount: *amount,
                }).collect(),
            epoch_length: chain_spec.beacon_chain_epoch_length,
            num_seats_per_slot: chain_spec.beacon_chain_num_seats_per_slot,
        };
        let authority = Authority::new(authority_config, &beacon_chain);

        Client {
            signer,
            state_db,
            beacon_chain,
            runtime,
            authority,
            tx_pool: RwLock::new(vec![]),
            import_queue: RwLock::new(ImportQueue::new()),
        }
    }

    pub fn receive_transaction(&self, t: SignedTransaction) {
        debug!(target: "client", "receive transaction {:?}", t);
        self.tx_pool.write().push(t);
    }

    pub fn view_call(&self, view_call: &ViewCall) -> ViewCallResult {
        self.runtime.view(self.beacon_chain.best_block().header().body.merkle_root_state, view_call)
    }

    pub fn handle_signed_transaction(&self, t: SignedTransaction) -> GenericResult {
        debug!(target: "client", "handle transaction {:?}", t);
        self.tx_pool.write().push(t);
        Ok(())
    }

    #[allow(unused)]
    fn validate_signature(&self, block: &BeaconBlock) -> bool {
        // TODO: validate multisig
        true
    }

    /// Import a block. Returns true if it is successfully inserted into the chain
    fn import_block(&self, block: BeaconBlock) -> bool {
        if self.beacon_chain.is_known(&block.hash()) {
            return false;
        }
        let parent_hash = block.header().parent_hash();
        if self.beacon_chain.is_known(&parent_hash) && self.validate_signature(&block) {
            let (header, transactions) = block.deconstruct();
            let num_transactions = transactions.len();
            // we can unwrap because parent is guaranteed to exist
            let last_header = self
                .beacon_chain
                .get_header(&BlockId::Hash(parent_hash))
                .expect("Parent is known but header not found.");
            let apply_state = ApplyState {
                root: last_header.body.merkle_root_state,
                block_index: last_header.body.index,
                parent_block_hash: parent_hash,
            };
            let (filtered_transactions, mut apply_result) =
                self.runtime.apply(&apply_state, transactions);
            if apply_result.root != header.body.merkle_root_state
                || filtered_transactions.len() != num_transactions
            {
                // TODO: something really bad happened
                return false;
            }
            self.state_db.commit(&mut apply_result.transaction).ok();
            // TODO: figure out where to store apply_result.authority_change_set.
            let block = Block::new(header, filtered_transactions);
            self.beacon_chain.insert_block(block);
            true
        } else {
            self.import_queue.write().insert(block);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chain::Chain;
    use primitives::types::MerkleHash;
    use test_utils::generate_test_client;

    #[test]
    fn test_import_queue_empty() {
        let client = generate_test_client();
        let genesis_block = client.beacon_chain.best_block();
        let block1 = BeaconBlock::new(
            1,
            genesis_block.hash(),
            genesis_block.header().body.merkle_root_state,
            vec![],
        );
        assert!(client.import_block(block1));
        assert_eq!(client.import_queue.read().len(), 0);
    }

    #[test]
    fn test_import_queue_non_empty() {
        let client = generate_test_client();
        let genesis_block = client.beacon_chain.best_block();
        let block1 = BeaconBlock::new(
            1,
            genesis_block.hash(),
            genesis_block.header().body.merkle_root_state,
            vec![],
        );
        let block2 = BeaconBlock::new(
            2,
            block1.hash(),
            genesis_block.header().body.merkle_root_state,
            vec![],
        );
        assert!(!client.import_block(block2));
        assert_eq!(client.import_queue.read().len(), 1);
        assert!(client.import_block(block1));
        // block2 is still in the queue.
        assert_eq!(client.import_queue.read().len(), 1);
    }

    #[test]
    fn test_duplicate_import() {
        let client = generate_test_client();
        let parent_hash = client.beacon_chain.genesis_hash;
        let block0 = BeaconBlock::new(0, parent_hash, MerkleHash::default(), vec![]);
        assert!(!client.import_block(block0));
        assert_eq!(client.import_queue.read().len(), 0);
    }

    #[test]
    fn test_import_blocks() {
        let client = generate_test_client();
        let genesis_block = client.beacon_chain.best_block();
        let block1 = BeaconBlock::new(
            1,
            genesis_block.hash(),
            genesis_block.header().body.merkle_root_state,
            vec![],
        );
        let block2 = BeaconBlock::new(
            2,
            block1.hash(),
            genesis_block.header().body.merkle_root_state,
            vec![],
        );
        client.import_blocks(vec![block1, block2]);
        assert_eq!(client.import_queue.read().len(), 0);
    }

    #[test]
    fn test_block_prod() {
        let client = generate_test_client();
        let block1 = client.prod_block();
        // Already imported & best block is the this.
        assert!(!client.import_block(block1.clone()));
        assert_eq!(client.beacon_chain.best_block(), block1);
    }
}
