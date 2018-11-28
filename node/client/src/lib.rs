extern crate beacon;
#[macro_use]
extern crate log;
extern crate network;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;

use std::sync::Arc;

use parking_lot::RwLock;

use beacon::authority::{Authority, AuthorityConfig};
use beacon::chain::{BlockChain, ChainConfig};
use beacon::types::{BeaconBlock, BeaconBlockHeader};
use chain_spec::ChainSpec;
use import_queue::ImportQueue;
use node_runtime::{ApplyState, Runtime};
use primitives::hash::CryptoHash;
use primitives::traits::{Block, GenericResult, Signer};
use primitives::types::{BlockId, SignedTransaction, ViewCall, ViewCallResult};
use storage::{StateDb, Storage};

mod import_queue;

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
    pub fn new(_chain_spec: &ChainSpec, storage: Arc<Storage>, signer: Arc<Signer>) -> Self {
        let state_db = Arc::new(StateDb::new(storage.clone()));
        let chain_config = ChainConfig {
            extra_col: storage::COL_BEACON_EXTRA,
            header_col: storage::COL_BEACON_HEADERS,
            block_col: storage::COL_BEACON_BLOCKS,
            index_col: storage::COL_BEACON_INDEX,
        };
        let runtime = Runtime::default();
        let genesis_root = runtime.genesis_state(&state_db);

        let genesis = BeaconBlock::new(0, CryptoHash::default(), genesis_root, vec![]);
        let beacon_chain = BlockChain::new(chain_config, genesis, storage);
        let authority_config =
            AuthorityConfig { initial_authorities: vec![signer.public_key()], epoch_length: 10 };
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
        self.runtime.view(
            self.state_db.clone(),
            self.beacon_chain.best_block().header().merkle_root_state,
            view_call,
        )
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
        let parent_hash = block.header.parent_hash;
        if self.beacon_chain.is_known(&parent_hash) && self.validate_signature(&block) {
            let (header, transactions) = block.deconstruct();
            let num_transactions = transactions.len();
            // we can unwrap because parent is guaranteed to exist
            let last_header = self
                .beacon_chain
                .get_header(&BlockId::Hash(parent_hash))
                .expect("Parent is known but header not found.");
            let apply_state = ApplyState {
                root: last_header.merkle_root_state,
                block_index: last_header.index,
                parent_block_hash: parent_hash,
            };
            let (filtered_transactions, mut apply_result) =
                self.runtime.apply(self.state_db.clone(), &apply_state, transactions);
            if apply_result.root != header.merkle_root_state
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

impl network::client::Client<BeaconBlock> for Client {
    fn get_block(&self, id: &BlockId) -> Option<BeaconBlock> {
        self.beacon_chain.get_block(id)
    }

    fn get_header(&self, id: &BlockId) -> Option<BeaconBlockHeader> {
        self.beacon_chain.get_header(id)
    }

    fn best_hash(&self) -> CryptoHash {
        let best_block = self.beacon_chain.best_block();
        best_block.hash()
    }

    fn best_index(&self) -> u64 {
        let best_block = self.beacon_chain.best_block();
        best_block.header().index
    }

    fn genesis_hash(&self) -> CryptoHash {
        self.beacon_chain.genesis_hash
    }

    fn import_blocks(&self, blocks: Vec<BeaconBlock>) {
        for block in blocks {
            let mut hash = block.hash();
            let mut b = block;
            while self.import_block(b) {
                match self.import_queue.write().remove(&hash) {
                    Some(next_block) => {
                        b = next_block;
                        hash = b.hash();
                    }
                    None => {
                        break;
                    }
                };
            }
        }
    }

    fn prod_block(&self) -> BeaconBlock {
        // TODO: compute actual merkle root and state, as well as signature, and
        // use some reasonable fork-choice rule
        let last_block = self.beacon_chain.best_block();
        let transactions = std::mem::replace(&mut *self.tx_pool.write(), vec![]);
        let apply_state = ApplyState {
            root: last_block.header().merkle_root_state,
            parent_block_hash: last_block.hash(),
            block_index: last_block.header().index + 1,
        };
        let (filtered_transactions, mut apply_result) =
            self.runtime.apply(self.state_db.clone(), &apply_state, transactions);
        self.state_db.commit(&mut apply_result.transaction).ok();
        let mut block = BeaconBlock::new(
            last_block.header().index + 1,
            last_block.hash(),
            apply_result.root,
            filtered_transactions,
        );
        block.sign(&self.signer);
        self.beacon_chain.insert_block(block.clone());
        block
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::Mutex;

    // test with protocol
    use network::client::Client as NetworkClient;
    use network::io::NetSyncIo;
    use network::protocol::{Protocol, ProtocolConfig, ProtocolHandler};
    use network::test_utils as network_test_utils;
    use primitives::traits::GenericResult;
    use primitives::types::{MerkleHash, TransactionBody};

    use super::*;

    use test_utils::generate_test_client;

    #[test]
    fn test_import_queue_empty() {
        let client = generate_test_client();
        let genesis_block = client.beacon_chain.best_block();
        let block1 = BeaconBlock::new(
            1,
            genesis_block.hash(),
            genesis_block.header().merkle_root_state,
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
            genesis_block.header().merkle_root_state,
            vec![],
        );
        let block2 =
            BeaconBlock::new(2, block1.hash(), genesis_block.header().merkle_root_state, vec![]);
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
            genesis_block.header().merkle_root_state,
            vec![],
        );
        let block2 =
            BeaconBlock::new(2, block1.hash(), genesis_block.header().merkle_root_state, vec![]);
        network::client::Client::import_blocks(&client, vec![block1, block2]);
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

    struct MockHandler {
        pub client: Arc<Client>,
    }

    impl ProtocolHandler for MockHandler {
        fn handle_transaction(&self, t: SignedTransaction) -> GenericResult {
            self.client.handle_signed_transaction(t)
        }
    }

    #[test]
    fn test_protocol_and_client() {
        let client = Arc::new(generate_test_client());
        let handler = MockHandler { client: client.clone() };
        let config = ProtocolConfig::new_with_default_id(network_test_utils::special_secret());
        let protocol = Protocol::new(config, handler, client.clone());
        let network_service = Arc::new(Mutex::new(network_test_utils::default_network_service()));
        let mut net_sync = NetSyncIo::new(&network_service, protocol.config.protocol_id);
        protocol.on_transaction_message(SignedTransaction::new(
            123,
            TransactionBody::new(1, 1, 2, 10, String::new(), vec![]),
        ));
        assert_eq!(client.tx_pool.read().len(), 1);
        assert_eq!(client.import_queue.read().len(), 0);
        protocol.prod_block::<BeaconBlockHeader>(&mut net_sync);
        assert_eq!(client.tx_pool.read().len(), 0);
        assert_eq!(client.import_queue.read().len(), 0);
        assert_eq!(client.best_index(), 1);
    }
}
