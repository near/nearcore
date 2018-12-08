use node_rpc::api::RpcImpl;
use node_runtime::{Runtime, StateDbViewer};
use futures::sync::mpsc::channel;
use futures::future;
use std::sync::Arc;
use storage::StateDb;
use parking_lot::RwLock;
use storage::test_utils::create_memory_db;
use chain::BlockChain;
use primitives::hash::CryptoHash;
use primitives::signer::InMemorySigner;
use beacon::types::BeaconBlock;
use node_runtime::test_utils::generate_test_chain_spec;
use super::pass_through_consensus;

pub fn start_service() {
    // Create shared-state objects.
    let storage = Arc::new(create_memory_db());
    let chain_spec = generate_test_chain_spec();

    let state_db = Arc::new(StateDb::new(storage.clone()));
    let runtime = Arc::new(RwLock::new(Runtime::new(state_db.clone())));
    let genesis_root = runtime.write().apply_genesis_state(
        &chain_spec.accounts,
        &chain_spec.genesis_wasm,
        &chain_spec.initial_authorities,
    );

    let genesis = BeaconBlock::new(
        0, CryptoHash::default(), genesis_root, vec![], vec![]
    );
    let beacon_chain = Arc::new(BlockChain::new(genesis, storage.clone()));
    let signer = Arc::new(InMemorySigner::default());

    // Create RPC Server.
    let state_db_viewer = StateDbViewer::new(beacon_chain.clone(), state_db.clone());

    // Create RPC Server.
    // TODO: TxFlow should be listening on these transactions.
    let (transactions_tx, transactions_rx) = channel(1024);
    let rpc_impl = RpcImpl::new(state_db_viewer,
                                transactions_tx.clone(),
                                signer.clone());
    let rpc_handler = node_rpc::api::get_handler(rpc_impl);
    let server = node_rpc::server::get_server(rpc_handler);
    tokio::spawn(future::lazy(|| {
        server.wait();
        Ok(())
    }));

    // Create a task that consumes the consensuses and produces the beacon chain blocks.
    let (
        beacon_block_consensus_body_tx,
        beacon_block_consensus_body_rx,
    ) = channel(1024);
    let block_producer_task = beacon_chain_handler::producer::create_beacon_block_producer_task(
        beacon_chain.clone(),
        runtime.clone(),
        signer.clone(),
        state_db.clone(),
        beacon_block_consensus_body_rx,
    );
    tokio::spawn(block_producer_task);

    // Create pass-through consensus.
    tokio::run(
        pass_through_consensus::create_task(transactions_rx, beacon_block_consensus_body_tx));
}
