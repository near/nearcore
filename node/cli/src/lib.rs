extern crate beacon;
extern crate beacon_chain_handler;
extern crate chain;
extern crate clap;
extern crate futures;
extern crate network;
extern crate node_rpc;
extern crate node_runtime;
extern crate primitives;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[cfg_attr(test, macro_use)]
extern crate serde_json;
extern crate storage;
extern crate tokio;
extern crate parking_lot;

use beacon::types::{BeaconBlock, BeaconBlockHeader};
use chain::BlockChain;
use clap::{App, Arg};
use futures::future;
use futures::sync::mpsc::channel;
use network::protocol::{Protocol, ProtocolConfig};
use network::service::{create_network_task, NetworkConfiguration, new_network_service};
use node_rpc::api::RpcImpl;
use node_runtime::{Runtime, StateDbViewer};
use primitives::hash::CryptoHash;
use primitives::signer::InMemorySigner;
use std::path::Path;
use std::sync::Arc;
use storage::{StateDb, Storage};
use parking_lot::{RwLock, Mutex};

pub mod chain_spec;

fn get_storage(base_path: &Path) -> Arc<Storage> {
    let mut storage_path = base_path.to_owned();
    storage_path.push("storage/db");
    Arc::new(storage::open_database(&storage_path.to_string_lossy()))
}

fn start_service(base_path: &Path, chain_spec_path: Option<&Path>) {
    // Create shared-state objects.
    let storage = get_storage(base_path);
    let chain_spec = chain_spec::read_or_default_chain_spec(&chain_spec_path);

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

    // Create RPC Server.
    let state_db_viewer = StateDbViewer::new(beacon_chain.clone(), state_db.clone());
    // TODO: TxFlow should be listening on these transactions.
    let (transactions_tx, _transactions_rx) = channel(1024);
    let (receipts_tx, _receipts_rx) = channel(1024);
    let rpc_impl = RpcImpl::new(state_db_viewer, transactions_tx.clone());
    let rpc_handler = node_rpc::api::get_handler(rpc_impl);
    let server = node_rpc::server::get_server(rpc_handler);
    tokio::spawn(future::lazy(|| {
        server.wait();
        Ok(())
    }));

    // Create a task that consumes the consensuses and produces the beacon chain blocks.
    let signer = Arc::new(InMemorySigner::default());
    let (
        _beacon_block_consensus_body_tx,
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

    // Create task that can import beacon chain blocks from other peers.
    let (beacon_block_tx, beacon_block_rx) = channel(1024);
    let block_importer_task = beacon_chain_handler::importer::create_beacon_block_importer_task(
        beacon_chain.clone(),
        runtime.clone(),
        state_db.clone(),
        beacon_block_rx
    );
    tokio::spawn(block_importer_task);

    // Create protocol and the network_task.
    // Note, that network and RPC are using the same channels to send transactions and receipts for
    // processing.
    let (net_messages_tx, net_messages_rx) = channel(1024);
    let protocol_config = ProtocolConfig::default();
    let protocol = Protocol::<_, BeaconBlockHeader>::new(
        protocol_config,
        beacon_chain.clone(),
        beacon_block_tx.clone(),
        transactions_tx.clone(),
        receipts_tx.clone(),
        net_messages_tx.clone()
    );
    let network_service = Arc::new(Mutex::new(new_network_service(
        &protocol_config,
        NetworkConfiguration::default(),
    )));
    let (network_task, messages_handler_task)
        = create_network_task(network_service, protocol, net_messages_rx);
    tokio::spawn(messages_handler_task);
    tokio::run(network_task);
}

pub fn run() {
    let matches = App::new("near")
        .arg(
            Arg::with_name("base_path")
                .short("b")
                .long("base-path")
                .value_name("PATH")
                .help("Sets a base path for persisted files")
                .takes_value(true),
        ).arg(
            Arg::with_name("chain_spec_file")
                .short("c")
                .long("chain-spec-file")
                .value_name("CHAIN_SPEC_FILE")
                .help("Sets a file location to read a custom chain spec")
                .takes_value(true),
        ).get_matches();

    let base_path =
        matches.value_of("base_path").map(|x| Path::new(x)).unwrap_or_else(|| Path::new("."));

    let chain_spec_path = matches.value_of("chain_spec_file").map(|x| Path::new(x));

    start_service(base_path, chain_spec_path);
}
