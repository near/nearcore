extern crate beacon;
extern crate chain;
extern crate clap;
extern crate client;
extern crate futures;
extern crate node_rpc;
extern crate node_runtime;
extern crate primitives;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[cfg_attr(test, macro_use)]
extern crate serde_json;
extern crate service;
extern crate storage;
extern crate tokio;

use clap::{App, Arg};
use futures::future;
use futures::sync::mpsc::channel;
use node_rpc::api::RpcImpl;
use std::path::Path;
use std::sync::Arc;
use storage::Storage;
use node_runtime::Runtime;
use storage::StateDb;
use beacon::types::BeaconBlock;
use primitives::hash::CryptoHash;
use chain::BlockChain;
use node_runtime::StateDbViewer;

mod chain_spec;

fn get_storage(base_path: &Path) -> Arc<Storage> {
    let mut storage_path = base_path.to_owned();
    storage_path.push("storage/db");
    Arc::new(storage::open_database(&storage_path.to_string_lossy()))
}

fn start_service(base_path: &Path, chain_spec_path: Option<&Path>) {
    let storage = get_storage(base_path);
    let chain_spec = chain_spec::read_or_default_chain_spec(&chain_spec_path);

    let state_db = Arc::new(StateDb::new(storage.clone()));
    let runtime = Runtime::new(state_db);
    let genesis_root = runtime.apply_genesis_state(
        &chain_spec.accounts,
        &chain_spec.genesis_wasm,
    );

    let genesis = BeaconBlock::new(0, CryptoHash::default(), genesis_root, vec![]);
    let beacon_chain = BlockChain::new(genesis, storage.clone());

    let state_db_viewer = StateDbViewer::new(beacon_chain, runtime);
    let (submit_txn_tx, _submit_txn_rx) = channel(1024);
    let rpc_impl = RpcImpl::new(state_db_viewer, submit_txn_tx);
    let rpc_handler = node_rpc::api::get_handler(rpc_impl);
    let server = node_rpc::server::get_server(rpc_handler);

    tokio::run(future::lazy(|| {
        server.wait();
        Ok(())
    }));
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
