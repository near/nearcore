extern crate beacon;
extern crate clap;
extern crate client;
extern crate network;
extern crate primitives;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[cfg_attr(test, macro_use)]
extern crate serde_json;
extern crate service;
extern crate storage;

use clap::{App, Arg};
use std::path::Path;
use std::sync::Arc;
use storage::Storage;

pub mod chain_spec;

fn get_storage(base_path: &Path) -> Arc<Storage> {
    let mut storage_path = base_path.to_owned();
    storage_path.push("storage/db");
    Arc::new(storage::open_database(&storage_path.to_string_lossy()))
}

fn start_service(base_path: &Path, chain_spec_path: Option<&Path>) {
    let _storage = get_storage(base_path);
    let _chain_spec = chain_spec::read_or_default_chain_spec(&chain_spec_path);
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
