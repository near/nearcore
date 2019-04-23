use std::path::PathBuf;
use std::str::FromStr;

use clap::{Arg, ArgMatches};
use node_runtime::chain_spec::{ChainSpec, ALICE_ID};
use primitives::types::AccountId;

const DEFAULT_BASE_PATH: &str = ".";
const DEFAULT_LOG_LEVEL: &str = "Info";
const MAX_NUM_BLOCKS_REQUEST: u64 = 100;
const MAX_BLOCK_SIZE: u32 = 10000;

#[derive(Clone)]
pub struct ClientConfig {
    pub base_path: PathBuf,
    pub account_id: Option<AccountId>,
    pub public_key: Option<String>,
    pub chain_spec: ChainSpec,
    /// Maximum number of blocks to be fetched in one request.
    pub block_fetch_limit: u64,
    /// Maximum block size
    pub block_size_limit: u32,
    pub log_level: log::LevelFilter,
}

impl ClientConfig {
    pub fn default_devnet() -> Self {
        Self {
            base_path: PathBuf::from(DEFAULT_BASE_PATH),
            account_id: Some(String::from(ALICE_ID)),
            public_key: None,
            chain_spec: ChainSpec::default_devnet(),
            block_fetch_limit: MAX_NUM_BLOCKS_REQUEST,
            block_size_limit: MAX_BLOCK_SIZE,
            log_level: log::LevelFilter::Info,
        }
    }
}

pub fn get_args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![
        Arg::with_name("base_path")
            .short("d")
            .long("base-path")
            .value_name("PATH")
            .help("Specify a base path for persisted files.")
            .default_value(DEFAULT_BASE_PATH)
            .takes_value(true),
        Arg::with_name("chain_spec_file")
            .short("c")
            .long("chain-spec-file")
            .value_name("CHAIN_SPEC")
            .help("Specify a file location to read a custom chain spec.")
            .takes_value(true),
        Arg::with_name("account_id")
            .short("a")
            .long("account-id")
            .value_name("ACCOUNT_ID")
            .help("Set the account id of the node")
            .takes_value(true),
        Arg::with_name("public_key")
            .short("k")
            .long("public-key")
            .value_name("PUBLIC_KEY")
            .help("Sets public key to sign with, can be omitted with 1 file in keystore")
            .takes_value(true),
        Arg::with_name("block_fetch_limit")
            .short("f")
            .long("block-fetch-limit")
            .value_name("BLOCK_FETCH_LIMIT")
            .help("Sets maximum number of blocks that should be fetched in one request.")
            .takes_value(true)
            .default_value("100"),
        Arg::with_name("block_size_limit")
            .long("block-size-limit")
            .value_name("BLOCK_SIZE_LIMIT")
            .help("Sets maximum block size")
            .takes_value(true)
            .default_value("10000"),
        Arg::with_name("log_level")
            .short("l")
            .long("log-level")
            .value_name("LOG_LEVEL")
            .help("Set log level. Can override specific targets with RUST_LOG.")
            .possible_values(&["Debug", "Info", "Warn"])
            .default_value(DEFAULT_LOG_LEVEL)
            .takes_value(true),
    ]
}

pub fn from_matches(matches: &ArgMatches, default_chain_spec: ChainSpec) -> ClientConfig {
    let base_path = matches.value_of("base_path").map(PathBuf::from).unwrap();
    let account_id = matches.value_of("account_id").map(String::from);
    let public_key = matches.value_of("public_key").map(String::from);
    let block_fetch_limit =
        matches.value_of("block_fetch_limit").and_then(|s| s.parse::<u64>().ok()).unwrap();
    let block_size_limit =
        matches.value_of("block_size_limit").and_then(|s| s.parse::<u32>().ok()).unwrap();
    let log_level = matches.value_of("log_level").map(log::LevelFilter::from_str).unwrap().unwrap();

    let chain_spec_path = matches.value_of("chain_spec_file").map(PathBuf::from);
    let chain_spec = ChainSpec::from_file_or_default(&chain_spec_path, default_chain_spec);
    ClientConfig {
        base_path,
        account_id,
        public_key,
        block_fetch_limit,
        block_size_limit,
        chain_spec,
        log_level,
    }
}
