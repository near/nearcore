use std::path::PathBuf;
use std::str::FromStr;

use clap::{Arg, ArgMatches};

use crate::chain_spec::read_or_default_chain_spec;
use crate::chain_spec::ChainSpec;
use primitives::types::AccountId;

const DEFAULT_BASE_PATH: &str = ".";
const DEFAULT_LOG_LEVEL: &str = "Info";

pub struct ClientConfig {
    pub base_path: PathBuf,
    pub account_id: AccountId,
    pub public_key: Option<String>,
    pub chain_spec: ChainSpec,
    pub log_level: log::LevelFilter,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from(DEFAULT_BASE_PATH),
            account_id: String::from("alice.near"),
            public_key: None,
            chain_spec: read_or_default_chain_spec(&None),
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
            .takes_value(true)
            // TODO(#282): Remove default account id from here.
            .default_value("alice.near"),
        Arg::with_name("public_key")
            .short("k")
            .long("public-key")
            .value_name("PUBLIC_KEY")
            .help("Sets public key to sign with, can be omitted with 1 file in keystore")
            .takes_value(true),
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

pub fn from_matches(matches: &ArgMatches) -> ClientConfig {
    let base_path = matches.value_of("base_path").map(PathBuf::from).unwrap();
    let account_id = matches.value_of("account_id").map(String::from).unwrap();
    let public_key = matches.value_of("public_key").map(String::from);
    let log_level = matches.value_of("log_level").map(log::LevelFilter::from_str).unwrap().unwrap();

    let chain_spec_path = matches.value_of("chain_spec_file").map(PathBuf::from);
    let chain_spec = read_or_default_chain_spec(&chain_spec_path);
    ClientConfig { base_path, account_id, public_key, chain_spec, log_level }
}
