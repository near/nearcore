///! Generate a ChainSpec that can be used for running alphanet.
///! The account names created during generation are: near.0, near.1, etc.

use clap::{App, Arg};
use std::path::PathBuf;
use testlib::alphanet_utils::generate_test_chain_spec;
use configs::chain_spec::save_chain_spec;

fn main() {
    let chain_spec_path_arg = &Arg::with_name("chain_spec_file")
        .short("c")
        .long("chain-spec-file")
        .value_name("CHAIN-SPEC-FILE")
        .help("Sets file location for chain spec")
        .default_value("node/configs/res/alphanet_chain.json")
        .required(true)
        .takes_value(true);
    let num_accounts_arg = &Arg::with_name("number_of_accounts")
        .short("n")
        .long("number-of-accounts")
        .value_name("NUMBER-OF-ACCOUNTS")
        .help("Sets the number of accounts to be generated in the chainspec.")
        .default_value("4")
        .required(true)
        .takes_value(true);
    let matches = App::new("keystore")
        .arg(chain_spec_path_arg)
        .arg(num_accounts_arg)
        .get_matches();


    let chain_spec_file = matches.value_of("chain_spec_file").map(PathBuf::from).unwrap();
    let num_accounts = matches.value_of("number_of_accounts").map(|x| x.parse::<u64>().unwrap()).unwrap();

    let mut acc_names = vec![];
    for i in 0..num_accounts {
        acc_names.push(format!("near.{}", i));
    }
    let chain_spec = generate_test_chain_spec(&acc_names, 1_000_000_000);
    save_chain_spec(&chain_spec_file, chain_spec);
}
