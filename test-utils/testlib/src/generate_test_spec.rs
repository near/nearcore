///! Generate a ChainSpec that can be used for running alphanet.
///! The account names created during generation are: near.0, near.1, etc.
use clap::{App, Arg};
use node_runtime::chain_spec::{AuthorityRotation, ChainSpec, DefaultIdType};
use std::path::PathBuf;

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
    let num_init_auth_arg = &Arg::with_name("number_of_init_authorities")
        .short("a")
        .long("number-of-init_authorities")
        .value_name("NUMBER-OF-INIT_AUTHORITIES")
        .help("Sets the number of initial authorities to be generated in the chainspec.")
        .default_value("4")
        .required(true)
        .takes_value(true);
    let matches = App::new("keystore")
        .arg(chain_spec_path_arg)
        .arg(num_accounts_arg)
        .arg(num_init_auth_arg)
        .get_matches();

    let chain_spec_file = matches.value_of("chain_spec_file").map(PathBuf::from).unwrap();
    let num_accounts =
        matches.value_of("number_of_accounts").map(|x| x.parse::<usize>().unwrap()).unwrap();
    let num_init_auth = matches
        .value_of("number_of_init_authorities")
        .map(|x| x.parse::<usize>().unwrap())
        .unwrap();
    let (chain_spec, _) = ChainSpec::testing_spec(
        DefaultIdType::Enumerated,
        num_accounts,
        num_init_auth,
        AuthorityRotation::ProofOfAuthority,
    );
    chain_spec.write_to_file(&chain_spec_file);
}
