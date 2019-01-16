extern crate keystore;
extern crate clap;
extern crate primitives;
extern crate serde_json;

use clap::{App, Arg, ArgMatches, SubCommand};
use primitives::hash::hash_struct;
use primitives::signature::{get_key_pair, sign};
use primitives::signer::{get_key_file, write_key_file};
use transaction::{SignedTransaction, TransactionBody};
use primitives::test_utils::get_key_pair_from_seed;
use std::path::PathBuf;
use std::process;

fn get_key_store_path(matches: &ArgMatches) -> PathBuf {
    matches
        .value_of("key_store_path")
        .map(PathBuf::from)
        .unwrap()
}

fn sign_transaction(matches: &ArgMatches) {
    let key_store_path = get_key_store_path(matches);

    let public_key = matches.value_of("public_key").map(String::from);
    let key_file = get_key_file(&key_store_path, public_key);

    let data = matches.value_of("data").unwrap();
    let body: TransactionBody = serde_json::from_str(data).unwrap();
    let hash = hash_struct(&body);
    let sender_sig = sign(hash.as_ref(), &key_file.secret_key);
    let transaction = SignedTransaction::new(
        sender_sig,
        body,
    );
    print!("{}", serde_json::to_string(&transaction).unwrap());
}

fn generate_key(matches: &ArgMatches) {
    let key_store_path = get_key_store_path(matches);
    let (public_key, secret_key) = match matches.value_of("test_seed") {
        Some(seed_string) => get_key_pair_from_seed(&seed_string),
        None => get_key_pair()
    };
    write_key_file(&key_store_path, public_key, secret_key);
}

fn get_public_key(matches: &ArgMatches) {
    let key_store_path = get_key_store_path(matches);
    let public_key = None;
    let key_file = get_key_file(&key_store_path, public_key);
    print!("{}", key_file.public_key);
}

fn main() {
    let key_store_path_arg = &Arg::with_name("key_store_path")
        .short("p")
        .long("keystore-path")
        .value_name("KEY_STORE_PATH")
        .help("Sets a directory location for key store")
        .default_value("keystore")
        .required(true)
        .takes_value(true);
    let matches = App::new("keystore")
        .subcommand(SubCommand::with_name("keygen")
            .arg(key_store_path_arg)
            .arg(Arg::with_name("test_seed")
                .long("test-seed")
                .value_name("TEST_SEED")
                .help(
                    "Specify a seed for generating a key pair.\
                     This should only be used for deterministically \
                     creating key pairs during tests.",
                )
                .takes_value(true),
            ))
        .subcommand(SubCommand::with_name("get_public_key")
            .arg(key_store_path_arg))
        .subcommand(SubCommand::with_name("sign_transaction")
            .arg(key_store_path_arg)
            .arg(Arg::with_name("data")
                .short("d")
                .long("data")
                .value_name("DATA")
                .help("JSON encoded transaction body to sign")
                .required(true)
                .takes_value(true)
            )
            .arg(Arg::with_name("public_key")
                .short("k")
                .long("public-key")
                .value_name("PUBLIC_KEY")
                .help("Sets public key to sign with, \
                    can be omitted with 1 file in keystore")
                .takes_value(true)
            )
        ).get_matches();

    if let Some(sub) = matches.subcommand_matches("keygen") {
        generate_key(sub);
    } else if let Some(sub) = matches.subcommand_matches("sign_transaction") {
        sign_transaction(sub);
    } else if let Some(sub) = matches.subcommand_matches("get_public_key") {
        get_public_key(sub);
    } else {
        println!("Incorrect usage. See usage with: keystore --help");
        process::exit(1);
    }
}
