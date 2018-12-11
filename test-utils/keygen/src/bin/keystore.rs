extern crate clap;
extern crate keystore;
extern crate primitives;
extern crate serde_json;

use clap::{App, Arg, ArgMatches, SubCommand};
use keystore::{get_key_file, write_key_file};
use primitives::hash::hash_struct;
use primitives::signature::sign;
use primitives::types::{SignedTransaction, TransactionBody};
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

    let public_key = matches.value_of("public_key");
    let key_file = get_key_file(&key_store_path, public_key);

    let data = matches.value_of("data").unwrap();
    let body: TransactionBody = serde_json::from_str(data).unwrap();
    let hash = hash_struct(&body);
    let sender_sig = sign(hash.as_ref(), &key_file.secret_key);
    let transaction = SignedTransaction {
        sender_sig,
        hash,
        body,
    };
    print!("{}", serde_json::to_string(&transaction).unwrap());
}

fn generate_key(matches: &ArgMatches) {
    let key_store_path = get_key_store_path(matches);
    write_key_file(&key_store_path);
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
            .arg(key_store_path_arg))
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
