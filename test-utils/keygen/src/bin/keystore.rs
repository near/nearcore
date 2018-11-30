extern crate clap;
extern crate keystore;
extern crate primitives;
extern crate serde_json;

use clap::{App, Arg, ArgMatches, SubCommand};
use keystore::{get_secret_key, write_key_file};
use primitives::hash::hash_struct;
use primitives::signature::sign;
use primitives::types::SignedTransaction;
use primitives::types::TransactionBody;
use std::path::Path;
use std::process;

fn sign_transaction(matches: &ArgMatches) {
    let key_store_path = matches
        .value_of("key_store_path")
        .map(|x| Path::new(x))
        .unwrap();

    let public_key = matches.value_of("public_key");
    let secret_key = get_secret_key(key_store_path, public_key);

    let data = matches.value_of("data").unwrap();
    let body: TransactionBody = serde_json::from_str(data).unwrap();
    let hash = hash_struct(&body);
    let sender_sig = sign(hash.as_ref(), &secret_key);
    let transaction = SignedTransaction {
        sender_sig,
        hash,
        body,
    };
    println!("{}", serde_json::to_string(&transaction).unwrap());
}

fn generate_key(matches: &ArgMatches) {
    let key_store_path = matches
        .value_of("key_store_path")
        .map(|x| Path::new(x))
        .unwrap();
    write_key_file(key_store_path);
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
            ))
        .get_matches();

    if let Some(sub) = matches.subcommand_matches("keygen") {
        generate_key(sub);
    } else if let Some(sub) = matches.subcommand_matches("sign_transaction") {
        sign_transaction(sub);
    } else {
        eprintln!("Incorrect usage. See usage with: keystore --help");
        process::exit(1);
    }
}
