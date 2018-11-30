extern crate clap;
extern crate primitives;
extern crate serde_json;

use clap::{App, Arg, ArgMatches, SubCommand};
use primitives::hash::hash_struct;
use primitives::signature::{SecretKey, sign};
use primitives::signature::get_keypair;
use primitives::types::TransactionBody;
use std::fs;
use std::path::Path;
use std::process;
use primitives::types::SignedTransaction;

fn generate_keys(matches: &ArgMatches) {
    let private_key_path = matches
        .value_of("output_file")
        .map(|x| Path::new(x))
        .unwrap();

    let public_key_path = private_key_path.with_extension("pub");

    if private_key_path.exists() {
        eprintln!("Private key file already exists: {:?}", &private_key_path);
        process::exit(1);
    } else if public_key_path.exists() {
        eprintln!("Public key file already exists: {:?}", &public_key_path);
        process::exit(1);
    }

    let (public_key, secret_key) = get_keypair();
    fs::write(public_key_path, public_key.to_string()).unwrap();
    fs::write(private_key_path, secret_key.to_string()).unwrap();
}

fn sign_data(matches: &ArgMatches) {
    let private_key_path = matches
        .value_of("private_key_file")
        .map(|x| Path::new(x))
        .unwrap();

    if !private_key_path.exists() {
        eprintln!("Private key file does not exist: {:?}", &private_key_path);
        process::exit(1);
    }
    let key_string = fs::read_to_string(private_key_path).unwrap();
    let private_key = SecretKey::from(&key_string);

    let data = matches.value_of("data").unwrap();
    let body: TransactionBody = serde_json::from_str(data).unwrap();
    let hash = hash_struct(&body);
    let sender_sig = sign(hash.as_ref(), &private_key);
    let transaction = SignedTransaction {
        sender_sig,
        hash,
        body,
    };
    println!("{}", serde_json::to_string(&transaction).unwrap());
}

fn main() {
    let matches = App::new("keystore")
        .subcommand(SubCommand::with_name("keygen")
            .arg(Arg::with_name("output_file")
                .short("o")
                .long("output-file")
                .value_name("OUTPUT_FILE")
                .help("Sets a file location for private key")
                .default_value("near_key")
                .required(true)
                .takes_value(true)
            )
        ).subcommand(
            SubCommand::with_name("sign_transaction")
                .arg(Arg::with_name("data")
                    .short("d")
                    .long("data")
                    .value_name("DATA")
                    .help("JSON encoded transaction body to sign")
                    .required(true)
                    .takes_value(true)
                )
                .arg(Arg::with_name("private_key_file")
                    .short("k")
                    .long("private-key-file")
                    .value_name("PRIVATE_KEY_FILE")
                    .help("Sets file location for private key to sign with")
                    .default_value("near_key")
                    .required(true)
                    .takes_value(true)
                )
        ).get_matches();

    if let Some(sub) = matches.subcommand_matches("keygen") {
        generate_keys(sub);
    } else if let Some(sub) = matches.subcommand_matches("sign_transaction") {
        sign_data(sub);
    } else {
        eprintln!("Incorrect usage. See usage with: keystore --help");
        process::exit(1);
    }
}
