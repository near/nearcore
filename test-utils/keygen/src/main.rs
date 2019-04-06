use clap::{App, Arg, ArgMatches, SubCommand};
use primitives::crypto::signature::sign;
use primitives::crypto::signer::get_key_file;
use std::path::PathBuf;
use std::process;
use primitives::crypto::signer::InMemorySigner;
use primitives::crypto::signer::write_block_producer_key_file;

fn get_key_store_path(matches: &ArgMatches) -> PathBuf {
    matches
        .value_of("key_store_path")
        .map(PathBuf::from)
        .unwrap()
}

fn sign_data(matches: &ArgMatches) {
    let key_store_path = get_key_store_path(matches);

    let public_key = matches.value_of("public_key").map(String::from);
    let key_file = get_key_file(&key_store_path, public_key);

    let data = matches.value_of("data").unwrap();
    let bytes = base64::decode(data).unwrap();
    let signature = sign(&bytes, &key_file.secret_key);
    let encoded = base64::encode(&signature);
    print!("{}", encoded);
}

fn generate_key(matches: &ArgMatches) {
    let key_store_path = get_key_store_path(matches);
    let signer = InMemorySigner::from_seed("not_used", matches.value_of("test_seed").unwrap());
    write_block_producer_key_file(&key_store_path.as_path(), signer.public_key, signer.secret_key, signer.bls_public_key, signer.bls_secret_key);
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
        .subcommand(SubCommand::with_name("sign")
            .arg(key_store_path_arg)
            .arg(Arg::with_name("data")
                .short("d")
                .long("data")
                .value_name("DATA")
                .help("base64 encoded bytes")
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
    } else if let Some(sub) = matches.subcommand_matches("sign") {
        sign_data(sub);
    } else if let Some(sub) = matches.subcommand_matches("get_public_key") {
        get_public_key(sub);
    } else {
        println!("Incorrect usage. See usage with: keystore --help");
        process::exit(1);
    }
}
