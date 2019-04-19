#[macro_use]
extern crate serde_derive;

use std::fs;
use std::path::{Path, PathBuf};
use std::process;

use clap::{App, Arg, ArgMatches, SubCommand};

use primitives::crypto::signature::{sign, PublicKey, SecretKey};
use primitives::crypto::signer::{get_key_file, write_block_producer_key_file, InMemorySigner};
use primitives::hash::hash;
use primitives::traits::ToBytes;

#[derive(Serialize)]
struct TypeValue {
    #[serde(rename = "type")]
    type_field: String,
    value: String,
}

#[derive(Serialize)]
struct TendermintKeyFile {
    address: String,
    pub_key: TypeValue,
    priv_key: TypeValue,
}

fn write_tendermint_key_file(key_store_path: &Path, public_key: PublicKey, secret_key: SecretKey) {
    if !key_store_path.exists() {
        fs::create_dir_all(key_store_path).unwrap();
    }

    let address_bytes = hash(&public_key.to_bytes()).as_ref()[..20].to_vec();
    let address = hex::encode(&address_bytes);
    let key_file = TendermintKeyFile {
        address,
        pub_key: TypeValue {
            type_field: "tendermint/PubKeyEd25519".to_string(),
            value: public_key.to_base64(),
        },
        priv_key: TypeValue {
            type_field: "tendermint/PrivKeyEd25519".to_string(),
            value: secret_key.to_base64(),
        },
    };
    let key_file_path = key_store_path.join(Path::new("priv_validator_key.json"));
    let serialized = serde_json::to_string(&key_file).unwrap();
    fs::write(key_file_path, serialized).unwrap();
}

fn get_key_store_path(matches: &ArgMatches) -> PathBuf {
    matches.value_of("key_store_path").map(PathBuf::from).unwrap()
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
    write_block_producer_key_file(
        &key_store_path.as_path(),
        signer.public_key,
        signer.secret_key,
        signer.bls_public_key,
        signer.bls_secret_key,
    );
}

fn generate_tendermint_key(matches: &ArgMatches) {
    let key_store_path = get_key_store_path(matches);
    let signer = InMemorySigner::from_seed("not_used", matches.value_of("test_seed").unwrap());
    write_tendermint_key_file(&key_store_path.as_path(), signer.public_key, signer.secret_key);
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
        .subcommand(
            SubCommand::with_name("keygen")
                .arg(key_store_path_arg)
                .arg(
                    Arg::with_name("test_seed")
                        .long("test-seed")
                        .value_name("TEST_SEED")
                        .help(
                            "Specify a seed for generating a key pair.\
                             This should only be used for deterministically \
                             creating key pairs during tests.",
                        )
                        .takes_value(true),
                )
                .arg(Arg::with_name("tendermint").long("tendermint").takes_value(false)),
        )
        .subcommand(SubCommand::with_name("get_public_key").arg(key_store_path_arg))
        .subcommand(
            SubCommand::with_name("sign")
                .arg(key_store_path_arg)
                .arg(
                    Arg::with_name("data")
                        .short("d")
                        .long("data")
                        .value_name("DATA")
                        .help("base64 encoded bytes")
                        .required(true)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("public_key")
                        .short("k")
                        .long("public-key")
                        .value_name("PUBLIC_KEY")
                        .help(
                            "Sets public key to sign with, \
                             can be omitted with 1 file in keystore",
                        )
                        .takes_value(true),
                ),
        )
        .get_matches();

    if let Some(sub) = matches.subcommand_matches("keygen") {
        if sub.is_present("tendermint") {
            generate_tendermint_key(sub);
        } else {
            generate_key(sub);
        }
    } else if let Some(sub) = matches.subcommand_matches("sign") {
        sign_data(sub);
    } else if let Some(sub) = matches.subcommand_matches("get_public_key") {
        get_public_key(sub);
    } else {
        println!("Incorrect usage. See usage with: keystore --help");
        process::exit(1);
    }
}
