use std::fs;
use std::path::{Path, PathBuf};

use clap::{App, AppSettings, Arg, SubCommand};

use near_crypto::{InMemorySigner, KeyType, SecretKey, Signer};
use neard::get_default_home;

fn generate_key_to_file(account_id: &str, key: SecretKey, path: PathBuf) {
    let signer = InMemorySigner::from_secret_key(account_id.to_string(), key);
    signer.write_to_file(path.as_path());
}

fn main() {
    let default_home = get_default_home();
    let matches = App::new("Key-pairs generator")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .about("Generates: access key-pairs, validation key-pairs, network key-pairs")
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("account-id")
                .long("account-id")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("generate-config")
                .long("generate-config")
                .help("Whether to generate a config file when generating keys. Requires account-id to be specified.")
                .takes_value(false),
        )
        .subcommand(
            SubCommand::with_name("signer-keys").about("Generate signer keys.").arg(
                Arg::with_name("num-keys")
                    .long("num-keys")
                    .takes_value(true)
                    .help("Number of signer keys to generate. (default 3)"),
            ),
        )
        .subcommand(
            SubCommand::with_name("node-key").about("Generate key for the node communication."),
        )
        .subcommand(SubCommand::with_name("validator-key").about("Generate staking key."))
        .get_matches();

    let home_dir = matches.value_of("home").map(|dir| Path::new(dir)).unwrap();
    fs::create_dir_all(home_dir).expect("Failed to create directory");
    let account_id = matches.value_of("account-id");
    let generate_config = matches.is_present("generate-config");

    match matches.subcommand() {
        ("signer-keys", Some(args)) => {
            let num_keys = args
                .value_of("num-keys")
                .map(|x| x.parse().expect("Failed to parse number keys."))
                .unwrap_or(3usize);
            let keys: Vec<SecretKey> =
                (0..num_keys).map(|_| SecretKey::from_random(KeyType::ED25519)).collect();
            let mut pks = vec![];
            for (i, key) in keys.into_iter().enumerate() {
                println!("Key#{}", i);
                println!("PK: {}", key.public_key());
                println!();
                if generate_config {
                    let account_id = account_id
                        .expect("Account id must be specified if --generate-config is used");
                    let key_file_name = format!("signer{}_key.json", i);
                    let mut path = home_dir.to_path_buf();
                    path.push(&key_file_name);
                    generate_key_to_file(account_id, key.clone(), path);
                }

                pks.push(key.public_key());
            }
            let pks: Vec<_> = pks.into_iter().map(|pk| format!("{}", pk)).collect();
            println!("List of public keys:");
            println!("{}", pks.join(","));
        }
        ("validator-key", Some(_)) => {
            let key = SecretKey::from_random(KeyType::ED25519);
            println!("PK: {}", key.public_key());
            if generate_config {
                let account_id =
                    account_id.expect("Account id must be specified if --generate-config is used");
                let mut path = home_dir.to_path_buf();
                path.push(neard::config::VALIDATOR_KEY_FILE);
                generate_key_to_file(account_id, key, path);
            }
        }
        ("node-key", Some(_args)) => {
            let key = SecretKey::from_random(KeyType::ED25519);
            println!("PK: {}", key.public_key());
            if generate_config {
                let mut path = home_dir.to_path_buf();
                path.push(neard::config::NODE_KEY_FILE);
                generate_key_to_file("", key, path);
            }
        }
        (_, _) => unreachable!(),
    }
}
