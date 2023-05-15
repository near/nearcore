use std::fs;
use std::path::PathBuf;

use clap::{Arg, Command};

use near_crypto::{InMemorySigner, KeyType, SecretKey, Signer};
use nearcore::get_default_home;

fn generate_key_to_file(account_id: &str, key: SecretKey, path: &PathBuf) -> std::io::Result<()> {
    let signer = InMemorySigner::from_secret_key(account_id.parse().unwrap(), key);
    signer.write_to_file(path.as_path())
}

fn main() {
    let matches = Command::new("Key-pairs generator")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .about("Generates: access key-pairs, validation key-pairs, network key-pairs")
        .arg(
            Arg::new("home")
                .long("home")
                .default_value(get_default_home().into_os_string())
                .value_parser(clap::value_parser!(PathBuf))
                .help("Directory for config and data (default \"~/.near\")")
                .action(clap::ArgAction::Set)
        )
        .arg(
            Arg::new("account-id")
                .long("account-id")
                .action(clap::ArgAction::Set)
        )
        .arg(
            Arg::new("generate-config")
                .long("generate-config")
                .help("Whether to generate a config file when generating keys. Requires account-id to be specified.")
                .action(clap::ArgAction::SetTrue),
        )
        .subcommand(
            Command::new("signer-keys").about("Generate signer keys.").arg(
                Arg::new("num-keys")
                    .long("num-keys")
                    .action(clap::ArgAction::Set)
                    .help("Number of signer keys to generate. (default 3)"),
            ),
        )
        .subcommand(
            Command::new("node-key").about("Generate key for the node communication."),
        )
        .subcommand(Command::new("validator-key").about("Generate staking key."))
        .get_matches();

    let home_dir = matches.get_one::<PathBuf>("home").unwrap();
    fs::create_dir_all(home_dir).expect("Failed to create directory");
    let account_id = matches.get_one::<String>("account-id");
    let generate_config = matches.get_flag("generate-config");

    match matches.subcommand() {
        Some(("signer-keys", args)) => {
            let num_keys = args
                .get_one::<String>("num-keys")
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
                    if let Err(e) = generate_key_to_file(account_id, key.clone(), &path) {
                        eprintln!("Error writing key to {}: {}", path.display(), e);
                        return;
                    }
                }

                pks.push(key.public_key());
            }
            let pks: Vec<_> = pks.into_iter().map(|pk| pk.to_string()).collect();
            println!("List of public keys:");
            println!("{}", pks.join(","));
        }
        Some(("validator-key", _)) => {
            let key = SecretKey::from_random(KeyType::ED25519);
            println!("PK: {}", key.public_key());
            if generate_config {
                let account_id =
                    account_id.expect("Account id must be specified if --generate-config is used");
                let mut path = home_dir.to_path_buf();
                path.push(nearcore::config::VALIDATOR_KEY_FILE);
                if let Err(e) = generate_key_to_file(account_id, key, &path) {
                    eprintln!("Error writing key to {}: {}", path.display(), e);
                    return;
                }
            }
        }
        Some(("node-key", _args)) => {
            let key = SecretKey::from_random(KeyType::ED25519);
            println!("PK: {}", key.public_key());
            if generate_config {
                let mut path = home_dir.to_path_buf();
                path.push(nearcore::config::NODE_KEY_FILE);
                if let Err(e) = generate_key_to_file("node", key, &path) {
                    eprintln!("Error writing key to {}: {}", path.display(), e);
                    return;
                }
            }
        }
        _ => unreachable!(),
    }
}
