use clap::{App, Arg};
use nearmint::NearMint;
use node_runtime::chain_spec::ChainSpec;
use node_runtime::ext::ACCOUNT_DATA_SEPARATOR;
use node_runtime::tx_stakes::TxTotalStake;
use primitives::account::{AccessKey, Account};
use primitives::crypto::signature::PublicKey;
use primitives::hash::hash;
use primitives::serialize::Decode;
use primitives::transaction::Callback;
use primitives::utils::col;
use std::convert::TryFrom;
use std::path::PathBuf;
use storage::trie::TrieIterator;
use storage::DBValue;

const DEFAULT_BASE_PATH: &str = "";

fn to_printable(blob: &[u8]) -> String {
    if blob.len() > 60 {
        format!("{} bytes, hash: {}", blob.len(), hash(blob))
    } else {
        let ugly = blob.iter().find(|&&x| x < b' ').is_some();
        if ugly {
            return format!("0x{}", hex::encode(blob));
        }
        match String::from_utf8(blob.to_vec()) {
            Ok(v) => format!(" {}", v),
            Err(_e) => format!("0x{}", hex::encode(blob)),
        }
    }
}

fn print_state_entry(key: Vec<u8>, value: DBValue) {
    let column = &key[0..1];
    match column {
        col::ACCOUNT => {
            let separator = (1..key.len()).find(|&x| key[x] == ACCOUNT_DATA_SEPARATOR[0]);
            if let Some(separator) = separator {
                let account_name = to_printable(&key[1..separator]);
                let contract_key = to_printable(&key[(separator + 1)..]);
                println!(
                    "Storage {:?},{:?}: {:?}",
                    account_name,
                    contract_key,
                    to_printable(&value)
                );
            } else {
                let account: Account = Decode::decode(&value).unwrap();
                let account_name = to_printable(&key[1..]);
                println!("Account {:?}: {:?}", account_name, account);
            }
        }
        col::CALLBACK => {
            let _callback: Callback = Decode::decode(&value).unwrap();
            let callback_id = to_printable(&key[1..]);
            println!("Callback {}: {}", callback_id, to_printable(&value));
        }
        col::CODE => {
            let account_name = to_printable(&key[1..]);
            println!("Code for {:?}: {}", account_name, to_printable(&value));
        }
        col::TX_STAKE => {
            let separator = (1..key.len()).find(|&x| key[x] == col::TX_STAKE_SEPARATOR[0]);
            let stake: TxTotalStake = Decode::decode(&value).unwrap();
            if let Some(separator) = separator {
                let account_name = to_printable(&key[1..separator]);
                let contract_id = to_printable(&key[(separator + 1)..]);
                println!("Tx_stake {:?},{:?}: {:?}", account_name, contract_id, stake);
            } else {
                let account_name = to_printable(&key[1..]);
                println!("Tx_stake {:?}: {:?}", account_name, stake);
            }
        }
        col::ACCESS_KEY => {
            let separator = (1..key.len()).find(|&x| key[x] == col::ACCESS_KEY[0]).unwrap();
            let access_key: AccessKey = Decode::decode(&value).unwrap();
            let account_name = to_printable(&key[1..separator]);
            let public_key = PublicKey::try_from(&key[(separator + 1)..]).unwrap();
            println!("Access key {:?},{:?}: {:?}", account_name, public_key, access_key);
        }
        _ => {
            println!(
                "Unknown column {}, {:?}: {:?}",
                column[0],
                to_printable(&key[1..]),
                to_printable(&value)
            );
        }
    }
}

fn main() {
    let matches = App::new("state-viewer")
        .args(&[
            Arg::with_name("base_path")
                .short("d")
                .long("base-path")
                .value_name("PATH")
                .help("Specify a base path for persisted files.")
                .default_value(DEFAULT_BASE_PATH)
                .takes_value(true),
            Arg::with_name("chain_spec_file")
                .short("c")
                .long("chain-spec-file")
                .value_name("CHAIN_SPEC")
                .help("Specify a file location to read a custom chain spec.")
                .takes_value(true),
            Arg::with_name("devnet")
                .long("devnet")
                .help("Run with DevNet validator configuration (single alice.near validator)")
                .takes_value(false),
        ])
        .get_matches();
    let base_path = matches.value_of("base_path").map(PathBuf::from).unwrap();
    let chain_spec = if matches.is_present("devnet") {
        ChainSpec::default_devnet()
    } else {
        let chain_spec_path = matches.value_of("chain_spec_file").map(PathBuf::from);
        ChainSpec::from_file_or_default(&chain_spec_path, ChainSpec::default_poa())
    };

    let nearmint = NearMint::new(&base_path, chain_spec);
    let trie = TrieIterator::new(&nearmint.trie, &nearmint.root).unwrap();
    println!("Storage root is {}, block height is {}", nearmint.root, nearmint.height);
    for item in trie {
        let (key, value) = item.unwrap();
        print_state_entry(key, value);
    }
}
