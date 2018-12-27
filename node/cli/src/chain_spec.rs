use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde_json;

use node_runtime::chain_spec::ChainSpec;
use primitives::types::{AccountId, ReadablePublicKey};
use beacon::authority::{AuthorityConfig, AuthorityProposal};

#[derive(Serialize, Deserialize)]
#[serde(remote = "ChainSpec")]
struct ChainSpecRef {
    accounts: Vec<(AccountId, ReadablePublicKey, u64)>,
    initial_authorities: Vec<(AccountId, ReadablePublicKey, u64)>,
    genesis_wasm: Vec<u8>,
    beacon_chain_epoch_length: u64,
    beacon_chain_num_seats_per_slot: u64,
    boot_nodes: Vec<String>,
}

#[derive(Deserialize, Serialize)]
struct ChainSpecDeserializer(#[serde(with = "ChainSpecRef")] ChainSpec);

pub fn serialize_chain_spec(chain_spec: ChainSpec) -> String {
    serde_json::to_string(&ChainSpecDeserializer(chain_spec))
        .expect("Error serializing the chain spec.")
}

pub fn deserialize_chain_spec(config: &str) -> ChainSpec {
    serde_json::from_str(config)
        .map(|ChainSpecDeserializer(c)| c)
        .expect("Error deserializing the chain spec.")
}

pub fn get_default_chain_spec() -> ChainSpec {
    let data = include_bytes!("../res/default_chain.json");
    serde_json::from_slice(data)
        .map(|ChainSpecDeserializer(c)| c)
        .expect("Error deserializing the default chain spec.")
}

pub fn read_or_default_chain_spec(chain_spec_path: &Option<PathBuf>) -> ChainSpec {
    match chain_spec_path {
        Some(path) => {
            let mut file = File::open(path).expect("Could not open chain spec file.");
            let mut contents = String::new();
            file.read_to_string(&mut contents).expect("Could not read from chain spec file.");
            deserialize_chain_spec(&contents)
        }
        None => get_default_chain_spec(),
    }
}

pub fn get_authority_config(chain_spec: &ChainSpec) -> AuthorityConfig {
    let initial_authorities: Vec<AuthorityProposal> = chain_spec.initial_authorities
        .iter()
        .map(|(account_id, key, amount)| {
            AuthorityProposal {
                account_id: account_id.clone(),
                public_key: key.into(),
                amount: *amount,
            }
        })
        .collect();
    AuthorityConfig {
        initial_authorities,
        epoch_length: chain_spec.beacon_chain_epoch_length,
        num_seats_per_slot: chain_spec.beacon_chain_num_seats_per_slot,
    }
}

#[test]
fn test_deserialize() {
    let data = json!({
        "accounts": [["alice", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 100]],
        "initial_authorities": [("alice", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 50)],
        "genesis_wasm": [0,1],
        "beacon_chain_epoch_length": 10,
        "beacon_chain_num_seats_per_slot": 100,
        "boot_nodes": [],
    });
    let spec = deserialize_chain_spec(&data.to_string());
    assert_eq!(
        spec.initial_authorities[0],
        ("alice".to_string(), "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq".to_string(), 50)
    );
}
