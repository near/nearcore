use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use serde_json;

use primitives::types::{AccountId, Balance, ReadablePublicKey};
use primitives::network::PeerInfo;

/// Specification of the blockchain in general.
pub struct ChainSpec {
    /// Genesis state accounts: (AccountId, PK, Initial Balance, Initial TX Stake)
    pub accounts: Vec<(AccountId, ReadablePublicKey, Balance, Balance)>,

    /// Genesis smart contract code.
    pub genesis_wasm: Vec<u8>,

    /// Genesis state authorities that bootstrap the chain.
    pub initial_authorities: Vec<(AccountId, ReadablePublicKey, Balance)>,

    pub beacon_chain_epoch_length: u64,
    pub beacon_chain_num_seats_per_slot: u64,

    pub boot_nodes: Vec<PeerInfo>,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "ChainSpec")]
struct ChainSpecRef {
    accounts: Vec<(AccountId, ReadablePublicKey, u64, u64)>,
    initial_authorities: Vec<(AccountId, ReadablePublicKey, u64)>,
    genesis_wasm: Vec<u8>,
    beacon_chain_epoch_length: u64,
    beacon_chain_num_seats_per_slot: u64,
    boot_nodes: Vec<PeerInfo>,
}

#[derive(Deserialize, Serialize)]
struct ChainSpecDeserializer(#[serde(with = "ChainSpecRef")] ChainSpec);

pub fn serialize_chain_spec(chain_spec: ChainSpec) -> String {
    serde_json::to_string(&ChainSpecDeserializer(chain_spec))
        .expect("Error serializing the chain spec.")
}

fn deserialize_chain_spec(config: &str) -> ChainSpec {
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

#[test]
fn test_deserialize() {
    let data = json!({
        "accounts": [["alice.near", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 100, 10]],
        "initial_authorities": [("alice.near", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 50)],
        "genesis_wasm": [0,1],
        "beacon_chain_epoch_length": 10,
        "beacon_chain_num_seats_per_slot": 100,
        "boot_nodes": [],
    });
    let spec = deserialize_chain_spec(&data.to_string());
    assert_eq!(
        spec.initial_authorities[0],
        ("alice.near".to_string(), "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq".to_string(), 50)
    );
}
