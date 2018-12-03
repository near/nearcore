use std::path::Path;
use std::io::Read;
use std::fs::File;

use client::chain_spec::ChainSpec;
use primitives::types::AccountAlias;
use serde_json;

#[derive(Serialize, Deserialize)]
#[serde(remote = "ChainSpec")]
struct ChainSpecRef {
    balances: Vec<(AccountAlias, u64)>,
    initial_authorities: Vec<AccountAlias>,
    genesis_wasm: Vec<u8>,
}

#[derive(Deserialize, Serialize)]
struct ChainSpecDeserializer(#[serde(with = "ChainSpecRef")] ChainSpec);

pub fn deserialize_chain_spec(config: &str) -> ChainSpec {
    serde_json::from_str(config).map(|ChainSpecDeserializer(c)| c)
        .expect("Error deserializing the chain spec.")
}

pub fn get_default_chain_spec() -> ChainSpec {
    let data = include_bytes!("../res/default_chain.json");
    serde_json::from_slice(data).map(|ChainSpecDeserializer(c)| c)
        .expect("Error deserializing the default chain spec.")
}

pub fn read_or_default_chain_spec(chain_spec_path: &Option<&Path>) -> ChainSpec {
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
        "balances": [["alice", 2]],
        "initial_authorities": ["john"],
        "genesis_wasm": [0,1]
    });
    let spec = deserialize_chain_spec(&data.to_string());
    assert_eq!(spec.initial_authorities[0], "john");
}
