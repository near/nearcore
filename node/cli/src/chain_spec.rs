use primitives::types::{AccountAlias, PublicKeyAlias};
use serde_json;
use node_runtime::chain_spec::ChainSpec;

#[derive(Serialize, Deserialize)]
#[serde(remote = "ChainSpec")]
struct ChainSpecRef {
    accounts: Vec<(AccountAlias, PublicKeyAlias, u64)>,
    initial_authorities: Vec<(PublicKeyAlias, u64)>,
    genesis_wasm: Vec<u8>,
    beacon_chain_epoch_length: u64,
    beacon_chain_num_seats_per_slot: u64,
}

#[derive(Deserialize, Serialize)]
struct ChainSpecDeserializer(#[serde(with = "ChainSpecRef")] ChainSpec);

pub fn deserialize_chain_spec(config: &str) -> Result<ChainSpec, serde_json::Error> {
    serde_json::from_str(config).map(|ChainSpecDeserializer(c)| c)
}

pub fn get_default_chain_spec() -> Result<ChainSpec, serde_json::Error> {
    let data = include_bytes!("../res/default_chain.json");
    serde_json::from_slice(data).map(|ChainSpecDeserializer(c)| c)
}

#[test]
fn test_deserialize() {
    let data = json!({
        "accounts": [["alice", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 100]],
        "initial_authorities": [("6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 50)],
        "genesis_wasm": [0,1],
        "beacon_chain_epoch_length": 10,
        "beacon_chain_num_seats_per_slot": 100,
    });
    let spec = deserialize_chain_spec(&data.to_string()).unwrap();
    assert_eq!(spec.initial_authorities[0], ("6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq".to_string(), 50));
}
