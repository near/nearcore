use client::chain_spec::ChainSpec;
use primitives::types::{AccountAlias, PublicKeyAlias};
use serde_json;

#[derive(Serialize, Deserialize)]
#[serde(remote = "ChainSpec")]
struct ChainSpecRef {
    accounts: Vec<(AccountAlias, PublicKeyAlias, u64)>,
    initial_authorities: Vec<PublicKeyAlias>,
    genesis_wasm: Vec<u8>,
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
        "accounts": [["alice", "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq", 2]],
        "initial_authorities": ["6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq"],
        "genesis_wasm": [0,1]
    });
    let spec = deserialize_chain_spec(&data.to_string()).unwrap();
    assert_eq!(spec.initial_authorities[0], "6fgp5mkRgsTWfd5UWw1VwHbNLLDYeLxrxw3jrkCeXNWq");
}
