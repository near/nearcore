use client::chain_spec::ChainSpec;
use serde_json;
use serde_json::Error;

#[derive(Serialize, Deserialize)]
#[serde(remote = "ChainSpec")]
struct ChainSpecRef {
    balances: Vec<(String, u128)>,
    initial_authorities: Vec<String>,
}

#[derive(Deserialize, Serialize)]
struct ChainSpecDeserializer(#[serde(with = "ChainSpecRef")] ChainSpec);

pub fn deserialize_chain_spec(config: &str) -> Result<ChainSpec, Error> {
    serde_json::from_str(config).map(|ChainSpecDeserializer(c)| c)
}

pub fn get_default_chain_spec() -> Result<ChainSpec, Error> {
    let data = include_bytes!("../res/default_chain.json");
    serde_json::from_slice(data).map(|ChainSpecDeserializer(c)| c)
}

#[test]
fn test_deserialize() {
    let data = json!({
        "balances": [["alice", 2]],
        "initial_authorities": ["john"]
    });
    let spec = deserialize_chain_spec(&data.to_string()).unwrap();
    assert_eq!(spec.initial_authorities[0], "john");
}
