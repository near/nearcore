//! See package description.
//! Usage example:
//! ```
//! cargo run --package near-vm-runner-standalone --bin near-vm-runner-standalone \
//! -- --method-name=hello --wasm-file=/tmp/main.wasm
//! ```
//! Optional `--context-file=/tmp/context.json --config-file=/tmp/config.json` could be added
//! to provide custom context and VM config.
use clap::{App, Arg};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::mocks::mock_external::{MockedExternal, Receipt};
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{VMConfig, VMContext, VMOutcome};
use near_vm_runner::{run, VMError};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::{fmt, fs};

#[derive(Debug, Clone)]
struct State(HashMap<Vec<u8>, Vec<u8>>);

impl Serialize for State {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(&base64::encode(&k).to_string(), &base64::encode(&v).to_string())?;
        }
        map.end()
    }
}

struct Base64HashMapVisitor;

impl<'de> Visitor<'de> for Base64HashMapVisitor {
    type Value = State;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("Base64 serialized HashMap")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = HashMap::with_capacity(access.size_hint().unwrap_or(0));

        while let Some((key, value)) = access.next_entry::<String, String>()? {
            map.insert(base64::decode(&key).unwrap(), base64::decode(&value).unwrap());
        }

        Ok(State(map))
    }
}

impl<'de> Deserialize<'de> for State {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(Base64HashMapVisitor {})
    }
}

#[derive(Debug, Clone, Serialize)]
struct StandaloneOutput {
    pub outcome: Option<VMOutcome>,
    pub err: Option<VMError>,
    pub receipts: Vec<Receipt>,
    pub state: State,
}

fn default_vm_context() -> VMContext {
    return VMContext {
        current_account_id: "alice".to_string(),
        signer_account_id: "bob".to_string(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol".to_string(),
        input: vec![],
        block_index: 1,
        block_timestamp: 1586796191203000000,
        account_balance: 10u128.pow(25),
        account_locked_balance: 0,
        storage_usage: 100,
        attached_deposit: 0,
        prepaid_gas: 10u64.pow(18),
        random_seed: vec![0, 1, 2],
        is_view: false,
        output_data_receivers: vec![],
        epoch_height: 1,
    };
}

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(
            Arg::with_name("context")
                .long("context")
                .value_name("CONTEXT")
                .help("Specifies the execution context in JSON format, see `VMContext`.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("context-file")
                .long("context-file")
                .value_name("CONTEXT_FILE")
                .help("Reads the context from the file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("input")
                .long("input")
                .value_name("INPUT")
                .help("Overrides input field of the context with the given string.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("method-name")
                .long("method-name")
                .value_name("METHOD_NAME")
                .help("The name of the method to call on the smart contract.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("state")
                .long("state")
                .value_name("STATE")
                .help("Key-value state in JSON base64 format for the smart contract \
                as HashMap.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("promise-results")
                .long("promise-results")
                .value_name("PROMISE-RESULTS")
                .help("If the contract should be called by a callback or several callbacks you can pass \
                result of executing functions that trigger the callback. For non-callback calls it can be omitted.")
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("CONFIG")
                .help("Specifies the economics and Wasm config in JSON format, see `Config`.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("config-file")
                .long("config-file")
                .value_name("CONFIG_FILE")
                .help("Reads the config from the file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("wasm-file")
                .long("wasm-file")
                .value_name("WASM_FILE")
                .help("File path that contains the Wasm code to run.")
                .takes_value(true),
        )
        .get_matches();

    let mut context: VMContext = match matches.value_of("context") {
        Some(value) => serde_json::from_str(value).unwrap(),
        None => match matches.value_of("context-file") {
            Some(filepath) => {
                let data = fs::read(&filepath).unwrap();
                serde_json::from_slice(&data).unwrap()
            }
            None => default_vm_context(),
        },
    };

    if let Some(value_str) = matches.value_of("input") {
        context.input = value_str.as_bytes().to_vec();
    }

    let mut fake_external = MockedExternal::new();

    if let Some(state_str) = matches.value_of("state") {
        let state: State = serde_json::from_str(state_str).unwrap();
        fake_external.fake_trie = state.0;
    }

    let method_name = matches
        .value_of("method-name")
        .expect("Name of the method must be specified")
        .as_bytes()
        .to_vec();

    let promise_results: Vec<PromiseResult> = matches
        .values_of("promise-results")
        .unwrap_or_default()
        .map(|res_str| serde_json::from_str(res_str).unwrap())
        .collect();

    let config: VMConfig = match matches.value_of("config") {
        Some(value) => serde_json::from_str(value).unwrap(),
        None => match matches.value_of("config-file") {
            Some(filepath) => {
                let data = fs::read(&filepath).unwrap();
                serde_json::from_slice(&data).unwrap()
            }
            None => VMConfig::default(),
        },
    };

    let code =
        fs::read(matches.value_of("wasm-file").expect("Wasm file needs to be specified")).unwrap();

    let fees = RuntimeFeesConfig::default();
    let (outcome, err) = run(
        vec![],
        &code,
        &method_name,
        &mut fake_external,
        context,
        &config,
        &fees,
        &promise_results,
    );

    println!(
        "{}",
        serde_json::to_string(&StandaloneOutput {
            outcome,
            err,
            receipts: fake_external.get_receipt_create_calls().clone(),
            state: State(fake_external.fake_trie),
        })
        .unwrap()
    )
}
