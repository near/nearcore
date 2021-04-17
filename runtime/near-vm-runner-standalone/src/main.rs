//! See package description.
//! Usage example:
//! ```
//! cargo run --package near-vm-runner-standalone --bin near-vm-runner-standalone \
//! -- --method-name=hello --wasm-file=/tmp/main.wasm
//! ```
//! Optional `--context-file=/tmp/context.json --config-file=/tmp/config.json` could be added
//! to provide custom context and VM config.
mod script;
mod tracing_timings;

use crate::script::Script;
use clap::{App, Arg};
use near_vm_logic::mocks::mock_external::Receipt;
use near_vm_logic::VMOutcome;
use near_vm_runner::{VMError, VMKind};
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeMap,
    {Deserialize, Deserializer, Serialize, Serializer},
};
use std::path::Path;
use std::{collections::HashMap, fmt, fs};

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
                .takes_value(true)
                .required(true),
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
            Arg::with_name("state-file")
                .long("state-file")
                .value_name("STATE_FILE")
                .help("Reads the state from the file")
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
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("vm-kind")
                .long("vm-kind")
                .value_name("VM_KIND")
                .help("Select VM kind to run.")
                .takes_value(true)
                .possible_values(&["wasmer", "wasmer1", "wasmtime"]),
        )
        .arg(
            Arg::with_name("profile-gas")
                .long("profile-gas")
                .help("Profiles gas consumption.")
        )
        .arg(
            Arg::with_name("timings")
                .long("timings")
                .help("Prints execution times of various components.")
        )
        .arg(
            Arg::with_name("protocol-version")
                .long("protocol-version")
                .help("Protocol version")
                .takes_value(true),
        )
        .get_matches();

    if matches.is_present("timings") {
        tracing_timings::enable();
    }

    let mut script = Script::default();

    match matches.value_of("vm-kind") {
        Some("wasmtime") => script.vm_kind(VMKind::Wasmtime),
        Some("wasmer") => script.vm_kind(VMKind::Wasmer0),
        Some("wasmer1") => script.vm_kind(VMKind::Wasmer1),
        _ => (),
    };
    if let Some(config) = matches.value_of("config") {
        script.vm_config(serde_json::from_str(config).unwrap());
    }
    if let Some(path) = matches.value_of("config-file") {
        script.vm_config_from_file(Path::new(path));
    }
    if let Some(version) = matches.value_of("protocol-version") {
        script.protocol_version(version.parse().unwrap())
    }
    let profile_gas = matches.is_present("profile-gas");
    script.profile(profile_gas);

    if let Some(state_str) = matches.value_of("state") {
        script.initial_state(serde_json::from_str(state_str).unwrap());
    }
    if let Some(path) = matches.value_of("state-file") {
        script.initial_state_from_file(Path::new(path));
    }

    let code = fs::read(matches.value_of("wasm-file").unwrap()).unwrap();
    let contract = script.contract(code);

    let method = matches.value_of("method-name").unwrap();
    let step = script.step(contract, method);

    if let Some(value) = matches.value_of("context") {
        step.context(serde_json::from_str(value).unwrap());
    }
    if let Some(path) = matches.value_of("context-file") {
        step.context_from_file(Path::new(path));
    }

    if let Some(value) = matches.value_of("input") {
        step.input(value.as_bytes().to_vec());
    }

    if let Some(values) = matches.values_of("promise-results") {
        let promise_results =
            values.map(serde_json::from_str).collect::<Result<Vec<_>, _>>().unwrap();
        step.promise_results(promise_results);
    }

    let mut results = script.run();
    let (outcome, err) = results.outcomes.pop().unwrap();

    let all_gas = match &outcome {
        Some(outcome) => outcome.burnt_gas,
        _ => 1,
    };

    println!(
        "{}",
        serde_json::to_string(&StandaloneOutput {
            outcome,
            err,
            receipts: results.state.get_receipt_create_calls().clone(),
            state: State(results.state.fake_trie),
        })
        .unwrap()
    );

    if profile_gas {
        assert_eq!(all_gas, results.profile.all_gas());
        println!("{:#?}", results.profile);
    }
}
