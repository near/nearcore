#![doc = include_str!("../README.md")]

mod script;

use crate::script::Script;
use clap::Parser;
use near_vm_logic::ProtocolVersion;
use near_vm_logic::VMOutcome;
use near_vm_runner::internal::VMKind;
use near_vm_runner::VMResult;
use serde::{
    de::{MapAccess, Visitor},
    ser::SerializeMap,
    {Deserialize, Deserializer, Serialize, Serializer},
};
use std::path::PathBuf;
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

#[derive(Parser)]
struct CliArgs {
    /// Specifies the execution context in JSON format, see `VMContext`.
    #[clap(long)]
    context: Option<String>,
    /// Reads the context from the file.
    #[clap(long)]
    context_file: Option<PathBuf>,
    /// Overrides input field of the context with the given string.
    #[clap(long)]
    input: Option<String>,
    /// The name of the method to call on the smart contract.
    #[clap(long)]
    method_name: String,
    /// Key-value state in JSON base64 format for the smart contract as HashMap.
    #[clap(long)]
    state: Option<String>,
    /// Reads the state from the file
    #[clap(long)]
    state_file: Option<PathBuf>,
    /// If the contract should be called by a callback or several callbacks you
    /// can pass result of executing functions that trigger the callback. For
    /// non-callback calls it can be omitted.
    #[clap(long)]
    promise_results: Vec<String>,
    /// Specifies the economics and Wasm config in JSON format, see `Config`.
    #[clap(long)]
    config: Option<String>,
    /// Reads the config from the file.
    #[clap(long)]
    config_file: Option<PathBuf>,
    /// File path that contains the Wasm code to run.
    #[clap(long)]
    wasm_file: PathBuf,
    /// Select VM kind to run.
    #[clap(long, possible_values = &["wasmer", "wasmer2", "wasmtime"])]
    vm_kind: Option<String>,
    /// Prints execution times of various components.
    #[clap(long)]
    timings: bool,
    /// Protocol version.
    #[clap(long)]
    protocol_version: Option<ProtocolVersion>,
}

#[allow(unused)]
#[derive(Debug, Clone)]
struct StandaloneOutput {
    pub outcome: VMOutcome,
    pub err: Option<String>,
    pub state: State,
}

fn main() {
    rayon::ThreadPoolBuilder::new()
        .stack_size(8 * 1024 * 1024)
        .build_global()
        .expect("install rayon thread pool globally");

    let cli_args = CliArgs::parse();

    if cli_args.timings {
        tracing_span_tree::span_tree().enable();
    }

    let mut script = Script::default();

    match cli_args.vm_kind.as_deref() {
        Some("wasmtime") => script.vm_kind(VMKind::Wasmtime),
        Some("wasmer") => script.vm_kind(VMKind::Wasmer0),
        Some("wasmer2") => script.vm_kind(VMKind::Wasmer2),
        _ => (),
    };
    if let Some(config) = &cli_args.config {
        script.vm_config(serde_json::from_str(config).unwrap());
    }
    if let Some(path) = &cli_args.config_file {
        script.vm_config_from_file(path);
    }
    if let Some(version) = cli_args.protocol_version {
        script.protocol_version(version)
    }

    if let Some(state_str) = &cli_args.state {
        script.initial_state(serde_json::from_str(state_str).unwrap());
    }
    if let Some(path) = &cli_args.state_file {
        script.initial_state_from_file(path);
    }

    let code = fs::read(&cli_args.wasm_file).unwrap();
    let contract = script.contract(code);

    let step = script.step(contract, &cli_args.method_name);

    if let Some(value) = &cli_args.context {
        step.context(serde_json::from_str(value).unwrap());
    }
    if let Some(path) = &cli_args.context_file {
        step.context_from_file(path);
    }

    if let Some(value) = cli_args.input {
        step.input(value.as_bytes().to_vec());
    }

    let promise_results =
        cli_args.promise_results.iter().map(|it| serde_json::from_str(it).unwrap()).collect();
    step.promise_results(promise_results);

    let mut results = script.run();
    let last_result = results.outcomes.pop().unwrap();
    let maybe_error = last_result.error();

    match &last_result {
        VMResult::Aborted(outcome, _) | VMResult::Ok(outcome) => {
            println!(
                "{:#?}",
                StandaloneOutput {
                    outcome: outcome.clone(),
                    err: maybe_error.map(|it| it.to_string()),
                    state: State(results.state.fake_trie),
                }
            );

            println!("\nLogs:");
            for log in &outcome.logs {
                println!("{}\n", log);
            }
            println!("{:#?}", outcome.profile);
        }
    }
}
