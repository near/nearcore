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
use near_vm_logic::types::{ProfileData, PromiseResult, ProtocolVersion};
use near_vm_logic::{VMConfig, VMContext, VMKind, VMOutcome};
use near_vm_runner::{run_vm, run_vm_profiled, VMError};
use serde::de::{MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::{fmt, fs};
use wasmer_runtime::{compiler_for_backend, Backend};
use wasmer_runtime_core::cache::Artifact;
use wasmer_runtime_core::load_cache_with;
use std::fs::File;
use std::io::{Read, Write};
use std::time::Instant;

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

fn read_file(name: &str) -> std::io::Result<Vec<u8>> {
    let mut file = File::open(name)?;

    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    return Ok(data);
}

fn write_file(name: &str, data: &Vec<u8>) -> std::io::Result<()> {
    let mut file = File::create(name)?;
    file.write(data)?;

    return Ok(());
}

fn save_contract_to_cache(code: &Vec<u8>, file: &str) {
    println!("save cache to {}", file);
    let start = Instant::now();
    let module = wasmer_runtime::compile(code).unwrap();
    println!("compiled {} μs", start.elapsed().as_micros());
    let start = Instant::now();
    let artifact = match module.cache() {
        Ok(artifact) => artifact,
        Err(err) => panic!("Cannot compile: {:?}", err),
    };
    println!("cached {} ns", start.elapsed().as_micros());
    let start = Instant::now();
    let code = artifact.serialize().unwrap();
    println!("serialized {} μs", start.elapsed().as_micros());
    write_file(file, &code).unwrap();
}

fn restore_contract_from_cache(file: &str) {
    println!("restore cache from {}", file);
    let bytes = read_file(file).unwrap();
    let start = Instant::now();
    let artifact = Artifact::deserialize(&bytes).unwrap();
    println!("deserialized {} μs", start.elapsed().as_micros());
    unsafe {
        let compiler = compiler_for_backend(Backend::Singlepass).unwrap();
        let start = Instant::now();
        match load_cache_with(artifact, compiler.as_ref()) {
            Ok(_) => println!("loaded {} μs", start.elapsed().as_micros()),
            Err(err) => panic!("Load error: {:?}", err),
        }
    }
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
            Arg::with_name("save-cached-contract")
                .long("save-cached-contract")
                .value_name("SAVE_CONTRACT")
                .help("Save contract to file.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("restore-cached-contract")
                .long("restore-cached-contract")
                .value_name("RESTORE_CONTRACT")
                .help("Restore cached contract from file.")
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
        .arg(
            Arg::with_name("vm-kind")
                .long("vm-kind")
                .value_name("VM_KIND")
                .help("Select VM kind to run.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("profile-gas")
                .long("profile-gas")
                .help("Profiles gas consumption.")
        )
        .arg(
            Arg::with_name("protocol-version")
                .long("protocol-version")
                .help("Protocol version")
                .takes_value(true),
        )
        .get_matches();

    let vm_kind: VMKind = match matches.value_of("vm-kind") {
        Some(value) => match value {
            "wasmtime" => VMKind::Wasmtime,
            "wasmer" => VMKind::Wasmer,
            _ => VMKind::default(),
        },
        None => VMKind::default(),
    };

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
        .value_of("method-name");

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

    let protocol_version = matches
        .value_of("protocol-version")
        .map(|s| s.parse().unwrap())
        .unwrap_or(ProtocolVersion::MAX);

    match matches.value_of("restore-cached-contract") {
        Some(cache_file) => {
            restore_contract_from_cache(cache_file);
            return;
        }
        _ => {}
    };

    let code =
        fs::read(matches.value_of("wasm-file").expect("Wasm file needs to be specified")).unwrap();

    match matches.value_of("save-cached-contract") {
        Some(cache_file) => {
            save_contract_to_cache(&code, cache_file);
            return;
        }
        _ => {}
    };

    let method_name = method_name
        .expect("Name of the method must be specified")
        .as_bytes()
        .to_vec();

    let fees = RuntimeFeesConfig::default();
    let profile_data = ProfileData::new();
    let do_profile = matches.is_present("profile-gas");
    let (outcome, err) = if do_profile {
        run_vm_profiled(
            vec![],
            &code,
            &method_name,
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind,
            profile_data.clone(),
            protocol_version,
            None,
        )
    } else {
        run_vm(
            vec![],
            &code,
            &method_name,
            &mut fake_external,
            context,
            &config,
            &fees,
            &promise_results,
            vm_kind,
            protocol_version,
            None,
        )
    };
    let all_gas = match outcome.clone() {
        Some(outcome) => outcome.burnt_gas,
        _ => 1,
    };
    println!(
        "{}",
        serde_json::to_string(&StandaloneOutput {
            outcome,
            err,
            receipts: fake_external.get_receipt_create_calls().clone(),
            state: State(fake_external.fake_trie),
        })
        .unwrap()
    );

    if do_profile {
        assert_eq!(all_gas, profile_data.all_gas());
        println!("{:#?}", profile_data);
    }
}
