//! See package description.
//! Usage example:
//! ```
//! cargo run --package near-vm-runner-standalone --bin near-vm-runner-standalone \
//! -- --context-file=/tmp/context.json --config-file=/tmp/config.json --method-name=hello \
//! --wasm-file=/tmp/main.wasm
//! ```
use clap::{App, Arg};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{VMConfig, VMContext};
use near_vm_runner::run;
use std::fs;

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
            None => panic!("Context should be specified."),
        },
    };

    if let Some(value_str) = matches.value_of("input") {
        context.input = value_str.as_bytes().to_vec();
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
            None => panic!("Config should be specified."),
        },
    };

    let code =
        fs::read(matches.value_of("wasm-file").expect("Wasm file needs to be specified")).unwrap();

    let mut fake_external = MockedExternal::new();
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

    if let Some(outcome) = outcome {
        let str = serde_json::to_string(&outcome).unwrap();
        println!("{}", str);
        for call in fake_external.get_receipt_create_calls() {
            let str = serde_json::to_string(&call).unwrap();
            println!("{}", str);
        }
    }

    if let Some(err) = err {
        println!("{:?}", err);
    }
}
