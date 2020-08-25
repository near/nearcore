use clap::{App, Arg};
use near_vm_logic::VMKind;
use neard::get_default_home;
use runtime_params_estimator::cases::run;
use runtime_params_estimator::testbed_runners::Config;
use runtime_params_estimator::testbed_runners::GasMetric;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    let default_home = get_default_home();
    let matches = App::new("Runtime parameters estimator")
        .arg(
            Arg::with_name("home")
                .long("home")
                .default_value(&default_home)
                .help("Directory for config and data (default \"~/.near\")")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("warmup-iters")
                .long("warmup-iters")
                .default_value("3")
                .required(true)
                .takes_value(true)
                .help("How many warm up iterations per block should we run."),
        )
        .arg(
            Arg::with_name("iters")
                .long("iters")
                .default_value("10")
                .required(true)
                .takes_value(true)
                .help("How many iterations per block are we going to try."),
        )
        .arg(
            Arg::with_name("accounts-num")
                .long("accounts-num")
                .default_value("10000")
                .required(true)
                .takes_value(true)
                .help("How many accounts were generated with `genesis-populate`."),
        )
        .arg(
            Arg::with_name("metric")
                .long("metric")
                .default_value("icount")
                .required(true)
                .takes_value(true)
                .help("What metric to use, possible values are icount or time."),
        )
        .arg(
            Arg::with_name("vm-kind")
                .long("vm-kind")
                .default_value("wasmer")
                .help("Which VM to test: wasmer or wasmtime"),
        )
        .arg(
            Arg::with_name("compile-only")
                .long("compile-only")
                .help("Only test contract compilation costs"),
        )
        .arg(
            Arg::with_name("disable-action-creation")
                .long("action-creation")
                .help("Disables action creation measurements"),
        )
        .arg(
            Arg::with_name("disable-transaction")
                .long("transaction")
                .help("Disables transaction measurements"),
        )
        .get_matches();

    let state_dump_path = matches.value_of("home").unwrap().to_string();
    let warmup_iters_per_block = matches.value_of("warmup-iters").unwrap().parse().unwrap();
    let iter_per_block = matches.value_of("iters").unwrap().parse().unwrap();
    let active_accounts = matches.value_of("accounts-num").unwrap().parse().unwrap();
    let metric = match matches.value_of("metric").unwrap() {
        "icount" => GasMetric::ICount,
        "time" => GasMetric::Time,
        other => panic!("Unknown metric {}", other),
    };
    let vm_kind = match matches.value_of("vm-kind") {
        Some("wasmer") => VMKind::Wasmer,
        Some("wasmtime") => VMKind::Wasmtime,
        _ => VMKind::Wasmer,
    };
    let disable_measure_action_creation = matches.is_present("action-creation");
    let disable_measure_transaction = matches.is_present("transaction");
    let runtime_config = run(
        Config {
            warmup_iters_per_block,
            iter_per_block,
            active_accounts,
            block_sizes: vec![],
            state_dump_path: state_dump_path.clone(),
            metric,
            vm_kind,
            disable_measure_action_creation,
            disable_measure_transaction,
        },
        matches.is_present("compile-only"),
    );

    println!("Generated RuntimeConfig:");
    println!("{:#?}", runtime_config);

    let str = serde_json::to_string_pretty(&runtime_config)
        .expect("Failed serializing the runtime config");
    let mut file = File::create(Path::new(&state_dump_path).join("runtime_config.json"))
        .expect("Failed to create file");
    if let Err(err) = file.write_all(str.as_bytes()) {
        panic!("Failed to write runtime config to file {}", err);
    }
}
