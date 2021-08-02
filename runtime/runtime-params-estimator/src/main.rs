use clap::Clap;
use near_vm_runner::VMKind;
use nearcore::get_default_home;
use runtime_params_estimator::cases::run;
use runtime_params_estimator::testbed_runners::Config;
use runtime_params_estimator::testbed_runners::GasMetric;
use std::fs;
use std::path::PathBuf;

#[derive(Clap)]
struct CliArgs {
    /// Directory for config and data (default "~/.near\").
    #[clap(long)]
    home: Option<PathBuf>,
    /// How many warm up iterations per block should we run.
    #[clap(long, default_value = "0")]
    warmup_iters: usize,
    /// How many iterations per block are we going to try.
    #[clap(long, default_value = "10")]
    iters: usize,
    /// How many accounts were generated with `genesis-populate`.
    #[clap(long, default_value = "10000")]
    accounts_num: usize,
    /// What metric to use.
    #[clap(long, default_value = "icount", possible_values = &["icount", "time"])]
    metric: String,
    /// Which VM to test.
    #[clap(long, default_value = "wasmer", possible_values = &["wasmer", "wasmer1", "wasmtime"])]
    vm_kind: String,
    /// Only test contract compilation costs.
    #[clap(long)]
    compile_only: bool,
    /// Disables action creation measurements.
    #[clap(long("action-creation"))]
    disable_action_creation: bool,
    /// Disables transaction measurements.
    #[clap(long("transaction"))]
    disable_transactions: bool,
}

fn main() {
    let cli_args = CliArgs::parse();

    let state_dump_path = cli_args.home.unwrap_or_else(|| get_default_home().into());
    let warmup_iters_per_block = cli_args.warmup_iters;
    let iter_per_block = cli_args.iters;
    let active_accounts = cli_args.accounts_num;
    let metric = match cli_args.metric.as_str() {
        "icount" => GasMetric::ICount,
        "time" => GasMetric::Time,
        other => unreachable!("Unknown metric {}", other),
    };
    let vm_kind = match cli_args.vm_kind.as_str() {
        "wasmer" => VMKind::Wasmer0,
        "wasmer2" => VMKind::Wasmer2,
        "wasmtime" => VMKind::Wasmtime,
        other => unreachable!("Unknown vm_kind {}", other),
    };
    let disable_measure_action_creation = cli_args.disable_action_creation;
    let disable_measure_transaction = cli_args.disable_transactions;
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
        cli_args.compile_only,
    );

    println!("Generated RuntimeConfig:");
    println!("{:#?}", runtime_config);

    let str = serde_json::to_string_pretty(&runtime_config)
        .expect("Failed serializing the runtime config");
    fs::write(state_dump_path.join("runtime_config.json"), &str)
        .unwrap_or_else(|err| panic!("Failed to write runtime config to file {}", err))
}
