#![doc = include_str!("../README.md")]

use anyhow::Context;
use clap::Clap;
use genesis_populate::GenesisBuilder;
use near_chain_configs::GenesisValidationMode;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::create_store;
use near_vm_runner::internal::VMKind;
use nearcore::{get_store_path, load_config};
use runtime_params_estimator::config::{Config, GasMetric};
use runtime_params_estimator::CostTable;
use runtime_params_estimator::{costs_to_runtime_config, QemuCommandBuilder};
use runtime_params_estimator::{read_resource, RocksDBTestConfig};
use std::env;
use std::fmt::Write;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time;

#[derive(Clap)]
struct CliArgs {
    /// Directory for config and data. If not set, a temporary directory is used
    /// to generate appropriate data.
    #[clap(long)]
    home: Option<PathBuf>,
    /// How many warm up iterations per block should we run.
    #[clap(long, default_value = "0")]
    warmup_iters: usize,
    /// How many iterations per block are we going to try.
    #[clap(long, default_value = "10")]
    iters: usize,
    /// Number of active accounts in the state (accounts used for estimation).
    #[clap(long, default_value = "20000")]
    accounts_num: usize,
    /// Number of additional accounts to add to the state, among which active accounts are selected.
    #[clap(long, default_value = "200000")]
    additional_accounts_num: u64,
    /// Skip building test contract which is used in metrics computation.
    #[clap(long)]
    skip_build_test_contract: bool,
    /// What metric to use.
    #[clap(long, default_value = "icount", possible_values = &["icount", "time"])]
    metric: String,
    /// Which VM to test.
    #[clap(long, possible_values = &["wasmer", "wasmer2", "wasmtime"])]
    vm_kind: Option<String>,
    /// Render existing `costs.txt` as `RuntimeConfig`.
    #[clap(long)]
    costs_file: Option<PathBuf>,
    /// Compare baseline `costs-file` with a different costs file.
    #[clap(long, requires("costs-file"))]
    compare_to: Option<PathBuf>,
    /// Only measure the specified metrics, computing a subset of costs.
    #[clap(long)]
    costs: Option<String>,
    /// Build and run the estimator inside a docker container via QEMU.
    #[clap(long)]
    docker: bool,
    /// Spawn a bash shell inside a docker container for debugging purposes.
    #[clap(long)]
    docker_shell: bool,
    /// If docker is also set, run estimator in the fully production setting to get usable cost
    /// table. See runtime-params-estimator/emu-cost/README.md for more details.
    /// Works only with enabled docker, because precise computations without it doesn't make sense.
    #[clap(long)]
    full: bool,
    /// Print extra debug information
    #[clap(long, multiple(true), possible_values=&["io", "rocksdb"])]
    debug: Vec<String>,
    /// Extra configuration parameters for RocksDB specific estimations
    #[clap(flatten)]
    db_test_config: RocksDBTestConfig,
}

fn main() -> anyhow::Result<()> {
    let start = time::Instant::now();

    let cli_args = CliArgs::parse();

    // TODO: consider implementing the same in Rust to reduce complexity.
    // Good example: runtime/near-test-contracts/build.rs
    if !cli_args.skip_build_test_contract {
        let build_test_contract = "./build.sh";
        let project_root = project_root();
        let estimator_dir = project_root.join("runtime/runtime-params-estimator/test-contract");
        std::process::Command::new(build_test_contract)
            .current_dir(estimator_dir)
            .output()
            .context("could not build test contract")?;
    }

    let temp_dir;
    let state_dump_path = match cli_args.home {
        Some(it) => it,
        None => {
            temp_dir = tempfile::tempdir()?;

            let contract_code = read_resource(if cfg!(feature = "nightly_protocol_features") {
                "test-contract/res/nightly_small_contract.wasm"
            } else {
                "test-contract/res/stable_small_contract.wasm"
            });

            let state_dump_path = temp_dir.path().to_path_buf();
            nearcore::init_configs(
                &state_dump_path,
                None,
                Some("test.near".parse().unwrap()),
                Some("alice.near"),
                1,
                true,
                None,
                false,
                None,
                false,
                None,
                None,
                None,
            )
            .expect("failed to init config");

            let near_config = load_config(&state_dump_path, GenesisValidationMode::Full);
            let store = create_store(&get_store_path(&state_dump_path));
            GenesisBuilder::from_config_and_store(
                &state_dump_path,
                Arc::new(near_config.genesis),
                store,
            )
            .add_additional_accounts(cli_args.additional_accounts_num)
            .add_additional_accounts_contract(contract_code)
            .print_progress()
            .build()
            .unwrap()
            .dump_state()
            .unwrap();

            state_dump_path
        }
    };

    let debug_options: Vec<_> = cli_args.debug.iter().map(String::as_str).collect();

    if cli_args.docker {
        return main_docker(
            &state_dump_path,
            cli_args.full,
            cli_args.docker_shell,
            debug_options.contains(&"io"),
        );
    }

    if let Some(compare_to) = cli_args.compare_to {
        let baseline = cli_args.costs_file.unwrap();

        let compare_to = read_costs_table(&compare_to)?;
        let baseline = read_costs_table(&baseline)?;
        println!("{}", baseline.diff(&compare_to));
        return Ok(());
    }

    if let Some(path) = cli_args.costs_file {
        let cost_table = read_costs_table(&path)?;

        let runtime_config = costs_to_runtime_config(&cost_table)?;

        println!("Generated RuntimeConfig:\n");
        println!("{:#?}", runtime_config);

        let str = serde_json::to_string_pretty(&runtime_config)
            .expect("Failed serializing the runtime config");

        let output_path = state_dump_path.join("runtime_config.json");
        fs::write(&output_path, &str)
            .with_context(|| format!("failed to write runtime config to file"))?;
        println!("\nOutput saved to:\n\n    {}", output_path.display());

        return Ok(());
    }

    let warmup_iters_per_block = cli_args.warmup_iters;
    let mut rocksdb_test_config = cli_args.db_test_config;
    rocksdb_test_config.debug_rocksdb = debug_options.contains(&"rocksdb");
    let iter_per_block = cli_args.iters;
    let active_accounts = cli_args.accounts_num;
    let metric = match cli_args.metric.as_str() {
        "icount" => GasMetric::ICount,
        "time" => GasMetric::Time,
        other => unreachable!("Unknown metric {}", other),
    };
    let vm_kind = match cli_args.vm_kind.as_deref() {
        Some("wasmer") => VMKind::Wasmer0,
        Some("wasmer2") => VMKind::Wasmer2,
        Some("wasmtime") => VMKind::Wasmtime,
        None => VMKind::for_protocol_version(PROTOCOL_VERSION),
        Some(other) => unreachable!("Unknown vm_kind {}", other),
    };
    let costs_to_measure = cli_args.costs.map(|it| it.split(',').map(str::to_string).collect());

    let config = Config {
        warmup_iters_per_block,
        iter_per_block,
        active_accounts,
        block_sizes: vec![],
        state_dump_path: state_dump_path.clone(),
        metric,
        vm_kind,
        costs_to_measure,
        rocksdb_test_config,
    };
    let cost_table = runtime_params_estimator::run(config);

    let output_path = {
        let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let commit =
            exec("git rev-parse --short HEAD").map(|hash| format!("-{}", hash)).unwrap_or_default();
        let file_name = format!("costs-{}{}.txt", timestamp, commit);

        env::current_dir()?.join(file_name)
    };
    fs::write(&output_path, &cost_table.to_string())?;
    println!(
        "\nFinished in {:.2?}, output saved to:\n\n    {}",
        start.elapsed(),
        output_path.display()
    );

    Ok(())
}

/// Spawns another instance of this binary but inside docker. Most command line args are passed through but `--docker` is removed.
fn main_docker(
    state_dump_path: &Path,
    full: bool,
    debug_shell: bool,
    debug_io_log: bool,
) -> anyhow::Result<()> {
    exec("docker --version").context("please install `docker`")?;

    let project_root = project_root();
    if exec("docker images -q rust-emu")?.is_empty() {
        // Build a docker image if there isn't one already.
        let status = Command::new("docker")
            .args(&["build", "--tag", "rust-emu"])
            .arg(project_root.join("runtime/runtime-params-estimator/emu-cost"))
            .status()?;
        if !status.success() {
            anyhow::bail!("failed to build a docker image")
        }
    }

    let init = {
        // Build a bash script to run inside the container. Concatenating a bash
        // script from strings is fragile, but I don't know a better way.

        let mut buf = String::new();
        buf.push_str("set -ex;\n");
        buf.push_str("cd /host/nearcore;\n");
        buf.push_str(
            "\
cargo build --manifest-path /host/nearcore/Cargo.toml \
  --package runtime-params-estimator --bin runtime-params-estimator \
  --features required --release;
",
        );

        let mut qemu_cmd_builder = QemuCommandBuilder::default();

        if debug_io_log {
            qemu_cmd_builder = qemu_cmd_builder.plugin_log(true).print_on_every_close(true);
        }
        let mut qemu_cmd =
            qemu_cmd_builder.build("/host/nearcore/target/release/runtime-params-estimator")?;

        qemu_cmd.args(&["--home", "/.near"]);
        buf.push_str(&format!("{:?}", qemu_cmd));

        // Sanitize & forward our arguments to the estimator to be run inside
        // docker.
        let mut args = env::args();
        let _binary_name = args.next();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--docker" | "--full" => continue,
                "--additional-accounts-num" | "--home" => {
                    args.next();
                    continue;
                }
                _ => {
                    write!(buf, " {:?}", arg).unwrap();
                }
            }
        }

        write!(buf, " --skip-build-test-contract").unwrap();
        write!(buf, " --additional-accounts-num 0").unwrap();

        buf
    };

    let nearcore =
        format!("type=bind,source={},target=/host/nearcore", project_root.to_str().unwrap());
    let nearhome = format!("type=bind,source={},target=/.near", state_dump_path.to_str().unwrap());

    let mut cmd = Command::new("docker");
    cmd.args(&["run", "--rm", "--cap-add=SYS_PTRACE", "--security-opt", "seccomp=unconfined"])
        .args(&["--mount", &nearcore])
        .args(&["--mount", &nearhome])
        .args(&["--mount", "source=rust-emu-target-dir,target=/host/nearcore/target"])
        .args(&["--mount", "source=rust-emu-cargo-dir,target=/usr/local/cargo"])
        .args(&["--interactive", "--tty"])
        .args(&["--env", "RUST_BACKTRACE=full"]);
    if full {
        cmd.args(&["--env", "CARGO_PROFILE_RELEASE_LTO=fat"])
            .args(&["--env", "CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1"]);
    }
    cmd.arg("rust-emu");

    if debug_shell {
        cmd.args(&["/usr/bin/env", "bash"]);
    } else {
        cmd.args(&["/usr/bin/env", "bash", "-c", &init]);
    }

    cmd.status()?;
    Ok(())
}

fn read_costs_table(path: &Path) -> anyhow::Result<CostTable> {
    fs::read_to_string(&path)
        .with_context(|| format!("failed to read costs file: {}", path.display()))?
        .parse::<CostTable>()
        .map_err(|()| anyhow::format_err!("failed to parse costs file: {}", path.display()))
}

fn exec(command: &str) -> anyhow::Result<String> {
    let args = command.split_ascii_whitespace().collect::<Vec<_>>();
    let (cmd, args) = args.split_first().unwrap();
    let output = std::process::Command::new(cmd)
        .args(args)
        .output()
        .with_context(|| format!("failed to run `{}`", command))?;
    if !output.status.success() {
        anyhow::bail!("failed to run `{}`", command);
    }
    let stdout =
        String::from_utf8(output.stdout).with_context(|| format!("failed to run `{}`", command))?;
    Ok(stdout.trim().to_string())
}

fn project_root() -> PathBuf {
    let dir = env!("CARGO_MANIFEST_DIR");
    let res = PathBuf::from(dir).ancestors().nth(2).unwrap().to_owned();
    assert!(res.join(".github").exists());
    res
}
