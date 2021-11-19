#![doc = include_str!("../README.md")]

use anyhow::Context;
use clap::Clap;
use genesis_populate::GenesisBuilder;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::create_store;
use near_vm_runner::internal::VMKind;
use nearcore::{get_store_path, load_config};
use runtime_params_estimator::cases::run;
use runtime_params_estimator::costs_to_runtime_config;
use runtime_params_estimator::testbed_runners::Config;
use runtime_params_estimator::testbed_runners::GasMetric;
use runtime_params_estimator::CostTable;
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
    /// Only test contract compilation costs.
    #[clap(long)]
    compile_only: bool,
    /// Render existing `costs.txt` as `RuntimeConfig`.
    #[clap(long)]
    costs_file: Option<PathBuf>,
    /// Only measure the specified metrics, computing a subset of costs.
    #[clap(long)]
    metrics_to_measure: Option<String>,
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
}

fn main() -> anyhow::Result<()> {
    let start = time::Instant::now();

    let cli_args = CliArgs::parse();

    let temp_dir;
    let state_dump_path = match cli_args.home {
        Some(it) => it,
        None => {
            temp_dir = tempfile::tempdir()?;

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
            );

            let near_config = load_config(&state_dump_path);
            let store = create_store(&get_store_path(&state_dump_path));
            GenesisBuilder::from_config_and_store(
                &state_dump_path,
                Arc::new(near_config.genesis),
                store,
            )
            .add_additional_accounts(cli_args.additional_accounts_num)
            .add_additional_accounts_contract(near_test_contracts::tiny_contract().to_vec())
            .print_progress()
            .build()
            .unwrap()
            .dump_state()
            .unwrap();

            state_dump_path
        }
    };

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

    if cli_args.docker {
        return main_docker(&state_dump_path, cli_args.full, cli_args.docker_shell);
    }

    if let Some(path) = cli_args.costs_file {
        let cost_table = fs::read_to_string(&path)
            .ok()
            .and_then(|it| it.parse::<CostTable>().ok())
            .with_context(|| format!("Failed to parse {}", path.display()))?;

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
    let metrics_to_measure =
        cli_args.metrics_to_measure.map(|it| it.split(',').map(str::to_string).collect());

    let cost_table = run(
        Config {
            warmup_iters_per_block,
            iter_per_block,
            active_accounts,
            block_sizes: vec![],
            state_dump_path: state_dump_path.clone(),
            metric,
            vm_kind,
            metrics_to_measure,
        },
        cli_args.compile_only,
    );

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

fn main_docker(state_dump_path: &Path, full: bool, debug_shell: bool) -> anyhow::Result<()> {
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
        buf.push_str(
            "\
/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
  -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so \
  -cpu Westmere-v1 /host/nearcore/target/release/runtime-params-estimator --home /.near",
        );

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
