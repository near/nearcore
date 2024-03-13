#![doc = include_str!("../README.md")]

use anyhow::Context;
use genesis_populate::GenesisBuilder;
use near_chain_configs::GenesisValidationMode;
use near_parameters::vm::VMKind;
use near_parameters::RuntimeConfigView;
use replay::ReplayCmd;
use runtime_params_estimator::config::{Config, GasMetric};
use runtime_params_estimator::{
    costs_to_runtime_config, Cost, CostTable, QemuCommandBuilder, RocksDBTestConfig,
};
use std::env;
use std::ffi::{OsStr, OsString};
use std::fmt::Write;
use std::fs::{self};
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time;
use tracing_subscriber::Layer;

mod replay;

#[derive(clap::Parser)]
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
    /// How many blocks behind the final head is assumed to be compared to the tip.
    ///
    /// This is used to simulate flat state deltas, which depend on finality.
    #[clap(long, default_value = "50")]
    pub finality_lag: usize,
    /// How many key-value pairs change per flat state delta.
    #[clap(long, default_value = "100")]
    pub fs_keys_per_delta: usize,
    /// Skip building test contract which is used in metrics computation.
    #[clap(long)]
    skip_build_test_contract: bool,
    /// What metric to use.
    ///
    /// `time` measures wall-clock time elapsed.
    /// `icount` counts the CPU instructions and syscall-level IO bytes executed
    ///  using qemu instrumentation.
    /// Note that `icount` measurements are not accurate when translating to gas. The main purpose of it is to
    /// have a stable output that can be used to detect performance regressions.
    #[clap(long, default_value = "time", value_parser(["icount", "time"]))]
    metric: String,
    /// Which VM to test.
    #[clap(long, value_enum, default_value_t = VMKind::NearVm)]
    vm_kind: VMKind,
    /// Render existing `costs.txt` as `RuntimeConfig`.
    #[clap(long)]
    costs_file: Option<PathBuf>,
    /// Compare baseline `costs-file` with a different costs file.
    #[clap(long, requires("costs_file"))]
    compare_to: Option<PathBuf>,
    /// Coma-separated lists of a subset of costs to estimate.
    #[clap(long, use_value_delimiter = true)]
    costs: Option<Vec<Cost>>,
    /// Build and run the estimator inside a docker container via QEMU.
    #[clap(long)]
    containerize: bool,
    /// Spawn a bash shell inside a docker container for debugging purposes.
    #[clap(long)]
    container_shell: bool,
    /// Drop OS cache before measurements for better IO accuracy. Requires sudo.
    #[clap(long)]
    drop_os_cache: bool,
    /// Print extra debug information.
    #[clap(long)]
    debug: bool,
    /// Print detailed estimation results in JSON format. One line with one JSON
    /// object per estimation.
    #[clap(long)]
    json_output: bool,
    /// Prints hierarchical execution-timing information using the tracing-span-tree crate.
    #[clap(long)]
    tracing_span_tree: bool,
    /// Records IO events in JSON format and stores it in a given file.
    #[clap(long)]
    record_io_trace: Option<PathBuf>,
    /// Use in-memory test DB, useful to avoid variance caused by DB.
    #[clap(long)]
    pub in_memory_db: bool,
    /// If false, only runs a minimal check that's faster than trying to get accurate results.
    #[clap(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub accurate: bool,
    /// Extra configuration parameters for RocksDB specific estimations
    #[clap(flatten)]
    db_test_config: RocksDBTestConfig,
    #[clap(subcommand)]
    sub_cmd: Option<CliSubCmd>,
}

#[derive(clap::Subcommand)]
enum CliSubCmd {
    Replay(ReplayCmd),
}

fn main() -> anyhow::Result<()> {
    let start = time::Instant::now();
    let cli_args: CliArgs = clap::Parser::parse();

    if let Some(cmd) = cli_args.sub_cmd {
        return match cmd {
            CliSubCmd::Replay(inner) => inner.run(&mut std::io::stdout()),
        };
    }

    if let Some(cost_table) = run_estimation(cli_args)? {
        let output_path = {
            let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
            let commit = exec("git rev-parse --short HEAD")
                .map(|hash| format!("-{}", hash))
                .unwrap_or_default();
            let file_name = format!("costs-{}{}.txt", timestamp, commit);

            env::current_dir()?.join(file_name)
        };
        fs::write(&output_path, &cost_table.to_string())?;
        eprintln!(
            "\nFinished in {:.2?}, output saved to:\n\n    {}",
            start.elapsed(),
            output_path.display()
        );
    }
    Ok(())
}

fn run_estimation(cli_args: CliArgs) -> anyhow::Result<Option<CostTable>> {
    let temp_dir;
    let state_dump_path = match cli_args.home {
        Some(it) => it,
        None => {
            temp_dir = tempfile::tempdir()?;
            temp_dir.path().to_path_buf()
        }
    };
    if state_dump_path.read_dir()?.next().is_none() {
        // Every created account gets this smart contract deployed, such that
        // any account can be used to perform estimations that require this
        // contract.
        // Note: This contract no longer has a fixed size, which means that
        // changes to the test contract might affect all kinds of estimations.
        // (Larger code = more time spent on reading it from the database, for
        // example.) But this is generally a sign of a badly designed
        // estimation, therefore we make no effort to guarantee a fixed size.
        // Also, continuous estimation should be able to pick up such changes.
        let contract_code = near_test_contracts::estimator_contract();

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
            None,
            false,
            None,
            None,
            None,
        )
        .expect("failed to init config");

        let near_config = nearcore::load_config(&state_dump_path, GenesisValidationMode::Full)
            .context("Error loading config")?;
        let store = near_store::NodeStorage::opener(
            &state_dump_path,
            near_config.config.archive,
            &near_config.config.store,
            None,
        )
        .open()
        .unwrap()
        .get_hot_store();
        GenesisBuilder::from_config_and_store(&state_dump_path, near_config, store)
            .add_additional_accounts(cli_args.additional_accounts_num)
            .add_additional_accounts_contract(contract_code.to_vec())
            .print_progress()
            .build()
            .unwrap()
            .dump_state()
            .unwrap();
    }

    if cli_args.containerize {
        main_container(
            &state_dump_path,
            cli_args.accurate,
            cli_args.container_shell,
            cli_args.json_output,
            cli_args.debug,
        )?;
        // The cost table has already been printed inside container, the outer
        // instance does not produce an output.
        return Ok(None);
    }

    if let Some(compare_to) = cli_args.compare_to {
        let baseline = cli_args.costs_file.unwrap();

        let compare_to = read_costs_table(&compare_to)?;
        let baseline = read_costs_table(&baseline)?;
        println!("{}", baseline.diff(&compare_to));
        return Ok(None);
    }

    if let Some(path) = cli_args.costs_file {
        let cost_table = read_costs_table(&path)?;

        let runtime_config = costs_to_runtime_config(&cost_table)?;

        println!("Generated RuntimeConfig:\n");
        println!("{:#?}", runtime_config);

        let config_view = RuntimeConfigView::from(runtime_config);
        let str = serde_json::to_string_pretty(&config_view)
            .expect("Failed serializing the runtime config");

        let output_path = state_dump_path.join("runtime_config.json");
        fs::write(&output_path, &str)
            .with_context(|| "failed to write runtime config to file".to_string())?;
        println!("\nOutput saved to:\n\n    {}", output_path.display());

        return Ok(None);
    }

    #[cfg(feature = "io_trace")]
    let mut _maybe_writer_guard = None;

    if cli_args.tracing_span_tree {
        tracing_span_tree::span_tree().enable();
    } else {
        use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
        let log_layer = tracing_subscriber::fmt::layer()
            .with_filter(tracing_subscriber::EnvFilter::from_default_env());
        let subscriber = tracing_subscriber::registry().with(log_layer);
        #[cfg(feature = "io_trace")]
        let subscriber = subscriber.with(cli_args.record_io_trace.map(|path| {
            let log_file =
                fs::File::create(path).expect("unable to create or truncate IO trace output file");
            let (subscriber, guard) = near_o11y::make_io_tracing_layer(log_file);
            _maybe_writer_guard = Some(guard);
            subscriber
        }));

        #[cfg(not(feature = "io_trace"))]
        if cli_args.record_io_trace.is_some() {
            anyhow::bail!("`--record-io-trace` requires `--feature=io_trace`");
        }

        tracing::subscriber::set_global_default(subscriber)
            .expect("setting default subscriber failed");
    };

    let warmup_iters_per_block = cli_args.warmup_iters;
    let mut rocksdb_test_config = cli_args.db_test_config;
    rocksdb_test_config.debug_rocksdb = cli_args.debug;
    rocksdb_test_config.drop_os_cache = cli_args.drop_os_cache;
    let iter_per_block = cli_args.iters;
    let active_accounts = cli_args.accounts_num;
    let metric = match cli_args.metric.as_str() {
        "icount" => GasMetric::ICount,
        "time" => GasMetric::Time,
        other => unreachable!("Unknown metric {}", other),
    };
    let config = Config {
        warmup_iters_per_block,
        iter_per_block,
        active_accounts,
        finality_lag: cli_args.finality_lag,
        fs_keys_per_delta: cli_args.fs_keys_per_delta,
        state_dump_path: state_dump_path,
        metric,
        vm_kind: cli_args.vm_kind,
        costs_to_measure: cli_args.costs,
        rocksdb_test_config,
        debug: cli_args.debug,
        json_output: cli_args.json_output,
        drop_os_cache: cli_args.drop_os_cache,
        in_memory_db: cli_args.in_memory_db,
        accurate: cli_args.accurate,
    };
    let cost_table = runtime_params_estimator::run(config);
    Ok(Some(cost_table))
}

/// Spawns another instance of this binary but inside a container.
///
/// Most command line args are passed through but `--containerize` is removed.
/// We are now also running with an in-memory database to increase turn-around
/// time and make the results more consistent. Note that this means qemu based
/// IO estimations are inaccurate. They never really have been very accurate
/// anyway and qemu is just not the right tool to measure IO costs.
fn main_container(
    state_dump_path: &Path,
    full: bool,
    debug_shell: bool,
    json_output: bool,
    debug: bool,
) -> anyhow::Result<()> {
    let profile = if full { "release" } else { "dev-release" };
    exec("podman --version").context("please install `podman`")?;

    let project_root = project_root();
    let tagged_image = container_image_name()?;
    if exec(&format!("podman images -q {}", tagged_image))?.is_empty() {
        // Build a docker image if there isn't one already.
        let status = Command::new("podman")
            .args(&["build", "--tag", &tagged_image])
            .arg(project_root.join("runtime/runtime-params-estimator/emu-cost"))
            .status()?;
        if !status.success() {
            anyhow::bail!("failed to build the container image")
        }
    }

    let init = {
        // Build a bash script to run inside the container. Concatenating a bash
        // script from strings is fragile, but I don't know a better way.

        let mut buf = String::new();
        buf.push_str("set -ex;\n");
        buf.push_str("export CARGO_HOME=/.cargo\n");
        buf.push_str("cd /host/nearcore;\n");
        #[cfg(feature = "protocol_feature_bls12381")]
        buf.push_str(
            "CFLAGS='-D__BLST_PORTABLE__' cargo build --manifest-path /host/nearcore/Cargo.toml",
        );
        #[cfg(not(feature = "protocol_feature_bls12381"))]
        buf.push_str("cargo build --manifest-path /host/nearcore/Cargo.toml");
        buf.push_str(" --package runtime-params-estimator --bin runtime-params-estimator");

        // Feature "required" is always necessary for accurate measurements.
        buf.push_str(" --features required");

        // Also add nightly protocol features to docker build if they are enabled.
        #[cfg(feature = "nightly")]
        buf.push_str(",nightly");
        #[cfg(feature = "nightly_protocol")]
        buf.push_str(",nightly_protocol");

        buf.push_str(" --profile ");
        buf.push_str(profile);
        buf.push_str(";");

        let mut qemu_cmd_builder = QemuCommandBuilder::default();

        if debug {
            qemu_cmd_builder = qemu_cmd_builder.plugin_log(true).print_on_every_close(true);
        }
        let mut qemu_cmd = qemu_cmd_builder
            .build(&format!("/host/nearcore/target/{profile}/runtime-params-estimator"))?;

        qemu_cmd.args(&["--home", "/.near"]);
        buf.push_str(&format!("{:?}", qemu_cmd));

        // Sanitize & forward our arguments to the estimator to be run inside
        // the container.
        let mut args = env::args();
        let _binary_name = args.next();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--containerize" => continue,
                "--additional-accounts-num" | "--home" => {
                    args.next();
                    continue;
                }
                _ => {
                    write!(buf, " {:?}", arg).unwrap();
                }
            }
        }

        // test contract has been built by host
        write!(buf, " --skip-build-test-contract").unwrap();
        // accounts have been inserted to state dump by host
        write!(buf, " --additional-accounts-num 0").unwrap();
        // We are now always running qemu based estimations with an in-memory DB
        // because it cannot account for the multi-threaded nature of RocksDB, or
        // the different latencies for disk and memory. Using in-memory DB at
        // least gives consistent and quick results.
        // Note that this still reads all values from the state dump and creates
        // a new testbed for each estimation, we only switch out the storage backend.
        write!(buf, " --in-memory-db").unwrap();

        buf
    };

    let mut nearcore_mount = OsString::from("type=bind,target=/host/nearcore,source=");
    nearcore_mount.push(project_root.as_os_str());
    let mut nearhome_mount = OsString::from("type=bind,target=/.near,source=");
    nearhome_mount.push(state_dump_path.as_os_str());
    let host_target_dir = project_root.join("target").join("estimator");
    let mut target_mount = OsString::from("type=bind,target=/host/nearcore/target,source=");
    target_mount.push(host_target_dir.as_os_str());
    std::fs::create_dir_all(&host_target_dir)
        .context("could not create host target dir at target/estimator")?;
    let cargo_home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| Some(PathBuf::from(std::env::var_os("HOME")?).join(".cargo")))
        .context("could not get cargo home directory")?;
    let mut cargo_mount = OsString::from("type=bind,target=/.cargo,source=");
    cargo_mount.push(cargo_home.as_os_str());
    std::fs::create_dir_all(&host_target_dir)
        .context("could not create host target dir at target/estimator")?;

    let mut cmd = Command::new("podman");
    cmd.args(&[
        "--runtime=crun",
        "run",
        "--rm",
        "--cap-add=SYS_PTRACE",
        "--security-opt",
        "seccomp=unconfined",
    ])
    .args(&["--network", "host"])
    .args(&[OsStr::new("--mount"), &nearcore_mount])
    .args(&[OsStr::new("--mount"), &nearhome_mount])
    .args(&[OsStr::new("--mount"), &target_mount])
    .args(&[OsStr::new("--mount"), &cargo_mount])
    .args(&["--env", "RUST_BACKTRACE=full"]);
    // Spawning an interactive shell and pseudo TTY is necessary for debug shell
    // and nice-to-have in the general case, for cargo to color its output. But
    // it also merges stderr and stdout, which is problem when the stdout should
    // be piped to another process. So far, only JSON output makes sense to
    // pipe, everything else goes to stderr.
    if debug_shell || !json_output {
        cmd.args(&["--interactive", "--tty"]);
    }
    cmd.arg(tagged_image);

    if debug_shell {
        cmd.args(&["/usr/bin/env", "bash"]);
    } else {
        cmd.args(&["/usr/bin/env", "bash", "-c", &init]);
    }

    cmd.status()?;
    Ok(())
}

/// Creates a podman image tag that is unique for each rust version to force re-build when it
/// changes.
fn container_image_name() -> Result<String, anyhow::Error> {
    let image = "rust-emu";
    let dockerfile =
        fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("emu-cost/Dockerfile"))?;
    // The Dockerfile is expected to have a line like this:
    // ```
    // FROM rust:x.y.z
    // ```
    // and the result should be `rust-x.y.z`
    let from_image = dockerfile
        .lines()
        .find_map(|line| line.split_once("FROM "))
        .context("could not parse rustc version from Dockerfile")?
        .1;
    let image_name = from_image.split_once("/").map(|(_, b)| b).unwrap_or(from_image);
    let tag = image_name.replace(":", "-");
    Ok(format!("{}:{}", image, tag))
}

fn read_costs_table(path: &Path) -> anyhow::Result<CostTable> {
    fs::read_to_string(&path)
        .with_context(|| format!("failed to read costs file: {}", path.display()))?
        .parse::<CostTable>()
        .map_err(|e| {
            anyhow::format_err!("failed to parse costs file at {} due to {e}", path.display())
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that we can run simple estimations from start ot finish, including
    /// the state dump creation in a temporary directory.
    ///
    /// This is complementary to regular full runs of all estimations. This test
    /// here is intended to run as pre-commit and therefore should finish
    /// quickly (target: 10-20s).
    ///
    /// Limitation: This will run on nightly test with all workspace features
    /// enabled. It will not cover all compilation errors for building the
    /// params-estimator in isolation.
    #[test]
    fn sanity_check() {
        // select a mix of estimations that are all fast
        let costs = vec![Cost::WasmInstruction, Cost::StorageHasKeyByte, Cost::AltBn128G1SumBase];
        let args = CliArgs {
            home: None,
            warmup_iters: 0,
            iters: 1,
            accounts_num: 100,
            additional_accounts_num: 100,
            finality_lag: 3,
            fs_keys_per_delta: 1,
            skip_build_test_contract: false,
            metric: "time".to_owned(),
            vm_kind: VMKind::NearVm,
            costs_file: None,
            compare_to: None,
            costs: Some(costs),
            containerize: false,
            container_shell: false,
            drop_os_cache: false,
            debug: true,
            json_output: false,
            tracing_span_tree: false,
            record_io_trace: None,
            in_memory_db: false,
            db_test_config: clap::Parser::parse_from(std::iter::empty::<std::ffi::OsString>()),
            sub_cmd: None,
            accurate: true, // we run a small number of estimations, no need to take more shortcuts
        };
        run_estimation(args).unwrap();
    }
}
