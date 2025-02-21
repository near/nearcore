use crate::{db::Db, import::ImportConfig};
use nix::unistd::Uid;
use xshell::{Shell, cmd};

/// Additional information required for estimation.
#[derive(Debug, clap::Parser)]
pub(crate) struct EstimateConfig {
    /// Specify the directory of a different repository, if not estimating the current one.
    #[clap(long)]
    pub external_repo: Option<String>,
    /// Specify the directory for near state used by the estimator. Will use
    /// temporary directory if unspecified.
    #[clap(long)]
    pub home: Option<String>,
    /// Comma separated list of metrics to use in estimation.
    #[clap(long, default_value = "icount,time", value_parser(["icount", "time"]), use_value_delimiter = true)]
    pub metrics: Vec<String>,
    /// Bundle of estimator config options.
    #[clap(long, value_enum, default_value_t = Mode::Default)]
    pub mode: Mode,
}

/// The mode selects a pre-selected bundle of config options for the estimator.
///
/// Pick one of these modes when running the estimator through the warehouse
/// scripts. For more control, such as choosing exactly which estimations to run
/// or skip, run the estimator directly without the warehouse script.
#[derive(Debug, Clone, Copy, PartialEq, clap::ValueEnum)]
pub(crate) enum Mode {
    /// Standardized config options for a full estimation.
    Default,
    /// Single iteration with fastest possible config options.
    Fast,
}

pub(crate) fn run_estimation(db: &Db, config: &EstimateConfig) -> anyhow::Result<()> {
    let sh = Shell::new()?;

    let mut _maybe_tmp = None;

    let estimator_home = match &config.home {
        Some(home) => home,
        None => {
            _maybe_tmp = Some(tempfile::tempdir()?);
            _maybe_tmp.as_ref().unwrap().path().to_str().unwrap()
        }
    };

    if let Some(external_repo) = &config.external_repo {
        sh.change_dir(external_repo);
    }
    let git_root = cmd!(sh, "git rev-parse --show-toplevel").read()?;

    // Ensure full optimization
    let _env_guard_one = sh.push_env("CARGO_PROFILE_RELEASE_LTO", "fat");
    let _env_guard_two = sh.push_env("CARGO_PROFILE_RELEASE_CODEGEN_UNITS", "1");

    // Build estimator
    let cargo_profile = config.mode.cargo_profile();
    cmd!(sh, "cargo build --profile {cargo_profile} -p runtime-params-estimator --features runtime-params-estimator/required").run()?;
    // Find binary, some users have CARGO_TARGET_DIR pointing to a custom target directory
    let estimator_binary = if let Ok(target_dir) = sh.var("CARGO_TARGET_DIR") {
        format!("{target_dir}/{cargo_profile}/runtime-params-estimator")
    } else {
        format!("{git_root}/target/{cargo_profile}/runtime-params-estimator")
    };

    // Actual estimations
    let output = cmd!(sh, "git rev-parse HEAD").output()?;
    let mut commit_hash = String::from_utf8_lossy(&output.stdout).to_string();
    commit_hash.pop(); // \n
    let iters = config.mode.iters();
    let warmup_iters = config.mode.warmup_iters();

    if config.metrics.iter().any(|m| m == "time") {
        let mut optional_args = vec![];

        #[cfg(target_family = "unix")]
        if Uid::effective().is_root() {
            optional_args.push("--drop-os-cache");
        } else {
            eprintln!(
                "Running as non-root, storage related costs might be inaccurate because OS caches cannot be dropped"
            );
        }

        optional_args.append(&mut config.mode.optional_args_time_metric());

        let estimation_output =
            cmd!(sh,
                "{estimator_binary} --iters {iters} --warmup-iters {warmup_iters} --json-output --home {estimator_home} {optional_args...} --metric time"
            ).read()?;
        db.import_json_lines(
            &ImportConfig { commit_hash: Some(commit_hash.clone()), protocol_version: None },
            &estimation_output,
        )?;
    }

    if config.metrics.iter().any(|m| m == "icount") {
        let estimation_output =
            cmd!(sh,
                "{estimator_binary} --iters {iters} --warmup-iters {warmup_iters} --json-output --home {estimator_home} --metric icount --containerize"
            ).read()?;
        db.import_json_lines(
            &ImportConfig { commit_hash: Some(commit_hash), protocol_version: None },
            &estimation_output,
        )?;
    }

    Ok(())
}

impl Mode {
    fn iters(self) -> &'static str {
        match self {
            Mode::Default => "5",
            Mode::Fast => "1",
        }
    }

    fn warmup_iters(self) -> &'static str {
        match self {
            Mode::Default => "1",
            Mode::Fast => "0",
        }
    }

    fn cargo_profile(self) -> &'static str {
        match self {
            Mode::Default => "release",
            Mode::Fast => "dev-release",
        }
    }

    fn optional_args_time_metric(self) -> Vec<&'static str> {
        match self {
            Mode::Default => vec![],
            Mode::Fast => vec![
                "--in-memory-db",
                "--additional-accounts-num=1000",
                "--accounts-num=1000",
                "--accurate=false",
            ],
        }
    }
}
