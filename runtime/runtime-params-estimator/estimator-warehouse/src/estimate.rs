use crate::{db::Db, import::ImportConfig};
use nix::unistd::Uid;
use xshell::{cmd, Shell};

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
    cmd!(sh, "cargo build --release -p runtime-params-estimator --features runtime-params-estimator/required").run()?;
    // Find binary, some users have CARGO_TARGET_DIR pointing to a custom target directory
    let estimator_binary = if let Ok(target_dir) = sh.var("CARGO_TARGET_DIR") {
        format!("{target_dir}/release/runtime-params-estimator")
    } else {
        format!("{git_root}/target/release/runtime-params-estimator")
    };

    // Actual estimations
    let output = cmd!(sh, "git rev-parse HEAD").output()?;
    let mut commit_hash = String::from_utf8_lossy(&output.stdout).to_string();
    commit_hash.pop(); // \n
    let iters = 5.to_string();
    let warmup_iters = 1.to_string();

    if config.metrics.iter().any(|m| m == "time") {
        let mut maybe_drop_cache = vec![];

        #[cfg(target_family = "unix")]
        if Uid::effective().is_root() {
            maybe_drop_cache.push("--drop-os-cache");
        }

        if maybe_drop_cache.is_empty() {
            eprintln!("Running as non-root, storage related costs might be inaccurate because OS caches cannot be dropped");
        };

        let estimation_output =
            cmd!(sh,
                "{estimator_binary} --iters {iters} --warmup-iters {warmup_iters} --json-output --home {estimator_home} {maybe_drop_cache...} --metric time"
            ).read()?;
        db.import_json_lines(
            &ImportConfig { commit_hash: Some(commit_hash.clone()), protocol_version: None },
            &estimation_output,
        )?;
    }

    if config.metrics.iter().any(|m| m == "icount") {
        let estimation_output =
            cmd!(sh,
                "{estimator_binary} --iters {iters} --warmup-iters {warmup_iters} --json-output --home {estimator_home} --metric icount --docker --full"
            ).read()?;
        db.import_json_lines(
            &ImportConfig { commit_hash: Some(commit_hash), protocol_version: None },
            &estimation_output,
        )?;
    }

    Ok(())
}
