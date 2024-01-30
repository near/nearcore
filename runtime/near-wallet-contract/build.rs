/// This file is run as a part of `cargo build` process and it builds the `Wallet Contract`.
/// The generated WASM file is put to the `./res` directory.
use anyhow::{anyhow, Context, Ok, Result};

use std::path::{Path, PathBuf};
use std::process::Command;

#[allow(unreachable_code)]
fn main() -> Result<()> {
    // TODO(eth-implicit) Remove this once we have a proper way to generate the Wallet Contract WASM file.
    return Ok(());
    build_contract("./wallet-contract", &[], "wallet_contract")
}

fn build_contract(dir: &str, args: &[&str], output: &str) -> Result<()> {
    let target_dir: PathBuf =
        std::env::var("OUT_DIR").context("Failed to read OUT_DIR environment variable")?.into();

    // We place the build artifacts in `target_dir` (workspace's build directory).
    let mut cmd = cargo_build_cmd(&target_dir);
    cmd.args(args);
    cmd.current_dir(dir);
    run_checking_status(cmd)?;

    let build_artifact_path =
        format!("wasm32-unknown-unknown/release/{}.wasm", dir.replace('-', "_"));
    let src = target_dir.join(build_artifact_path);
    let wasm_target_path = format!("./res/{}.wasm", output);

    std::fs::copy(&src, &wasm_target_path)
        .with_context(|| format!("Failed to copy `{}` to `{}`", src.display(), wasm_target_path))?;

    println!("cargo:rerun-if-changed={}", dir);
    Ok(())
}

/// Creates `cargo build` command to compile the WASM file.
/// Note that we are in `build.rs` file, so this will be called as a part
/// of the global `cargo build` process that already has some flags set.
/// `env_remove` invocations will remove these flags from the nested `cargo build`
/// process, to avoid unexpected behaviors due to the workspace configurations.
// TODO(eth-implicit) Change it to have a reproducible hash of the WASM file.
// see https://github.com/near/nearcore/pull/10269#discussion_r1430139987.
fn cargo_build_cmd(target_dir: &Path) -> Command {
    let mut res = Command::new("cargo");

    res.env_remove("CARGO_BUILD_RUSTFLAGS");
    res.env_remove("CARGO_ENCODED_RUSTFLAGS");
    res.env_remove("RUSTC_WORKSPACE_WRAPPER");

    res.env("RUSTFLAGS", "-Dwarnings");
    res.env("CARGO_TARGET_DIR", target_dir);

    res.args(["build", "--target=wasm32-unknown-unknown", "--release"]);

    res
}

fn run_checking_status(mut cmd: Command) -> Result<()> {
    cmd.status().with_context(|| format!("Failed to run command `{cmd:?}`")).and_then(|status| {
        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("Command `{cmd:?}` exited with non-zero status: {status:?}"))
        }
    })
}
