/// This file is run as a part of `cargo build` process and it builds the `Wallet Contract`.
/// The generated WASM file is put to the `./res` directory.
use anyhow::{anyhow, Context};

use std::path::Path;
use std::process::Command;

const IMAGE_TAG: &str = "13430592a7be246dd5a29439791f4081e0107ff3";

fn main() -> anyhow::Result<()> {
    build_contract("./implementation", "eth_wallet_contract", "wallet_contract")
}

fn build_contract(dir: &str, contract_name: &str, output: &str) -> anyhow::Result<()> {
    let wasm_target_path = format!("./res/{}.wasm", output);
    if Path::new(&wasm_target_path).exists() {
        // Skip building if an artifact is already present
        return Ok(());
    }

    // We place the build artifacts in `target_dir` (workspace's build directory).
    let absolute_dir = Path::new(dir).canonicalize()?;
    docker_build(absolute_dir.to_str().expect("path should be valid UTF-8"))?;

    let build_artifact_path =
        format!("target/wasm32-unknown-unknown/release/{}.wasm", contract_name);
    let src = absolute_dir.join(build_artifact_path);

    std::fs::copy(&src, &wasm_target_path)
        .with_context(|| format!("Failed to copy `{}` to `{}`", src.display(), wasm_target_path))?;

    println!("cargo:rerun-if-changed={}", dir);
    println!("cargo:rerun-if-changed={}", wasm_target_path);
    Ok(())
}

/// Creates `cargo build` command to compile the WASM file.
/// Note that we are in `build.rs` file, so this will be called as a part
/// of the global `cargo build` process that already has some flags set.
/// `env_remove` invocations will remove these flags from the nested `cargo build`
/// process, to avoid unexpected behaviors due to the workspace configurations.
fn docker_build(host_path: &str) -> anyhow::Result<()> {
    let volume_arg = format!("{host_path}:/host");
    let image_name = format!("nearprotocol/contract-builder:master-{IMAGE_TAG}-amd64");

    let mut cmd = Command::new("docker");
    let status = cmd
        .args([
            "run",
            "--volume",
            &volume_arg,
            "-w",
            "/host",
            "-i",
            "--rm",
            &image_name,
            "./docker-entrypoint.sh",
        ])
        .status();
    status.with_context(|| format!("Failed to run command `{cmd:?}`")).and_then(|status| {
        if status.success() {
            Ok(())
        } else {
            Err(anyhow!("Command `{cmd:?}` exited with non-zero status: {status:?}"))
        }
    })
}
