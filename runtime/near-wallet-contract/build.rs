/// This file is run as a part of `cargo build` process and it builds the `Wallet Contract`.
/// The generated WASM file is put to the `./res` directory.
use anyhow::{anyhow, Context};

use std::path::Path;
use std::process::Command;

const IMAGE_TAG: &str = "13430592a7be246dd5a29439791f4081e0107ff3";

/// See https://chainlist.org/chain/397
const MAINNET_CHAIN_ID: u64 = 397;

/// See https://chainlist.org/chain/398
const TESTNET_CHAIN_ID: u64 = 398;

/// Not officially registered on chainlist.org because this is for local testing only.
const LOCALNET_CHAIN_ID: u64 = 399;

fn main() -> anyhow::Result<()> {
    let contract_dir = "./implementation";

    build_contract(
        contract_dir,
        "eth_wallet_contract",
        "wallet_contract_mainnet",
        MAINNET_CHAIN_ID,
    )
    .context("Mainnet build failed")?;

    build_contract(
        contract_dir,
        "eth_wallet_contract",
        "wallet_contract_testnet",
        TESTNET_CHAIN_ID,
    )
    .context("Testnet build failed")?;

    build_contract(
        contract_dir,
        "eth_wallet_contract",
        "wallet_contract_localnet",
        LOCALNET_CHAIN_ID,
    )
    .context("Localnet build failed")?;

    println!("cargo:rerun-if-changed={}", contract_dir);
    println!("cargo:rerun-if-changed={}", "./res");

    Ok(())
}

fn build_contract(
    dir: &str,
    contract_name: &str,
    output: &str,
    chain_id: u64,
) -> anyhow::Result<()> {
    let wasm_target_path = format!("./res/{}.wasm", output);
    if Path::new(&wasm_target_path).exists() {
        // Skip building if an artifact is already present
        return Ok(());
    }

    let absolute_dir = Path::new(dir).canonicalize()?;

    let chain_id_path = absolute_dir.join("wallet-contract/src/CHAIN_ID");
    let chain_id_content = std::fs::read(&chain_id_path).context("Failed to read CHAIN_ID file")?;

    // Update the chain id before building
    std::fs::write(&chain_id_path, chain_id.to_string().into_bytes())?;
    docker_build(absolute_dir.to_str().expect("path should be valid UTF-8"))?;

    // Restore chain id file to original value after building
    std::fs::write(&chain_id_path, chain_id_content)?;

    let build_artifact_path =
        format!("target/wasm32-unknown-unknown/release/{}.wasm", contract_name);
    let src = absolute_dir.join(build_artifact_path);

    std::fs::copy(&src, &wasm_target_path)
        .with_context(|| format!("Failed to copy `{}` to `{}`", src.display(), wasm_target_path))?;
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
