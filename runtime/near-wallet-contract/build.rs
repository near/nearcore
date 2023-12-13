/// This file is run as a part of `cargo build` process and it builds the `Wallet Contract`.
/// The generated WASM file is put to the `./res` directory.
use std::path::{Path, PathBuf};
use std::process::{exit, Command};

type Error = Box<dyn std::error::Error>;

fn main() {
    build_contract("./wallet-contract", &[], "wallet_contract").unwrap_or_else(|err| {
        eprintln!("{}", err);
        exit(1);
    });
}

fn build_contract(dir: &str, args: &[&str], output: &str) -> Result<(), Error> {
    let target_dir: PathBuf = std::env::var("OUT_DIR").unwrap().into();
    // We place the build artifacts in `target_dir` (workspace's build directory).
    let mut cmd = cargo_build_cmd(&target_dir);
    cmd.args(args);
    cmd.current_dir(dir);
    check_status(cmd)?;

    let build_artifact_path =
        format!("wasm32-unknown-unknown/release/{}.wasm", dir.replace('-', "_"));
    let src = target_dir.join(build_artifact_path);
    let wasm_target_path = format!("./res/{}.wasm", output);
    std::fs::copy(&src, wasm_target_path)
        .map_err(|err| format!("failed to copy `{}`: {}", src.display(), err))?;
    println!("cargo:rerun-if-changed=./{}/src/lib.rs", dir);
    println!("cargo:rerun-if-changed=./{}/Cargo.toml", dir);
    Ok(())
}

/// Creates `cargo build` command to compile the WASM file.
/// Note that we are in `build.rs` file, so this will be called as a part
/// of the global `cargo build` process that already has some flags set.
/// `env_remove` invocations will remove these flags from the nested `cargo build`
/// process, to avoid unexpected behaviors due to the workspace configurations.
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

fn check_status(mut cmd: Command) -> Result<(), Error> {
    cmd.status()
        .map_err(|err| format!("command `{cmd:?}` failed to run: {err}"))
        .and_then(|status| {
            if status.success() {
                Ok(())
            } else {
                Err(format!("command `{cmd:?}` exited with non-zero status: {status:?}"))
            }
        })
        .map_err(Error::from)
}
