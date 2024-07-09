/// This script is used to build the contracts and copy the wasm files to the
/// `res` directory.
///
/// It writes a few logs with the `debug` prefix. Those are ignored by cargo (as
/// any other messages with prefix other than `cargo::`) but can be seen in the
/// build logs.
use std::env;
use std::process::Command;

type Error = Box<dyn std::error::Error>;

const TEST_FEATURES_ENV: &str = "CARGO_FEATURE_TEST_FEATURES";

fn main() {
    if let Err(err) = try_main() {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn try_main() -> Result<(), Error> {
    let mut test_contract_features = vec!["latest_protocol"];

    let test_features = &env::var(TEST_FEATURES_ENV);
    println!("cargo:rerun-if-env-changed={TEST_FEATURES_ENV}");
    println!("debug: test_features = {test_features:?}");
    if test_features.is_ok() {
        test_contract_features.push("test_features");
    }

    let test_contract_features_string = test_contract_features.join(",");
    build_contract(
        "./test-contract-rs",
        &["--features", &test_contract_features_string],
        "test_contract_rs",
    )?;

    test_contract_features.push("nightly");
    let test_contract_features_string = test_contract_features.join(",");
    build_contract(
        "./test-contract-rs",
        &["--features", &test_contract_features_string],
        "nightly_test_contract_rs",
    )?;
    build_contract("./contract-for-fuzzing-rs", &[], "contract_for_fuzzing_rs")?;
    build_contract("./estimator-contract", &[], "stable_estimator_contract")?;
    build_contract(
        "./estimator-contract",
        &["--features", "nightly"],
        "nightly_estimator_contract",
    )?;
    Ok(())
}

/// build the contract and copy the wasm file to the `res` directory
fn build_contract(dir: &str, args: &[&str], output: &str) -> Result<(), Error> {
    let target_dir = out_dir();

    // build the contract
    let mut cmd = cargo_build_cmd(&target_dir);
    cmd.args(args);
    cmd.current_dir(dir);
    check_status(cmd)?;

    // copy the wasm file to the `res` directory
    let target_path = format!("wasm32-unknown-unknown/release/{}.wasm", dir.replace('-', "_"));
    let from = target_dir.join(target_path);
    let to = format!("./res/{}.wasm", output);
    let copy_result = std::fs::copy(&from, &to);
    copy_result.map_err(|err| format!("failed to copy `{}`: {}", from.display(), err))?;

    println!("cargo:rerun-if-changed=./{}/src/lib.rs", dir);
    println!("cargo:rerun-if-changed=./{}/Cargo.toml", dir);

    println!("debug: from = {from:?}, to = {to:?}");

    Ok(())
}

fn cargo_build_cmd(target_dir: &std::path::Path) -> Command {
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
    println!("debug: running command: {cmd:?}");
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

fn out_dir() -> std::path::PathBuf {
    std::env::var("OUT_DIR").unwrap().into()
}
