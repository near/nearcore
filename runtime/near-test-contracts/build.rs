use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::{env, fs, io, process};

type Error = Box<dyn std::error::Error>;

fn main() {
    if let Err(err) = try_main() {
        eprintln!("{}", err);
        process::exit(1);
    }
}

fn try_main() -> Result<(), Error> {
    build_contract("./test-contract-rs", &["--features", "latest_protocol"], "test_contract_rs")?;
    build_contract(
        "./test-contract-rs",
        &["--features", "latest_protocol,nightly"],
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

fn build_contract(dir: &str, args: &[&str], output: &str) -> Result<(), Error> {
    let target_dir = out_dir();

    let mut cmd = cargo_build_cmd(&target_dir);
    cmd.args(args);
    cmd.current_dir(dir);
    check_status(cmd)?;

    let src =
        target_dir.join(format!("wasm32-unknown-unknown/release/{}.wasm", dir.replace('-', "_")));
    fs::copy(&src, format!("./res/{}.wasm", output))
        .map_err(|err| format!("failed to copy `{}`: {}", src.display(), err))?;
    println!("cargo:rerun-if-changed=./{}/src/lib.rs", dir);
    println!("cargo:rerun-if-changed=./{}/Cargo.toml", dir);
    Ok(())
}

fn cargo_build_cmd(target_dir: &Path) -> Command {
    let mut res = Command::new("cargo");

    res.env_remove("CARGO_BUILD_RUSTFLAGS");
    res.env_remove("CARGO_ENCODED_RUSTFLAGS");

    res.env("RUSTFLAGS", "-Dwarnings");
    res.env("CARGO_TARGET_DIR", target_dir);

    res.args(["build", "--target=wasm32-unknown-unknown", "--release"]);

    res
}

fn check_status(mut cmd: Command) -> Result<(), Error> {
    let status = cmd.status().map_err(|err| {
        io::Error::new(io::ErrorKind::Other, format!("command `{:?}` failed to run: {}", cmd, err))
    })?;
    if !status.success() {
        return Err(format!("command `{:?}` exited with non-zero status: {:?}", cmd, status).into());
    }
    Ok(())
}

fn out_dir() -> PathBuf {
    env::var("OUT_DIR").unwrap().into()
}
