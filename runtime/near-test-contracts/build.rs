use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::{env, fs, io, process};

fn main() {
    if let Err(err) = try_main() {
        eprintln!("{}", err);
        process::exit(1);
    }
}

fn try_main() -> io::Result<()> {
    build_contract("./test-contract-rs", &[], "test_contract_rs")?;
    build_contract(
        "./test-contract-rs",
        &["--features", "nightly_protocol_features"],
        "nightly_test_contract_rs",
    )?;
    build_contract("./contract-for-fuzzing-rs", &[], "contract_for_fuzzing_rs")?;
    build_contract(
        "./test-contract-rs",
        &["--features", "base_protocol"],
        "test_contract_rs_base_protocol",
    )?;
    Ok(())
}

fn build_contract(dir: &str, args: &[&str], output: &str) -> io::Result<()> {
    let mut cmd = cargo_build_cmd();
    cmd.args(args);
    cmd.current_dir(dir);
    check_status(cmd)?;

    let target_dir = shared_target_dir().unwrap_or_else(|| format!("./{}/target", dir).into());
    fs::copy(
        target_dir.join(format!("wasm32-unknown-unknown/release/{}.wasm", dir.replace('-', "_"))),
        format!("./res/{}.wasm", output),
    )?;
    println!("cargo:rerun-if-changed=./{}/src/lib.rs", dir);
    println!("cargo:rerun-if-changed=./{}/Cargo.toml", dir);
    Ok(())
}

fn cargo_build_cmd() -> Command {
    let mut res = Command::new("cargo");
    res.env("RUSTFLAGS", "-C link-arg=-s");
    res.env_remove("CARGO_ENCODED_RUSTFLAGS");
    if let Some(target_dir) = shared_target_dir() {
        res.env("CARGO_TARGET_DIR", target_dir);
    }

    res.args(&["build", "--target=wasm32-unknown-unknown", "--release"]);
    res
}

fn check_status(mut cmd: Command) -> io::Result<()> {
    let status = cmd.status().map_err(|err| {
        io::Error::new(io::ErrorKind::Other, format!("command `{:?}` failed to run: {}", cmd, err))
    })?;
    if !status.success() {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("command `{:?}` exited with non-zero status: {:?}", cmd, status),
        ));
    }
    Ok(())
}

fn shared_target_dir() -> Option<PathBuf> {
    let target_dir = env::var("CARGO_TARGET_DIR").ok()?;
    // Avoid sharing the same target directory with the patent Cargo
    // invocation, to avoid deadlock on the target dir.
    //
    // That is, this logic is needed for the case like the following:
    //
    //    CARGO_TARGET_DIR=/tmp cargo build -p near-test-contracts --release
    Some(Path::new(&target_dir).join("near-test-contracts"))
}
