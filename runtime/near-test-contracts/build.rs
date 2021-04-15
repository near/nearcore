use std::process::Command;
use std::{fs, io, process};

fn main() {
    if let Err(err) = try_main() {
        eprintln!("{}", err);
        process::exit(1);
    }
}

fn try_main() -> io::Result<()> {
    let mut cmd = Command::new("rustup");
    cmd.args(&["target", "add", "wasm32-unknown-unknown"]);
    check_status(cmd)?;

    build_contract("./test-contract-rs", &[], "test_contract_rs")?;
    build_contract(
        "./test-contract-rs",
        &["--features", "nightly_protocol_features"],
        "nightly_test_contract_rs",
    )?;
    build_contract("./tiny-contract-rs", &[], "tiny_contract_rs")?;
    Ok(())
}

fn build_contract(dir: &str, args: &[&str], output: &str) -> io::Result<()> {
    let mut cmd = cargo_build_cmd();
    cmd.args(args);
    cmd.current_dir(dir);
    check_status(cmd)?;

    fs::copy(
        format!("./{}/target/wasm32-unknown-unknown/release/{}.wasm", dir, dir.replace('-', "_")),
        format!("./res/{}.wasm", output),
    )?;
    println!("cargo:rerun-if-changed=./{}/src/lib.rs", dir);
    println!("cargo:rerun-if-changed=./{}/Cargo.toml", dir);
    Ok(())
}

fn cargo_build_cmd() -> Command {
    let mut res = Command::new("cargo");
    res.env("RUSTFLAGS", "-C link-arg=-s");
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
