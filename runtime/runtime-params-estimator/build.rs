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
    let res = Path::new("./test-contract/res");

    fs::create_dir_all(res)?;
    build_contract("small", &res.join("smallest_contract.wasm"))?;

    for mode in ["stable", "nightly"] {
        let make_payload = {
            let features = if mode == "nightly" { "nightly_protocol_features" } else { "" };
            let baseline = res.join(format!("{}_baseline_contract.wasm", mode));
            build_contract(features, &baseline)?;
            let size = std::fs::metadata(&baseline)?.len();

            move |desired_size: u64| -> io::Result<()> {
                let payload_size = desired_size.saturating_sub(size);
                let payload: Vec<u8> = random_bytes().take(payload_size as usize).collect();
                fs::write(res.join("payload"), &payload)
            }
        };

        let kibibyte = 1024;

        let features =
            if mode == "nightly" { "payload,nightly_protocol_features" } else { "payload" };

        make_payload(10 * kibibyte)?;
        build_contract(&features, &res.join(format!("{}_small_contract.wasm", mode)))?;

        make_payload(100 * kibibyte)?;
        build_contract(&features, &res.join(format!("{}_medium_contract.wasm", mode)))?;

        make_payload(1024 * kibibyte)?;
        build_contract(&features, &res.join(format!("{}_large_contract.wasm", mode)))?;
    }

    println!("cargo:rerun-if-changed=./test-contract/src");
    println!("cargo:rerun-if-changed=./test-contract/Cargo.toml");

    Ok(())
}

fn build_contract(features: &str, output: &Path) -> io::Result<()> {
    let mut cargo = Command::new("cargo");
    cargo.env_remove("CARGO_ENCODED_RUSTFLAGS");
    if let Some(target_dir) = shared_target_dir() {
        cargo.env("CARGO_TARGET_DIR", target_dir);
    }

    cargo.args(&["build", "--target=wasm32-unknown-unknown", "--release"]);
    cargo.current_dir("./test-contract");
    cargo.args(&["--features", features]);
    check_status(cargo)?;

    let target_dir = shared_target_dir().unwrap_or_else(|| "./test-contract/target".into());
    fs::copy(target_dir.join("wasm32-unknown-unknown/release/test_contract.wasm"), output)?;
    Ok(())
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
    //    CARGO_TARGET_DIR=/tmp cargo build -p param-estimator --release
    Some(Path::new(&target_dir).join("estimator-test-contract"))
}

// This is build.rs, so we are relatively sensitive to compile times and don't
// want or need to pull in `rand`.
//
// Source:
// <https://github.com/rust-lang/rust/blob/1.55.0/library/core/src/slice/sort.rs#L559-L573>
fn random_bytes() -> impl Iterator<Item = u8> {
    let mut random = 92u32;
    std::iter::repeat_with(move || {
        random ^= random << 13;
        random ^= random >> 17;
        random ^= random << 5;
        random.to_le_bytes()
    })
    .flatten()
}
