#![cfg(test)]
use std::{
    ffi::{OsStr, OsString},
    path::PathBuf,
    process::Command,
};

/// Add common cargo arguments for tests run by this code.
fn cargo_env(cmd: &mut Command) -> tempfile::TempDir {
    // Use a temporary target directory to avoid invalidating any cache after tests are run (so
    // that running `cargo nextest` twice does not rebuild half of the workspace on the 2nd
    // rebuild.
    let directory = tempfile::tempdir().expect("create a temporary directory for style checks");
    cmd.env("CARGO_TARGET_DIR", directory.path());
    directory
}

fn ensure_success(mut cmd: std::process::Command) {
    println!("Running {:?}", cmd);
    match cmd.status() {
        Err(e) => {
            panic!("Could not spawn the command: {e}")
        }
        Ok(out) if !out.success() => panic!("exit code {:?}", out),
        Ok(_) => {}
    }
}

#[test]
fn rustfmt() {
    let cargo = std::env::var_os("CARGO").unwrap_or(OsString::from("cargo"));
    let mut cmd = Command::new(cargo);
    let _guard = cargo_env(&mut cmd);
    cmd.args(&["fmt", "--", "--check"]);
    ensure_success(cmd);
}

#[test]
fn clippy() {
    // Works as long as this crate isn't moved.
    let root = std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or(OsString::from("./"));
    let script_path = [
        &*root,
        OsStr::new(".."),
        OsStr::new(".."),
        OsStr::new("scripts"),
        OsStr::new("run_clippy.sh"),
    ]
    .into_iter()
    .collect::<PathBuf>();
    let mut command = Command::new(script_path);
    let _guard = cargo_env(&mut command);
    ensure_success(command);
}

#[test]
fn deny() {
    let cargo = std::env::var_os("CARGO").unwrap_or(OsString::from("cargo"));
    let mut cmd = Command::new(cargo);
    let _guard = cargo_env(&mut cmd);
    cmd.args(&["deny", "--all-features", "check", "bans"]);
    ensure_success(cmd);
}
