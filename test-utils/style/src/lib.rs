#![cfg(test)]
use std::{
    ffi::{OsStr, OsString},
    path::PathBuf,
    process::Command,
};

/// Add common cargo arguments for tests run by this code.
fn cargo_env(cmd: &mut Command) {
    // Set the working directory to the project root, rather than using whatever default nextest
    // gives us.
    let style_root = std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or(OsString::from("./"));
    let wp_root: PathBuf = [&style_root, OsStr::new(".."), OsStr::new("..")].into_iter().collect();
    cmd.current_dir(&wp_root);

    // Use a different target directory to avoid invalidating any cache after tests are run (so
    // that running `cargo nextest` twice does not rebuild half of the workspace on the 2nd
    // rebuild. Unfortunately cargo itself does not readily expose this information to us, so we
    // have to guess a little as to where this directory might end up.
    //
    // NB: We aren't using a temporary directory proper here in order to *allow* keeping cache
    // between individual `clippy` runs and such.
    let target_dir: PathBuf =
        [wp_root.as_os_str(), OsStr::new("target"), OsStr::new("style")].into_iter().collect();
    cmd.env("CARGO_TARGET_DIR", target_dir.as_path());
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
    cargo_env(&mut cmd);
    cmd.args(&["fmt", "--", "--check"]);
    ensure_success(cmd);
}

#[test]
fn clippy() {
    let cargo = std::env::var_os("CARGO").unwrap_or(OsString::from("cargo"));
    let mut cmd = Command::new(cargo);
    cargo_env(&mut cmd);
    cmd.args(&["clippy", "--all-targets", "--all-features", "--locked"]);
    ensure_success(cmd);
}

#[test]
fn deny() {
    let cargo = std::env::var_os("CARGO").unwrap_or(OsString::from("cargo"));
    let mut cmd = Command::new(cargo);
    cargo_env(&mut cmd);
    cmd.args(&["deny", "--all-features", "--locked", "check", "bans"]);
    ensure_success(cmd);
}

#[test]
fn themis() {
    let cargo = std::env::var_os("CARGO").unwrap_or(OsString::from("cargo"));
    let mut cmd = Command::new(cargo);
    cargo_env(&mut cmd);
    cmd.args(&["run", "--locked", "-p", "themis"]);
    ensure_success(cmd);
}
