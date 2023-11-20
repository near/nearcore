#![cfg(test)]
use std::{
    ffi::{OsStr, OsString},
    path::PathBuf,
    process::Command,
};

/// Add common cargo arguments for tests run by this code.
fn cargo_env(cmd: &mut Command, target_dir: Option<&str>) {
    // Set the working directory to the project root, rather than using whatever default nextest
    // gives us.
    let style_root = std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or(OsString::from("./"));
    let wp_root: PathBuf = [&style_root, OsStr::new(".."), OsStr::new("..")].into_iter().collect();
    cmd.current_dir(&wp_root);

    if let Some(tgt_dir) = target_dir {
        // Use a different target directory to avoid invalidating any cache after tests are run (so
        // that running `cargo nextest` twice does not rebuild half of the workspace on the 2nd
        // rebuild. Unfortunately cargo itself does not readily expose this information to us, so
        // we have to guess a little as to where this directory might end up.
        //
        // NB: We aren't using a temporary directory proper here in order to *allow* keeping cache
        // between individual `clippy` runs and such.
        let target_dir: PathBuf =
            [wp_root.as_os_str(), OsStr::new("target"), OsStr::new(tgt_dir)].into_iter().collect();
        cmd.env("CARGO_TARGET_DIR", target_dir.as_path());
    }
}

/// Create a cargo command.
///
/// You will want to set `target_dir` to some unique `Some` value whenever there’s a chance that
/// this invocation of `cargo` will build any project code. Setting unique values avoids lock
/// contention and unintentional cache invalidation.
fn cargo(target_dir: Option<&str>) -> Command {
    let cargo = std::env::var_os("CARGO").unwrap_or(OsString::from("cargo"));
    let mut cmd = Command::new(cargo);
    cargo_env(&mut cmd, target_dir);
    cmd
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
    let mut cmd = cargo(None);
    cmd.args(&["fmt", "--", "--check"]);
    ensure_success(cmd);
}

#[test]
fn clippy() {
    let mut cmd = cargo(Some("style"));
    cmd.args(&["clippy", "--all-targets", "--all-features", "--locked"]);
    ensure_success(cmd);
}

#[test]
fn deny() {
    let mut cmd = cargo(None);
    cmd.args(&["deny", "--all-features", "--locked", "check", "bans"]);
    ensure_success(cmd);
}

#[test]
fn themis() {
    let mut cmd = cargo(Some("themis"));
    cmd.args(&["run", "--locked", "-p", "themis"]);
    ensure_success(cmd);
}
