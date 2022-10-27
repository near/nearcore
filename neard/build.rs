//! Generates neard version information and stores it in environment variables.
//!
//! The build script sets `NEARD_VERSION` and `NEARD_BUILD` to be neard version
//! and build respectively.  Version is the official semver such as 1.24.1 if
//! the executable is built from a release branch or `trunk` if it’s built from
//! master.  Build is a `git describe` of the commit the binary was built at
//! (for official releases it should be the same as version).

use anyhow::{anyhow, Result};

use std::os::unix::ffi::OsStringExt;

/// Returns value of given environment variable or error if missing.
///
/// This also outputs necessary ‘cargo:rerun-if-env-changed’ tag to make sure
/// build script is rerun if the environment variable changes.
fn env(key: &str) -> Result<std::ffi::OsString> {
    println!("cargo:rerun-if-env-changed={}", key);
    std::env::var_os(key).ok_or_else(|| anyhow!("missing ‘{}’ environment variable", key))
}

/// Calls program with given arguments and returns its standard output.  If
/// calling the program fails or it exits with non-zero exit status returns an
/// error.
fn command(prog: &str, args: &[&str], cwd: Option<std::path::PathBuf>) -> Result<Vec<u8>> {
    println!("cargo:rerun-if-env-changed=PATH");
    let mut cmd = std::process::Command::new(prog);
    cmd.args(args);
    cmd.stderr(std::process::Stdio::inherit());
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    let out = cmd.output()?;
    if out.status.success() {
        let mut stdout = out.stdout;
        if let Some(b'\n') = stdout.last() {
            stdout.pop();
            if let Some(b'\r') = stdout.last() {
                stdout.pop();
            }
        }
        Ok(stdout)
    } else if let Some(code) = out.status.code() {
        Err(anyhow!("{}: terminated with {}", prog, code))
    } else {
        Err(anyhow!("{}: killed by signal", prog))
    }
}

/// Returns version read from git repository or ‘unknown’ if could not be
/// determined.
///
/// Uses `git describe --always --dirty=-modified` to get the version.  For
/// builds on release tags this will return that tag.  In other cases the
/// version will describe the commit by including its hash.  If the working
/// directory isn’t clean, the version will include `-modified` suffix.
fn get_git_version() -> Result<String> {
    // Figure out git directory.  Don’t just assume it’s ../.git because that
    // doesn’t work with git worktrees so use `git rev-parse --git-dir` instead.
    let pkg_dir = std::path::PathBuf::from(env("CARGO_MANIFEST_DIR")?);
    let git_dir = command("git", &["rev-parse", "--git-dir"], Some(pkg_dir));
    let git_dir = match git_dir {
        Ok(git_dir) => std::path::PathBuf::from(std::ffi::OsString::from_vec(git_dir)),
        Err(msg) => {
            // We’re probably not inside of a git repository so report git
            // version as unknown.
            println!("cargo:warning=unable to determine git version (not in git repository?)");
            println!("cargo:warning={}", msg);
            return Ok("unknown".to_string());
        }
    };

    // Make Cargo rerun us if currently checked out commit or the state of the
    // working tree changes.  We try to accomplish that by looking at a few
    // crucial git state files.  This probably may result in some false
    // negatives but it’s best we’ve got.
    for subpath in ["HEAD", "logs/HEAD", "index"] {
        let path = git_dir.join(subpath).canonicalize()?;
        println!("cargo:rerun-if-changed={}", path.display());
    }

    // * --always → if there is no matching tag, use commit hash
    // * --dirty=-modified → append ‘-modified’ if there are local changes
    // * --tags → consider tags even if they are unnanotated
    // * --match=[0-9]* → only consider tags starting with a digit; this
    //   prevents tags such as `crates-0.14.0` from being considered
    let args = &["describe", "--always", "--dirty=-modified", "--tags", "--match=[0-9]*"];
    let out = command("git", args, None)?;
    match String::from_utf8_lossy(&out) {
        std::borrow::Cow::Borrowed(version) => Ok(version.trim().to_string()),
        std::borrow::Cow::Owned(version) => Err(anyhow!("git: invalid output: {}", version)),
    }
}

fn main() {
    if let Err(err) = try_main() {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn try_main() -> Result<()> {
    let version = env("CARGO_PKG_VERSION")?;
    let version = match version.to_string_lossy() {
        std::borrow::Cow::Borrowed("0.0.0") => "trunk",
        std::borrow::Cow::Borrowed(version) => version,
        std::borrow::Cow::Owned(version) => {
            anyhow::bail!("invalid ‘CARGO_PKG_VERSION’: {}", version)
        }
    };
    println!("cargo:rustc-env=NEARD_VERSION={}", version);

    println!("cargo:rustc-env=NEARD_BUILD={}", get_git_version()?);

    println!("cargo:rustc-env=NEARD_RUSTC_VERSION={}", rustc_version::version()?);

    Ok(())
}
