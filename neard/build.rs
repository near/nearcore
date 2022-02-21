//! Generates neard version information and stores it in environment variables.
//!
//! The build script sets `NEARD_VERSION` and `NEARD_BUILD` to be neard version
//! and build respectively.  Version is the official semver such as 1.24.1 if
//! the executable is built from a release branch or `trunk` if it’s built from
//! master.  Build is a `git describe` of the commit the binary was built at
//! (for official releases it should be the same as version).

use anyhow::{anyhow, Result};

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
fn command(prog: &str, args: &[&str]) -> Result<Vec<u8>> {
    println!("cargo:rerun-if-env-changed=PATH");
    let out = std::process::Command::new(prog)
        .args(args)
        .stderr(std::process::Stdio::inherit())
        .output()?;
    if out.status.success() {
        Ok(out.stdout)
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
    let git_dir = std::path::Path::new(&env("CARGO_MANIFEST_DIR")?).join("../.git");
    for subpath in ["HEAD", "logs/HEAD", "index"] {
        let path = git_dir.join(subpath).canonicalize()?;
        println!("cargo:rerun-if-changed={}", path.display());
    }
    let out = command("git", &["describe", "--always", "--dirty=-modified"]);
    match out.as_ref().map(|ver| String::from_utf8_lossy(&ver)) {
        Ok(std::borrow::Cow::Borrowed(version)) => Ok(version.trim().to_string()),
        Ok(std::borrow::Cow::Owned(version)) => Err(anyhow!("git: invalid output: {}", version)),
        Err(msg) => {
            println!("cargo:warning=unable to determine git version");
            println!("cargo:warning={}", msg);
            Ok("unknown".to_string())
        }
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
