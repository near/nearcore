use anyhow::{anyhow, Result};

/// Returns value of given environment variable or error if missing.
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

    Ok(())
}
