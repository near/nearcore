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
    match command("git", &["describe", "--always", "--dirty=-modified"]) {
        Ok(out) => Ok(std::str::from_utf8(&out)?.trim().into()),
        Err(msg) => {
            println!("cargo:warning=unable to determine git version");
            println!("cargo:warning={}", msg);
            Ok("unknown".into())
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
    let version = version.to_str().unwrap();
    let version = match version {
        "0.0.0" => "trunk".to_string(),
        version => version.to_string(),
    };
    println!("cargo:rustc-env=NEARD_VERSION={}", version);

    println!("cargo:rustc-env=NEARD_BUILD={}", get_git_version()?);

    Ok(())
}
