//! OS sandbox for compiler-daemon workers.
//!
//! The compiler communicates exclusively through already-open pipes and does
//! not need pathname-based filesystem access. Linux workers therefore enter a
//! Landlock domain with no allow rules before processing untrusted Wasm.

#[cfg(target_os = "linux")]
mod linux {
    use landlock::{
        ABI, Access, AccessFs, AccessNet, CompatLevel, Compatible, LandlockStatus, Ruleset,
        RulesetAttr, RulesetCreatedAttr, RulesetStatus, Scope,
    };
    use std::fs::read_dir;
    #[cfg(feature = "test_features")]
    use std::io::Result as IoResult;

    #[derive(Debug)]
    pub struct SandboxStatus {
        #[cfg(feature = "test_features")]
        effective_abi: ABI,
    }

    /// Enter a deny-all Landlock domain.
    ///
    /// ABI v1 filesystem mediation is a hard requirement. Newer filesystem,
    /// network, and process-scoping restrictions are enabled whenever the
    /// running kernel supports them. No allow rules are installed.
    pub fn apply() -> Result<SandboxStatus, String> {
        ensure_single_threaded()?;

        let status = Ruleset::default()
            // A Linux compiler worker must have at least baseline filesystem
            // mediation. Continuing without it would silently run unsandboxed.
            .set_compatibility(CompatLevel::HardRequirement)
            .handle_access(AccessFs::from_all(ABI::V1))
            .map_err(|err| format!("failed to require landlock filesystem access rights: {err}"))?
            // Handle every newer right understood by this build when the
            // kernel supports it. Best-effort here means compatibility with
            // older Landlock ABIs, not best-effort sandbox activation.
            .set_compatibility(CompatLevel::BestEffort)
            .handle_access(AccessFs::from_all(ABI::V7))
            .map_err(|err| format!("failed to configure landlock filesystem access: {err}"))?
            .handle_access(AccessNet::from_all(ABI::V7))
            .map_err(|err| format!("failed to configure landlock network access: {err}"))?
            .scope(Scope::from_all(ABI::V7))
            .map_err(|err| format!("failed to configure landlock process scope: {err}"))?
            .create()
            .map_err(|err| format!("failed to create landlock ruleset: {err}"))?
            // A compromised long-lived worker must not be able to flood the
            // host audit log with intentionally denied accesses.
            .set_compatibility(CompatLevel::BestEffort)
            .log_same_exec(false)
            .map_err(|err| format!("failed to configure landlock audit logging: {err}"))?
            .restrict_self()
            .map_err(|err| format!("failed to enforce landlock ruleset: {err}"))?;

        if !status.no_new_privs {
            return Err("landlock did not enable no_new_privs".to_owned());
        }
        if status.ruleset == RulesetStatus::NotEnforced {
            return Err(format!("landlock ruleset was not enforced: {status:?}"));
        }
        let _effective_abi = match status.landlock {
            LandlockStatus::Available { effective_abi, .. } if effective_abi >= ABI::V1 => {
                effective_abi
            }
            _ => return Err(format!("landlock is unavailable: {status:?}")),
        };

        Ok(SandboxStatus {
            #[cfg(feature = "test_features")]
            effective_abi: _effective_abi,
        })
    }

    /// Landlock domains apply to the calling thread and threads created by it.
    /// Refuse to continue if an earlier initialization step created sibling
    /// threads which would remain outside the sandbox.
    fn ensure_single_threaded() -> Result<(), String> {
        let task_count = read_dir("/proc/self/task")
            .map_err(|err| format!("failed to inspect compiler daemon threads: {err}"))?
            .count();
        if task_count != 1 {
            return Err(format!(
                "compiler daemon must enter landlock before creating threads, found {task_count}"
            ));
        }
        Ok(())
    }

    #[cfg(feature = "test_features")]
    pub fn run_probe(status: &SandboxStatus) -> Result<(), String> {
        use std::fs::{File, OpenOptions};
        use std::net::TcpListener;
        use std::process::id;

        expect_denied("read a file", File::open("/etc/passwd"))?;
        expect_denied("list a directory", read_dir("/"))?;

        let path = format!("/tmp/near-vm-runner-landlock-probe-{}", id());
        expect_denied("create a file", OpenOptions::new().write(true).create_new(true).open(path))?;

        if status.effective_abi >= ABI::V4 {
            expect_denied("bind a TCP socket", TcpListener::bind(("127.0.0.1", 0)))?;
        }
        Ok(())
    }

    #[cfg(feature = "test_features")]
    fn expect_denied<T>(operation: &str, result: IoResult<T>) -> Result<(), String> {
        match result {
            Err(err) if err.raw_os_error() == Some(libc::EACCES) => Ok(()),
            Err(err) => {
                Err(format!("landlock probe failed to {operation} with unexpected error: {err}"))
            }
            Ok(_) => Err(format!("landlock probe unexpectedly managed to {operation}")),
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux::{SandboxStatus, apply};

#[cfg(all(target_os = "linux", feature = "test_features"))]
pub use linux::run_probe;

#[cfg(not(target_os = "linux"))]
#[derive(Debug)]
pub struct SandboxStatus;

/// Landlock is Linux-specific. Other supported development platforms retain
/// process isolation but cannot enforce this filesystem policy.
#[cfg(not(target_os = "linux"))]
pub fn apply() -> Result<SandboxStatus, String> {
    eprintln!("warning: compiler daemon landlock sandbox is unavailable on this platform");
    Ok(SandboxStatus)
}
