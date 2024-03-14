use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{CacheError, CompilationError, VMRunnerError};
use crate::logic::types::PromiseResult;
use crate::logic::{CompiledContract, CompiledContractCache, External, VMContext, VMOutcome};
use crate::ContractCode;
use near_parameters::vm::{Config, VMKind};
use near_parameters::RuntimeFeesConfig;
use near_primitives_core::hash::CryptoHash;
use std::io::{Read, Write};
use std::path::PathBuf;

/// Returned by VM::run method.
///
/// `VMRunnerError` means nearcore is buggy or the data base has been corrupted.
/// We are unable to produce a deterministic result. The correct action usually
/// is to crash or maybe ban a peer and/or send a challenge.
///
/// A `VMOutcome` is a graceful completion of a VM execution. It can also contain
/// an guest error message in the `aborted` field. But these are not errors in
/// the real sense, those are just reasons why execution failed at some point.
/// Such as when a smart contract code panics.
/// Note that the fact that `VMOutcome` contains is tracked on the blockchain.
/// All validators must produce an error deterministically or all should succeed.
/// (See also `PartialExecutionStatus`.)
/// Similarly, the gas values on `VMOutcome` must be the exact same on all
/// validators, even when a guest error occurs, or else their state will diverge.
pub(crate) type VMResult<T = VMOutcome> = Result<T, VMRunnerError>;

/// Validate and run the specified contract.
///
/// This is the entry point for executing a NEAR protocol contract. Before the
/// entry point (as specified by the `method_name` argument) of the contract
/// code is executed, the contract will be validated (see
/// [`crate::prepare::prepare_contract`]), instrumented (e.g. for gas
/// accounting), and linked with the externs specified via the `ext` argument.
///
/// [`VMContext::input`] will be passed to the contract entrypoint as an
/// argument.
///
/// The contract will be executed with the default VM implementation for the
/// current protocol version.
///
/// The gas cost for contract preparation will be subtracted by the VM
/// implementation.
pub fn run(
    code: &ContractCode,
    method_name: &str,
    ext: &mut dyn External,
    context: VMContext,
    wasm_config: &Config,
    fees_config: &RuntimeFeesConfig,
    promise_results: &[PromiseResult],
    cache: Option<&dyn CompiledContractCache>,
) -> VMResult {
    let vm_kind = wasm_config.vm_kind;
    let span = tracing::debug_span!(
        target: "vm",
        "run",
        "code.len" = code.code().len(),
        %method_name,
        ?vm_kind,
        burnt_gas = tracing::field::Empty,
    )
    .entered();

    let runtime = vm_kind
        .runtime(wasm_config.clone())
        .unwrap_or_else(|| panic!("the {vm_kind:?} runtime has not been enabled at compile time"));

    let outcome =
        runtime.run(code, method_name, ext, context, fees_config, promise_results, cache)?;

    span.record("burnt_gas", &outcome.burnt_gas);
    Ok(outcome)
}

pub trait VM {
    /// Validate and run the specified contract.
    ///
    /// This is the entry point for executing a NEAR protocol contract. Before
    /// the entry point (as specified by the `method_name` argument) of the
    /// contract code is executed, the contract will be validated (see
    /// [`crate::prepare::prepare_contract`]), instrumented (e.g. for gas
    /// accounting), and linked with the externs specified via the `ext`
    /// argument.
    ///
    /// [`VMContext::input`] will be passed to the contract entrypoint as an
    /// argument.
    ///
    /// The gas cost for contract preparation will be subtracted by the VM
    /// implementation.
    fn run(
        &self,
        code: &ContractCode,
        method_name: &str,
        ext: &mut dyn External,
        context: VMContext,
        fees_config: &RuntimeFeesConfig,
        promise_results: &[PromiseResult],
        cache: Option<&dyn CompiledContractCache>,
    ) -> VMResult;

    /// Precompile a WASM contract to a VM specific format and store the result
    /// into the `cache`.
    ///
    /// Further calls to [`Self::run`] or [`Self::precompile`] with the same
    /// `code`, `cache` and [`Config`] may reuse the results of this
    /// precompilation step.
    fn precompile(
        &self,
        code: &ContractCode,
        cache: &dyn CompiledContractCache,
    ) -> Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError>;
}

pub trait VMKindExt {
    fn is_available(&self) -> bool;
    /// Make a [`VM`] for this [`VMKind`].
    ///
    /// This is not intended to be used by code other than internal tools like
    /// the estimator.
    fn runtime(&self, config: Config) -> Option<Box<dyn VM>>;
}

impl VMKindExt for VMKind {
    fn is_available(&self) -> bool {
        match self {
            Self::Wasmer0 => cfg!(all(feature = "wasmer0_vm", target_arch = "x86_64")),
            Self::Wasmtime => cfg!(feature = "wasmtime_vm"),
            Self::Wasmer2 => cfg!(all(feature = "wasmer2_vm", target_arch = "x86_64")),
            Self::NearVm => cfg!(all(feature = "near_vm", target_arch = "x86_64")),
        }
    }
    fn runtime(&self, config: Config) -> Option<Box<dyn VM>> {
        match self {
            #[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
            Self::Wasmer0 => Some(Box::new(crate::wasmer_runner::Wasmer0VM::new(config))),
            #[cfg(feature = "wasmtime_vm")]
            Self::Wasmtime => Some(Box::new(crate::wasmtime_runner::WasmtimeVM::new(config))),
            #[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
            Self::Wasmer2 => Some(Box::new(crate::wasmer2_runner::Wasmer2VM::new(config))),
            #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
            Self::NearVm => Some(Box::new(crate::near_vm_runner::NearVM::new(config))),
            #[allow(unreachable_patterns)] // reachable when some of the VMs are disabled.
            _ => {
                let _ = config;
                None
            }
        }
    }
}

pub struct FilesystemCompiledContractCache {
    dir: rustix::fd::OwnedFd,
}

impl FilesystemCompiledContractCache {
    pub fn new() -> Self {
        let path = std::env::var_os("NEAR_CONTRACT_CACHE");
        let path = path.map(PathBuf::from).unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").expect("$HOME"))
                .join(".near")
                .join("data")
                .join("contracts")
        });
        std::fs::create_dir_all(&path).expect("create contract directory");
        let dir = rustix::fs::open(path, rustix::fs::OFlags::DIRECTORY, rustix::fs::Mode::empty())
            .expect("open contract dir");
        Self { dir }
    }
}

/// Cache for compiled contracts code in plain filesystem.
impl CompiledContractCache for FilesystemCompiledContractCache {
    fn put(&self, key: &CryptoHash, value: CompiledContract) -> std::io::Result<()> {
        use rustix::fs::{Mode, OFlags};
        let final_filename = key.to_string();
        let filename = format!("{}.temp", key);
        let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
        let flags = OFlags::CREATE | OFlags::TRUNC | OFlags::WRONLY;
        // FIXME: we probably want to make a file with a temporary name here first (use mktemp
        // crate) and then move it into the expected place.
        // FIXME: remove the file once done if it still exists.
        let mut file = std::fs::File::from(rustix::fs::openat(&self.dir, &filename, flags, mode)?);
        match value {
            CompiledContract::CompileModuleError(_) => {
                // TODO: ideally we probably want this to go into a separate file, or use some sort
                // of heuristic unique flag rather than attempting to encode this entire enum.
                // That's because just dumping the bytes down below gives us pretty nice
                // properties, such as being able to "just" mmap the file...
            }
            CompiledContract::Code(bytes) => {
                file.write_all(&bytes)?;
            }
        }
        drop(file);
        // This is atomic, so there won't be instances where getters see an intermediate state.
        rustix::fs::renameat(&self.dir, filename, &self.dir, final_filename)?;
        Ok(())
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContract>> {
        use rustix::fs::{Mode, OFlags};
        let filename = key.to_string();
        let mode = Mode::empty();
        let flags = OFlags::RDONLY;
        let file = rustix::fs::openat(&self.dir, &filename, flags, mode);
        match file {
            Ok(file) => {
                let stat = rustix::fs::fstat(&file)?;
                // FIXME: this could be file mmap too :)
                let mut out = Vec::with_capacity(stat.st_size.try_into().unwrap());
                let mut file = std::fs::File::from(file);
                file.read_to_end(&mut out)?;
                return Ok(Some(CompiledContract::Code(out)));
            }
            Err(rustix::io::Errno::NOENT) => {
                return Ok(None);
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
}
