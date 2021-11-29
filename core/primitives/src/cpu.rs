#[derive(Debug)]
pub enum Error {
    /// Unsupported architecture
    Architecture,
    /// The following required features are not supported by the CPU.
    MissingFeatures(Vec<&'static str>),
    /// SSE is available but disabled.
    SSEDisabled,
    /// AVX is available but disabled.
    AVXDisabled,
}

impl Error {
    pub const NOTE: &'static str = "confirm hardware requirements at \
        <https://docs.near.org/docs/develop/node/validator/hardware>.";

    pub fn terminate(&self) -> ! {
        eprintln!("error: {}\nnote: {}", self, Self::NOTE);
        std::process::exit(1);
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Error::*;
        match self {
            Architecture => f.write_str("unsupported CPU architecture")?,
            SSEDisabled => f.write_str("SSE CPU feature is supported but disabled")?,
            AVXDisabled => f.write_str("AVX CPU feature is supported but disabled")?,
            MissingFeatures(features) => {
                f.write_str("required CPU features are missing: ")?;
                let mut features = features.iter().peekable();
                while let Some(feature) = features.next() {
                    f.write_str(feature)?;
                    if features.peek().is_some() {
                        f.write_str(", ")?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Try to ensure that the required CPU features are available and enabled for the current CPU.
///
/// If any of the features are not available, the returned error will specify which ones are
/// missing.
///
/// Note that this is a best-effort kind of thing. If these features are already enabled when
/// compiling the program, then we may very well end up executing some unsupported instruction
/// before we even get to executing this code.
pub fn verify_cpu_compatibility() -> Result<(), Error> {
    imp::verify_cpu_compatibility()
}

mod imp {
    #[cfg(not(target_arch = "x86_64"))]
    pub fn verify_cpu_compatibility() -> Result<(), super::Error> {
        Err(super::Error::Architecture)
    }

    #[cfg(target_arch = "x86_64")]
    pub fn verify_cpu_compatibility() -> Result<(), super::Error> {
        ensure_cpu_info()?;
        ensure_cpu_state()
    }

    // NB: You can search for the CPUID flags by searching for `CPUID.01H` in the Intel® 64 and
    // IA-32 architectures software developer’s manual combined volumes: 1, 2A, 2B, 2C, 2D, 3A, 3B,
    // 3C, 3D, and 4, available for a download at
    // <https://www.intel.com/content/www/us/en/developer/articles/technical/intel-sdm.html>.
    //
    // Alternatively a resource such as https://www.felixcloutier.com/x86/cpuid may be more
    // convenient as it provides all the information in a single table.
    fn ensure_cpu_info() -> Result<(), super::Error> {
        let cpu_info = unsafe {
            const CPUID_LEAF_PROCESSOR_INFO: u32 = 0x01;
            // SAFETY: this is “just” a CPUID instruction. Some very ancient chips don't support
            // this instruction and may do anything else with the opcode, but at that point che CPU
            // is very significantly outside of the hardware requirements specified by us.
            //
            // As an implementation note, we cannot use `is_*_feature_available!` macro from libstd
            // here, because it will unconditionally return `true` when
            // `cfg!(target_feature="...")` is true.
            std::arch::x86_64::__cpuid(CPUID_LEAF_PROCESSOR_INFO)
        };
        let (ecx, edx) = (cpu_info.ecx, cpu_info.edx);
        const SSE_EDX_FLAG: u32 = 1 << 25;
        const SSE2_EDX_FLAG: u32 = 1 << 26;
        const SSE3_ECX_FLAG: u32 = 1 << 0;
        const SSSE3_ECX_FLAG: u32 = 1 << 9;
        const CMPXCHG16B_ECX_FLAG: u32 = 1 << 13;
        const SSE41_ECX_FLAG: u32 = 1 << 19;
        const SSE42_ECX_FLAG: u32 = 1 << 20;
        const POPCNT_ECX_FLAG: u32 = 1 << 23;
        const AVX_ECX_FLAG: u32 = 1 << 28;
        let mut features = Vec::new();
        if edx & SSE_EDX_FLAG != SSE_EDX_FLAG {
            features.push("SSE");
        }
        if edx & SSE2_EDX_FLAG != SSE2_EDX_FLAG {
            features.push("SSE2");
        }
        if ecx & SSE3_ECX_FLAG != SSE3_ECX_FLAG {
            features.push("SSE3");
        }
        if ecx & SSSE3_ECX_FLAG != SSSE3_ECX_FLAG {
            features.push("SSSE3");
        }
        if ecx & CMPXCHG16B_ECX_FLAG != CMPXCHG16B_ECX_FLAG {
            features.push("CMPXCHG16B");
        }
        if ecx & SSE41_ECX_FLAG != SSE41_ECX_FLAG {
            features.push("SSE4.1");
        }
        if ecx & SSE42_ECX_FLAG != SSE42_ECX_FLAG {
            features.push("SSE4.2");
        }
        if ecx & POPCNT_ECX_FLAG != POPCNT_ECX_FLAG {
            features.push("POPCNT");
        }
        if ecx & AVX_ECX_FLAG != AVX_ECX_FLAG {
            features.push("AVX");
        }

        if !features.is_empty() {
            return Err(super::Error::MissingFeatures(features));
        }
        Ok(())
    }

    // NB: See the comment for `ensure_cpu_info`.
    fn ensure_cpu_state() -> Result<(), super::Error> {
        let cpu_state = unsafe {
            const CPUID_LEAF_PROCESSOR_EXTENDED_STATE: u32 = 0x0D;
            // SAFETY: this is “just” a CPUID instruction. Some very ancient chips don't support
            // this instruction and may do anything else with the opcode, but at that point che CPU
            // is very significantly outside of the hardware requirements specified by us.
            //
            // As an implementation note, we cannot use `is_*_feature_available!` macro from libstd
            // here, because it will unconditionally return `true` when
            // `cfg!(target_feature="...")` is true.
            std::arch::x86_64::__cpuid(CPUID_LEAF_PROCESSOR_EXTENDED_STATE)
        };
        const SSE_EAX_FLAG: u32 = 1 << 1;
        if cpu_state.eax & SSE_EAX_FLAG != SSE_EAX_FLAG {
            return Err(super::Error::SSEDisabled);
        }
        const AVX_EAX_FLAG: u32 = 1 << 2;
        if cpu_state.eax & AVX_EAX_FLAG != AVX_EAX_FLAG {
            return Err(super::Error::AVXDisabled);
        }
        Ok(())
    }
}
