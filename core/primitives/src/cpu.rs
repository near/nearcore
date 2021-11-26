/// Try to ensure that the required CPU features are available and enabled for the current CPU.
///
/// Note that this is a best-effort kind of thing. If these features are already enabled when
/// compiling the program, then we may very well end up executing some unsupported instruction
/// before we even get to executing this code.
pub fn ensure_cpu_compatibility(force: bool) {
    if cfg!(near_production) || force {
        imp::ensure_cpu_compatibility();
    } else {
        // Intentionally empty: allow execution without verifying the properties of the CPU.
    }
}

mod imp {
    const NOTE: &str = "confirm hardware requirements at \
        <https://docs.near.org/docs/develop/node/validator/hardware>.";

    #[cfg(not(target_arch = "x86_64"))]
    pub fn ensure_cpu_compatibility() {
        eprintln!("error: processor running a x86_64 instruction set is required.\nnote: {}", NOTE);
        std::process::exit(1);
    }

    #[cfg(target_arch = "x86_64")]
    pub fn ensure_cpu_compatibility() {
        // SAFETY: this is “just” a CPUID. Some very ancient chips don't support this instruction
        // and may do anything else with the opcode.
        //
        // As an implementation note, we cannot use `is_*_feature_available!` macro from libstd
        // here, because it will unconditionally return `true` when `cfg(target_feature="...")` is
        // true.
        let cpuid01 = unsafe { std::arch::x86_64::__cpuid(0x01) };
        ensure_cpuid01(cpuid01.ecx, cpuid01.edx);
        let cpuid0d = unsafe { std::arch::x86_64::__cpuid(0x0d) };
        ensure_cpuid0d(cpuid0d.eax);
    }

    fn ensure_cpuid01(ecx: u32, edx: u32) {
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
            let joined = features.join(", ");
            eprintln!("error: processor does not support these required CPU features: {}.", joined);
            eprintln!("note: {}", NOTE);
            std::process::exit(1);
        }
    }

    fn ensure_cpuid0d(eax: u32) {
        const SSE_EAX_FLAG: u32 = 1 << 1;
        if eax & SSE_EAX_FLAG != SSE_EAX_FLAG {
            eprintln!("error: SSE CPU feature is not enabled.\nnote: {}", NOTE);
            std::process::exit(1);
        }
        const AVX_EAX_FLAG: u32 = 1 << 2;
        if eax & AVX_EAX_FLAG != AVX_EAX_FLAG {
            eprintln!("error: AVX CPU feature is not enabled.\nnote: {}", NOTE);
            std::process::exit(1);
        }
    }
}
