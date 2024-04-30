mod memory;
mod runner;

pub use memory::NearVmMemory;
pub(crate) use runner::NearVM;

#[derive(Hash)]
struct NearVmConfig {
    seed: u32,
    engine: NearVmEngine,
    compiler: NearVmCompiler,
}

impl NearVmConfig {
    fn config_hash(self: Self) -> u64 {
        crate::utils::stable_hash(&self)
    }
}

#[derive(Hash, PartialEq, Debug)]
#[allow(unused)]
enum NearVmEngine {
    Universal = 1,
    StaticLib = 2,
    DynamicLib = 3,
}

#[derive(Hash, PartialEq, Debug)]
#[allow(unused)]
enum NearVmCompiler {
    Singlepass = 1,
    Cranelift = 2,
    Llvm = 3,
}

// We use following scheme for the bits forming seed:
//  kind << 29, kind 2 is for NearVm
//  major version << 6
//  minor version
const VM_CONFIG: NearVmConfig = NearVmConfig {
    seed: (2 << 29) | (2 << 6) | 2,
    engine: NearVmEngine::Universal,
    compiler: NearVmCompiler::Singlepass,
};

pub(crate) fn near_vm_vm_hash() -> u64 {
    VM_CONFIG.config_hash()
}
