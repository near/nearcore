mod cache;
mod compile_errors;
mod fuzzers;
mod rs_contract;
mod runtime_errors;
pub(crate) mod test_builder;
mod ts_contract;
mod wasm_validation;

use crate::vm_kind::VMKind;
use near_primitives::version::ProtocolVersion;
use near_vm_logic::{MemSlice, MemoryLike, VMContext};

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

const LATEST_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::MAX;

fn with_vm_variants(runner: fn(VMKind) -> ()) {
    #[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
    runner(VMKind::Wasmer0);

    #[cfg(feature = "wasmtime_vm")]
    runner(VMKind::Wasmtime);

    #[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
    runner(VMKind::Wasmer2);
}

fn create_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.parse().unwrap(),
        signer_account_id: SIGNER_ACCOUNT_ID.parse().unwrap(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.parse().unwrap(),
        input,
        block_height: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: 2u128,
        account_locked_balance: 0,
        storage_usage: 12,
        attached_deposit: 2u128,
        prepaid_gas: 10_u64.pow(14),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    }
}

/// Small helper to compute expected loading gas cost charged before loading.
///
/// Includes hard-coded value for runtime parameter values
/// `wasm_contract_loading_base` and `wasm_contract_loading_bytes` which would
/// have to be updated if they change in the future.
#[allow(unused)]
fn prepaid_loading_gas(bytes: usize) -> u64 {
    if cfg!(feature = "protocol_feature_fix_contract_loading_cost") {
        35_445_963 + bytes as u64 * 21_6750
    } else {
        0
    }
}

/// Tests for implementation of MemoryLike interface.
///
/// The `factory` returns a [`MemoryLike`] implementation to be tested.  The
/// memory must be configured for indices `0..WASM_PAGE_SIZE` to be valid.
///
/// Panics if any of the tests fails.
#[allow(dead_code)]
pub(crate) fn test_memory_like(factory: impl FnOnce() -> Box<dyn MemoryLike>) {
    // Hardcoded to work around build errors when wasmer_types is not available.
    // Set to value of wasmer_types::WASM_PAGE_SIZE
    const PAGE: u64 = 0x10000;

    struct TestContext {
        mem: Box<dyn MemoryLike>,
        buf: [u8; PAGE as usize + 1],
    }

    impl TestContext {
        fn test_read(&mut self, ptr: u64, len: u64, value: u8) {
            self.buf.fill(!value);
            self.mem.fits_memory(MemSlice { ptr, len }).unwrap();
            self.mem.read_memory(ptr, &mut self.buf[..(len as usize)]).unwrap();
            assert!(self.buf[..(len as usize)].iter().all(|&v| v == value));
        }

        fn test_write(&mut self, ptr: u64, len: u64, value: u8) {
            self.buf.fill(value);
            self.mem.fits_memory(MemSlice { ptr, len }).unwrap();
            self.mem.write_memory(ptr, &self.buf[..(len as usize)]).unwrap();
        }

        fn test_oob(&mut self, ptr: u64, len: u64) {
            self.buf.fill(42);
            self.mem.fits_memory(MemSlice { ptr, len }).unwrap_err();
            self.mem.read_memory(ptr, &mut self.buf[..(len as usize)]).unwrap_err();
            assert!(self.buf[..(len as usize)].iter().all(|&v| v == 42));
            self.mem.write_memory(ptr, &self.buf[..(len as usize)]).unwrap_err();
        }
    }

    let mut ctx = TestContext { mem: factory(), buf: [0; PAGE as usize + 1] };

    // Test memory is initialised to zero.
    ctx.test_read(0, PAGE, 0);
    ctx.test_read(PAGE, 0, 0);
    ctx.test_read(0, PAGE / 2, 0);
    ctx.test_read(PAGE / 2, PAGE / 2, 0);

    // Test writing works.
    ctx.test_write(0, PAGE / 2, 42);
    ctx.test_read(0, PAGE / 2, 42);
    ctx.test_read(PAGE / 2, PAGE / 2, 0);

    ctx.test_write(PAGE / 4, PAGE / 4, 24);
    ctx.test_read(0, PAGE / 4, 42);
    ctx.test_read(PAGE / 4, PAGE / 4, 24);
    ctx.test_read(PAGE / 2, PAGE / 2, 0);

    // Zero memory.
    ctx.test_write(0, PAGE, 0);
    ctx.test_read(0, PAGE, 0);

    // Test out-of-bounds checks.
    ctx.test_oob(0, PAGE + 1);
    ctx.test_oob(1, PAGE);
    ctx.test_oob(PAGE - 1, 2);
    ctx.test_oob(PAGE, 1);

    // None of the writes in OOB should have any effect.
    ctx.test_read(0, PAGE, 0);
}
