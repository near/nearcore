//! QEMU instrumentation code to get used resources as measured by the QEMU plugin.

use std::os::raw::c_void;

// We use several "magical" file descriptors to interact with the plugin in QEMU
// intercepting read syscall. Plugin counts instructions executed and amount of data transferred
// by IO operations. We "normalize" all those costs into instruction count.
const CATCH_BASE: u32 = 0xcafebabe;
const HYPERCALL_START_COUNTING: u32 = 0;
const HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED: u32 = 1;
const HYPERCALL_GET_BYTES_READ: u32 = 2;
const HYPERCALL_GET_BYTES_WRITTEN: u32 = 3;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct QemuMeasurement {
    pub instructions: u64,
    pub io_r_bytes: u64,
    pub io_w_bytes: u64,
}

impl QemuMeasurement {
    pub(crate) fn start_count_instructions() {
        hypercall(HYPERCALL_START_COUNTING);
    }

    pub(crate) fn end_count_instructions() -> QemuMeasurement {
        let instructions = hypercall(HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED).into();
        let io_r_bytes = hypercall(HYPERCALL_GET_BYTES_READ).into();
        let io_w_bytes = hypercall(HYPERCALL_GET_BYTES_WRITTEN).into();

        QemuMeasurement { instructions, io_r_bytes, io_w_bytes }
    }
}

fn hypercall(index: u32) -> u64 {
    let mut result: u64 = 0;
    unsafe {
        libc::read((CATCH_BASE + index) as i32, &mut result as *mut _ as *mut c_void, 8);
    }
    result
}
