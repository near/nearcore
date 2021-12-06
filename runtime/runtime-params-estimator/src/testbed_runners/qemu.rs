//! QEMU instrumentation code to get used resources as measured by the QEMU plugin.

use std::{ops, os::raw::c_void};

use num_rational::Ratio;

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
    // Integers are not enough because results are averaged over transactions in a block.
    pub instructions: Ratio<u64>,
    pub io_r_bytes: Ratio<u64>,
    pub io_w_bytes: Ratio<u64>,
}

fn hypercall(index: u32) -> u64 {
    let mut result: u64 = 0;
    unsafe {
        libc::read((CATCH_BASE + index) as i32, &mut result as *mut _ as *mut c_void, 8);
    }
    result
}

pub(crate) fn start_count_instructions() {
    hypercall(HYPERCALL_START_COUNTING);
}

pub(crate) fn end_count_instructions() -> QemuMeasurement {
    let instructions = hypercall(HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED).into();
    let io_r_bytes = hypercall(HYPERCALL_GET_BYTES_READ).into();
    let io_w_bytes = hypercall(HYPERCALL_GET_BYTES_WRITTEN).into();

    QemuMeasurement { instructions, io_r_bytes, io_w_bytes }
}

impl QemuMeasurement {
    pub fn zero() -> Self {
        Self { instructions: 0.into(), io_r_bytes: 0.into(), io_w_bytes: 0.into() }
    }
}

impl std::fmt::Debug for QemuMeasurement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}i {}r {}w",
            self.instructions.to_integer(),
            self.io_r_bytes.to_integer(),
            self.io_w_bytes.to_integer()
        )
    }
}

impl ops::Add for QemuMeasurement {
    type Output = QemuMeasurement;

    fn add(self, rhs: QemuMeasurement) -> Self::Output {
        QemuMeasurement {
            instructions: self.instructions + rhs.instructions,
            io_r_bytes: self.io_r_bytes + rhs.io_r_bytes,
            io_w_bytes: self.io_w_bytes + rhs.io_w_bytes,
        }
    }
}

impl ops::Sub for QemuMeasurement {
    type Output = QemuMeasurement;

    fn sub(self, rhs: QemuMeasurement) -> Self::Output {
        QemuMeasurement {
            instructions: self.instructions - rhs.instructions,
            io_r_bytes: self.io_r_bytes - rhs.io_r_bytes,
            io_w_bytes: self.io_w_bytes - rhs.io_w_bytes,
        }
    }
}

impl ops::Mul<u64> for QemuMeasurement {
    type Output = QemuMeasurement;

    fn mul(self, rhs: u64) -> Self::Output {
        QemuMeasurement {
            instructions: self.instructions * rhs,
            io_r_bytes: self.io_r_bytes * rhs,
            io_w_bytes: self.io_w_bytes * rhs,
        }
    }
}

impl ops::Div<u64> for QemuMeasurement {
    type Output = QemuMeasurement;

    fn div(self, rhs: u64) -> Self::Output {
        QemuMeasurement {
            instructions: self.instructions / rhs,
            io_r_bytes: self.io_r_bytes / rhs,
            io_w_bytes: self.io_w_bytes / rhs,
        }
    }
}
