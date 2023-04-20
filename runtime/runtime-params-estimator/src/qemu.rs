//! QEMU instrumentation code to get used resources as measured by the QEMU plugin.

use num_rational::Ratio;
use std::fmt::Write;
use std::ops;
use std::os::raw::c_void;
use std::process::Command;

// We use several "magical" file descriptors to interact with the plugin in QEMU
// intercepting read syscall. Plugin counts instructions executed and amount of data transferred
// by IO operations. We "normalize" all those costs into instruction count.
const CATCH_BASE: u32 = 0xcafebabe;
const HYPERCALL_START_COUNTING: u32 = 0;
const HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED: u32 = 1;
const HYPERCALL_GET_BYTES_READ: u32 = 2;
const HYPERCALL_GET_BYTES_WRITTEN: u32 = 3;

#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct QemuMeasurement {
    pub instructions: Ratio<u64>,
    pub io_r_bytes: Ratio<u64>,
    pub io_w_bytes: Ratio<u64>,
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

    pub(crate) fn zero() -> Self {
        QemuMeasurement { instructions: 0.into(), io_r_bytes: 0.into(), io_w_bytes: 0.into() }
    }
}
fn hypercall(index: u32) -> u64 {
    let mut result: u64 = 0;
    unsafe {
        libc::read((CATCH_BASE + index) as i32, &mut result as *mut _ as *mut c_void, 8);
    }
    result
}

/// Create a command to be executed inside QEMU with the custom counter plugin.
#[derive(Default)]
pub struct QemuCommandBuilder {
    started: bool,
    on_every_close: bool,
    count_per_thread: bool,
    plugin_log: bool,
}

impl QemuCommandBuilder {
    /// Start measurement immediately, without having to call `start_count_instructions` first.
    pub fn started(mut self, yes: bool) -> Self {
        self.started = yes;
        self
    }
    /// Print the counters on every close() syscall
    pub fn print_on_every_close(mut self, yes: bool) -> Self {
        self.on_every_close = yes;
        self
    }
    /// Instantiate different counters for each thread
    pub fn count_per_thread(mut self, yes: bool) -> Self {
        self.count_per_thread = yes;
        self
    }

    /// Enable plugin log output to stderr
    pub fn plugin_log(mut self, yes: bool) -> Self {
        self.plugin_log = yes;
        self
    }

    /// Create the final command line
    pub fn build(&self, inner_cmd: &str) -> anyhow::Result<Command> {
        let mut cmd = Command::new(
            "/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64",
        );

        let plugin_path =
            "/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so";

        let mut buf = format!("file={}", plugin_path);
        if self.started {
            write!(buf, ",arg=\"started\"")?;
        }
        if self.count_per_thread {
            write!(buf, ",arg=\"count_per_thread\"")?;
        }
        if self.on_every_close {
            write!(buf, ",arg=\"on_every_close\"")?;
        }
        cmd.args(&["-plugin", &buf]);

        if self.plugin_log {
            cmd.args(&["-d", "plugin"]);
        }

        cmd.args(&["-cpu", "Westmere-v1"]);
        cmd.arg(inner_cmd);

        Ok(cmd)
    }
}

impl ops::Div<u64> for QemuMeasurement {
    type Output = QemuMeasurement;

    fn div(mut self, rhs: u64) -> Self::Output {
        self.instructions /= rhs;
        self.io_r_bytes /= rhs;
        self.io_w_bytes /= rhs;
        self
    }
}

impl ops::Add for QemuMeasurement {
    type Output = QemuMeasurement;

    fn add(mut self, rhs: QemuMeasurement) -> Self::Output {
        self.instructions += rhs.instructions;
        self.io_r_bytes += rhs.io_r_bytes;
        self.io_w_bytes += rhs.io_w_bytes;
        self
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
