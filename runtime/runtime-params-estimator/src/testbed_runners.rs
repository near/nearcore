use near_primitives::types::AccountId;
use near_vm_runner::internal::VMKind;
use std::os::raw::c_void;
use std::path::PathBuf;
use std::time::Instant;

/// Get account id from its index.
pub fn get_account_id(account_index: usize) -> AccountId {
    AccountId::try_from(format!("near_{}_{}", account_index, account_index)).unwrap()
}

/// Total number of transactions that we need to prepare.
pub fn total_transactions(config: &Config) -> usize {
    config.block_sizes.iter().sum::<usize>() * config.iter_per_block
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GasMetric {
    // If we measure gas in number of executed instructions, must run under simulator.
    ICount,
    // If we measure gas in elapsed time.
    Time,
}

/// Configuration which we use to run measurements.
#[derive(Debug, Clone)]
pub struct Config {
    /// How many warm up iterations per block should we run.
    pub warmup_iters_per_block: usize,
    /// How many iterations per block are we going to try.
    pub iter_per_block: usize,
    /// Total active accounts.
    pub active_accounts: usize,
    /// Number of the transactions in the block.
    pub block_sizes: Vec<usize>,
    /// Where state dump is located in case we need to create a testbed.
    pub state_dump_path: PathBuf,
    /// Metric used for counting.
    pub metric: GasMetric,
    /// VMKind used
    pub vm_kind: VMKind,
    /// When non-none, only the specified metrics will be measured.
    pub metrics_to_measure: Option<Vec<String>>,
}

// We use several "magical" file descriptors to interact with the plugin in QEMU
// intercepting read syscall. Plugin counts instructions executed and amount of data transferred
// by IO operations. We "normalize" all those costs into instruction count.
const CATCH_BASE: u32 = 0xcafebabe;
const HYPERCALL_START_COUNTING: u32 = 0;
const HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED: u32 = 1;
const HYPERCALL_GET_BYTES_READ: u32 = 2;
const HYPERCALL_GET_BYTES_WRITTEN: u32 = 3;

// See runtime/runtime-params-estimator/emu-cost/README.md for the motivation of constant values.
const READ_BYTE_COST: u64 = 27;
const WRITE_BYTE_COST: u64 = 47;

fn hypercall(index: u32) -> u64 {
    let mut result: u64 = 0;
    unsafe {
        libc::read((CATCH_BASE + index) as i32, &mut result as *mut _ as *mut c_void, 8);
    }
    result
}

pub enum Consumed {
    Instant(Instant),
    None,
}

fn start_count_instructions() -> Consumed {
    hypercall(HYPERCALL_START_COUNTING);
    Consumed::None
}

fn end_count_instructions() -> u64 {
    const USE_IO_COSTS: bool = true;
    if USE_IO_COSTS {
        let result_insn = hypercall(HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED);
        let result_read = hypercall(HYPERCALL_GET_BYTES_READ);
        let result_written = hypercall(HYPERCALL_GET_BYTES_WRITTEN);

        result_insn + result_read * READ_BYTE_COST + result_written * WRITE_BYTE_COST
    } else {
        hypercall(HYPERCALL_STOP_AND_GET_INSTRUCTIONS_EXECUTED)
    }
}

fn start_count_time() -> Consumed {
    Consumed::Instant(Instant::now())
}

fn end_count_time(consumed: &Consumed) -> u64 {
    match *consumed {
        Consumed::Instant(instant) => instant.elapsed().as_nanos().try_into().unwrap(),
        Consumed::None => panic!("Must not be so"),
    }
}

pub fn start_count(metric: GasMetric) -> Consumed {
    return match metric {
        GasMetric::ICount => start_count_instructions(),
        GasMetric::Time => start_count_time(),
    };
}

pub fn end_count(metric: GasMetric, consumed: &Consumed) -> u64 {
    return match metric {
        GasMetric::ICount => end_count_instructions(),
        GasMetric::Time => end_count_time(consumed),
    };
}
