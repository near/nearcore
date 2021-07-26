use std::path::Path;

use once_cell::sync::OnceCell;

// Lists all cases that we want to measure.
pub mod cases;
// Generates runtime fees from the measurements.
pub mod runtime_fees_generator;
// Generates external costs from the measurements.
pub mod ext_costs_generator;
// Runs a VM (Default: Wasmer) on the given contract and measures the time it takes to do a single operation.
pub mod vm_estimator;
// Collects and processes stats. Prints them on display, plots them, writes them into a file.
pub mod stats;
// Encapsulates the runtime so that it can be run separately from the rest of the node.
pub mod testbed;
// Prepares transactions and feeds them to the testbed in batches. Performs the warm up, takes care
// of nonces.
mod gas_metering;
pub mod testbed_runners;
mod function_call;

/// Lazily loads contract's code from a directory in the source tree.
pub(crate) struct TestContract {
    path: &'static str,
    cell: OnceCell<Vec<u8>>,
}

impl TestContract {
    pub(crate) const fn new(path: &'static str) -> TestContract {
        TestContract { path, cell: OnceCell::new() }
    }
}

impl std::ops::Deref for TestContract {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.cell
            .get_or_init(|| {
                let dir = env!("CARGO_MANIFEST_DIR");
                let path = Path::new(dir).join(self.path);
                std::fs::read(&path).unwrap_or_else(|_err| {
                    panic!("failed to load test resource: {}", path.display())
                })
            })
            .as_slice()
    }
}
