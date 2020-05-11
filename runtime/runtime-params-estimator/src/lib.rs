#![feature(asm)]

// Lists all cases that we want to measure.
pub mod cases;
// Generates runtime fees from the measurements.
pub mod runtime_fees_generator;
// Generates external costs from the measurements.
pub mod ext_costs_generator;
// Runs Wasmer on the given contract and measures the time it takes to do a single operation.
pub mod wasmer_estimator;
// Collects and processes stats. Prints them on display, plots them, writes them into a file.
pub mod stats;
// Encapsulates the runtime so that it can be run separately from the rest of the node.
pub mod testbed;
// Prepares transactions and feeds them to the testbed in batches. Performs the warm up, takes care
// of nonces.
pub mod testbed_runners;
