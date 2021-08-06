//! run_test is a framework for creating and executing runtime scenarios.
//! You can create Scenario in rust code or have it in a JSON file.
//! Scenario::run executes scenario, keeping track of different metrics.
//! So far, the only metric is how much time block production takes.
//! fuzzing provides Arbitrary trait for Scenario, thus enabling creating random scenarios.
pub mod fuzzing;
pub mod run_test;
pub use crate::run_test::{BlockConfig, NetworkConfig, Scenario, TransactionConfig};
