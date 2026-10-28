//! One test that drives a pre-spice chain across the spice activation boundary under an
//! arbitrary combination of faults, checked by one fixed set of oracles. The design is in
//! `docs/architecture/next/spice_activation_scenarios.md`.
//!
//! A [`Scenario`] is a list of typed faults. [`run_scenarios`] runs each through the
//! driver, which builds the environment from the profile and the faults, runs the chain one
//! block at a time firing the progress points the faults attach to, and at the end runs the
//! oracles with the relaxations the faults declare.

use driver::run_scenario;
use near_o11y::testonly::init_test_logger;
use scenario::Scenario;
use std::panic::{AssertUnwindSafe, catch_unwind};

mod driver;
mod faults;
pub(crate) mod loads;
pub(crate) mod oracles;
mod resolve;
mod scenario;

pub(crate) use driver::{drop_chunks_marked_missing, run_until_estimated_first_spice_height};

/// Runs each scenario in turn and keeps going after a failure, so one run shows the whole
/// picture. Panics at the end listing every failing scenario with its message.
///
/// Each scenario runs under `catch_unwind` with `AssertUnwindSafe`, since the env's
/// captures are not unwind-safe, and the env is dropped inside it so its 30-second
/// shutdown finishes before the next scenario builds; a scenario whose shutdown panics is
/// reported as failed too.
pub fn run_scenarios(scenarios: Vec<Scenario>) {
    init_test_logger();
    let mut failures = Vec::new();
    for scenario in scenarios {
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| run_scenario(scenario))) {
            failures.push(driver::panic_message(payload.as_ref()));
        }
    }
    assert!(
        failures.is_empty(),
        "{} scenarios failed:\n\n{}",
        failures.len(),
        failures.join("\n\n"),
    );
}

/// The plain crossing through the scenario driver, run in CI. Nextest's default filter
/// skips `slow_test_` names locally; run it with `--ignore-default-filter`.
#[test]
#[cfg_attr(not(feature = "protocol_feature_spice"), ignore)]
fn slow_test_spice_activation_scenario_empty() {
    run_scenarios(vec![Scenario::empty()]);
}

/// Preset group, run on request.
#[test]
#[ignore]
fn ultra_slow_test_spice_activation_matrix_core() {
    run_scenarios(vec![Scenario::empty()]);
}
