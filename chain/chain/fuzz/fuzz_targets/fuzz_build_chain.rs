#![no_main]
use libfuzzer_sys::fuzz_target;

use chrono::TimeZone;
use std::str::FromStr;

use near_chain::{Block, Error,Chain, Provenance};
use near_chain::test_utils::{setup, build_chain_from_random_data};
use near_primitives::time::{Clock, MockClockGuard};


fuzz_target!(|data: &[u8]| {
    build_chain_from_random_data(data);
});
