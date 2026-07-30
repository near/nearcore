//! Chain IDs of commonly used environment.

/// Main production environment.
pub const MAINNET: &str = "mainnet";

/// Primary testing environment.
pub const TESTNET: &str = "testnet";

/// Pre-release testing environment.
pub const MOCKNET: &str = "mocknet";

/// Local development environment.
pub const LOCALNET: &str = "localnet";

/// Used by ft-benchmark.  http://go/crt-benchmark
pub const BENCHMARKNET: &str = "benchmarknet";

/// Chain id of the networks the test setups spin up. Some setups append a
/// suffix, e.g. `random_chain_id` appends a random one.
pub const TEST_CHAIN: &str = "test-chain";

/// Used by congestion control tests in nayduck.
pub const CONGESTION_CONTROL_TEST: &str = "test-chain-congestion-control";

/// Whether the chain id belongs to a network created locally, i.e. a localnet or
/// one of the test networks.
pub fn is_local(chain_id: &str) -> bool {
    chain_id == LOCALNET || chain_id.starts_with(TEST_CHAIN)
}
