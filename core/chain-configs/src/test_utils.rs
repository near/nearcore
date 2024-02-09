use near_primitives::static_clock::StaticClock;
use near_primitives::version::PROTOCOL_VERSION;
use num_rational::Ratio;

use crate::GenesisConfig;

impl GenesisConfig {
    pub fn test() -> Self {
        GenesisConfig {
            genesis_time: StaticClock::utc(),
            genesis_height: 0,
            gas_limit: 10u64.pow(15),
            min_gas_price: 0,
            max_gas_price: 1_000_000_000,
            total_supply: 1_000_000_000,
            gas_price_adjustment_rate: Ratio::from_integer(0),
            transaction_validity_period: 100,
            epoch_length: 5,
            protocol_version: PROTOCOL_VERSION,
            ..Default::default()
        }
    }
}
