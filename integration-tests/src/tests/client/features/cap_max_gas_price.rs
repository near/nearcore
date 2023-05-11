use near_primitives::num_rational::Ratio;
use near_primitives::version::{ProtocolFeature, ProtocolVersion};

use crate::tests::client::process_blocks::prepare_env_with_congestion;

fn does_gas_price_exceed_limit(protocol_version: ProtocolVersion) -> bool {
    let mut env = prepare_env_with_congestion(protocol_version, Some(Ratio::new_raw(2, 1)), 7).0;
    let mut was_congested = false;
    let mut price_exceeded_limit = false;

    for i in 3..20 {
        env.produce_block(0, i);
        let block = env.clients[0].chain.get_block_by_height(i).unwrap().clone();
        let protocol_version = env.clients[0]
            .epoch_manager
            .get_epoch_protocol_version(block.header().epoch_id())
            .unwrap();
        let min_gas_price =
            env.clients[0].chain.block_economics_config.min_gas_price(protocol_version);
        was_congested |= block.chunks()[0].gas_used() >= block.chunks()[0].gas_limit();
        price_exceeded_limit |= block.header().gas_price() > 20 * min_gas_price;
    }

    assert!(was_congested);
    price_exceeded_limit
}

#[test]
fn test_not_capped_gas_price() {
    assert!(does_gas_price_exceed_limit(ProtocolFeature::CapMaxGasPrice.protocol_version() - 1));
}

#[test]
fn test_capped_gas_price() {
    assert!(!does_gas_price_exceed_limit(ProtocolFeature::CapMaxGasPrice.protocol_version()));
}
