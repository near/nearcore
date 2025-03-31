use near_primitives::num_rational::Ratio;
use near_primitives::version::PROTOCOL_VERSION;

use crate::utils::process_blocks::prepare_env_with_congestion;

#[test]
fn test_capped_gas_price() {
    let mut env = prepare_env_with_congestion(PROTOCOL_VERSION, Some(Ratio::new_raw(2, 1)), 7).0;
    let mut was_congested = false;
    let mut price_exceeded_limit = false;

    for i in 3..20 {
        env.produce_block(0, i);
        let block = env.clients[0].chain.get_block_by_height(i).unwrap().clone();
        let min_gas_price = env.clients[0].chain.block_economics_config.min_gas_price();
        was_congested |= block.chunks()[0].prev_gas_used() >= block.chunks()[0].gas_limit();
        price_exceeded_limit |= block.header().next_gas_price() > 20 * min_gas_price;
    }

    assert!(was_congested);
    assert!(!price_exceeded_limit);
}
