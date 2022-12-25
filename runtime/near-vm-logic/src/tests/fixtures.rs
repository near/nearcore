use crate::{VMContext, VMLimitConfig};
use near_primitives_core::config::ViewConfig;
use near_primitives_core::types::Gas;

pub fn get_context(input: Vec<u8>, is_view: bool) -> VMContext {
    VMContext {
        current_account_id: "alice.near".parse().unwrap(),
        signer_account_id: "bob.near".parse().unwrap(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol.near".parse().unwrap(),
        input,
        block_height: 0,
        block_timestamp: 0,
        epoch_height: 0,
        account_balance: 100,
        storage_usage: 0,
        account_locked_balance: 0,
        attached_deposit: 10,
        prepaid_gas: 10_u64.pow(14),
        random_seed: vec![],
        view_config: match is_view {
            true => Some(ViewConfig { max_gas_burnt: VMLimitConfig::test().max_gas_burnt }),
            false => None,
        },
        output_data_receivers: vec![],
    }
}

pub fn get_context_with_prepaid_gas(prepaid_gas: Gas) -> VMContext {
    let mut context = get_context(vec![], false);
    context.prepaid_gas = prepaid_gas;
    context
}
