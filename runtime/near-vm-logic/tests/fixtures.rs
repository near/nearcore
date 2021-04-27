use near_vm_logic::VMContext;

#[allow(dead_code)]
pub fn get_context(input: Vec<u8>, is_view: bool) -> VMContext {
    VMContext {
        current_account_id: "alice.near".to_string(),
        signer_account_id: "bob.near".to_string(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol.near".to_string(),
        input,
        block_index: 0,
        #[cfg(feature = "protocol_feature_vm_hash")]
        block_hash: [0u8; 32],
        #[cfg(feature = "protocol_feature_vm_hash")]
        prev_block_hash: [0u8; 32],
        block_timestamp: 0,
        epoch_height: 0,
        account_balance: 100,
        storage_usage: 0,
        account_locked_balance: 0,
        attached_deposit: 10,
        prepaid_gas: 10_u64.pow(14),
        random_seed: vec![],
        is_view,
        output_data_receivers: vec![],
    }
}
