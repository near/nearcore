use near_vm_logic::VMContext;

pub fn get_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: "alice.near".to_string(),
        signer_account_id: "bob.near".to_string(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol.near".to_string(),
        input,
        block_index: 0,
        block_timestamp: 0,
        account_balance: 100,
        storage_usage: 0,
        attached_deposit: 10,
        prepaid_gas: 10u64.pow(9),
        random_seed: vec![],
        free_of_charge: false,
        output_data_receivers: vec![],
    }
}
