use crate::tests::vm_logic_builder::VMLogicBuilder;
use crate::VMContext;

pub fn create_context() -> VMContext {
    VMContext {
        current_account_id: "alice".parse().unwrap(),
        signer_account_id: "bob".parse().unwrap(),
        signer_account_pk: vec![0, 1, 2, 3, 4],
        predecessor_account_id: "carol".parse().unwrap(),
        input: vec![0, 1, 2, 3, 5],
        block_index: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: 2u128,
        account_locked_balance: 1u128,
        storage_usage: 12,
        attached_deposit: 2u128,
        prepaid_gas: 10_u64.pow(14),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
        current_gas_price: 10u128.pow(8),
        receipt_gas_price: 5 * 10u128.pow(8),
    }
}

macro_rules! decl_test_bytes {
    ($testname:ident, $method:ident, $input:expr) => {
        #[test]
        fn $testname() {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build(create_context());
            let res = vec![0u8; $input.len()];
            logic.$method(0).expect("read bytes into register from context should be ok");
            logic.read_register(0, res.as_ptr() as _).expect("read register should be ok");
            assert_eq!(res, $input);
        }
    };
}

macro_rules! decl_test_u64 {
    ($testname:ident, $method:ident, $input:expr) => {
        #[test]
        fn $testname() {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build(create_context());
            let res = logic.$method().expect("read from context should be ok");
            assert_eq!(res, $input);
        }
    };
}

macro_rules! decl_test_u128 {
    ($testname:ident, $method:ident, $input:expr) => {
        #[test]
        fn $testname() {
            let mut logic_builder = VMLogicBuilder::default();
            let mut logic = logic_builder.build(create_context());
            let buf = [0u8; std::mem::size_of::<u128>()];

            logic.$method(buf.as_ptr() as _).expect("read from context should be ok");
            let res = u128::from_le_bytes(buf);
            assert_eq!(res, $input);
        }
    };
}

decl_test_bytes!(
    test_current_account_id,
    current_account_id,
    create_context().current_account_id.as_ref().as_bytes()
);
decl_test_bytes!(
    test_signer_account_id,
    signer_account_id,
    create_context().signer_account_id.as_ref().as_bytes()
);
decl_test_bytes!(
    test_predecessor_account_id,
    predecessor_account_id,
    create_context().predecessor_account_id.as_ref().as_bytes()
);
decl_test_bytes!(
    test_signer_account_pk,
    signer_account_pk,
    create_context().signer_account_pk.as_slice()
);

decl_test_bytes!(test_random_seed, random_seed, create_context().random_seed.as_slice());

decl_test_bytes!(test_input, input, create_context().input.as_slice());

decl_test_u64!(test_block_index, block_index, create_context().block_index);
decl_test_u64!(test_block_timestamp, block_timestamp, create_context().block_timestamp);
decl_test_u64!(test_storage_usage, storage_usage, create_context().storage_usage);
decl_test_u64!(test_prepaid_gas, prepaid_gas, create_context().prepaid_gas);

decl_test_u128!(
    test_account_balance,
    account_balance,
    create_context().account_balance + create_context().attached_deposit
);
decl_test_u128!(
    test_account_locked_balance,
    account_locked_balance,
    create_context().account_locked_balance
);
decl_test_u128!(test_attached_deposit, attached_deposit, create_context().attached_deposit);
