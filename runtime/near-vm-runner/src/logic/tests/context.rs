use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use near_primitives_core::config::{VMLimitConfig, ViewConfig};

macro_rules! decl_test_bytes {
    ($testname:ident, $method:ident, $ctx:ident, $want:expr) => {
        #[test]
        fn $testname() {
            let mut logic_builder = VMLogicBuilder::default();
            let $ctx = &logic_builder.context;
            let want = $want.to_vec();
            let mut logic = logic_builder.build();
            logic.$method(0).expect("read bytes into register from context should be ok");
            logic.assert_read_register(&want[..], 0);
        }
    };
}

macro_rules! decl_test_u64 {
    ($testname:ident, $method:ident, $ctx:ident, $want:expr) => {
        #[test]
        fn $testname() {
            let mut logic_builder = VMLogicBuilder::default();
            let $ctx = &logic_builder.context;
            let want = $want;

            let mut logic = logic_builder.build();
            let got = logic.$method().expect("read from context should be ok");
            assert_eq!(want, got);
        }
    };
}

macro_rules! decl_test_u128 {
    ($testname:ident, $method:ident, $ctx:ident, $want:expr) => {
        #[test]
        fn $testname() {
            let mut logic_builder = VMLogicBuilder::default();
            let $ctx = &logic_builder.context;
            let want = $want;

            let mut logic = logic_builder.build();
            logic.$method(0).expect("read from context should be ok");
            let got = logic.internal_mem_read(0, 16).try_into().unwrap();
            assert_eq!(u128::from_le_bytes(got), want);
        }
    };
}

decl_test_bytes!(
    test_current_account_id,
    current_account_id,
    ctx,
    ctx.current_account_id.as_ref().as_bytes()
);
decl_test_bytes!(
    test_signer_account_id,
    signer_account_id,
    ctx,
    ctx.signer_account_id.as_ref().as_bytes()
);
decl_test_bytes!(
    test_predecessor_account_id,
    predecessor_account_id,
    ctx,
    ctx.predecessor_account_id.as_ref().as_bytes()
);
decl_test_bytes!(test_signer_account_pk, signer_account_pk, ctx, ctx.signer_account_pk);

decl_test_bytes!(test_random_seed, random_seed, ctx, ctx.random_seed);

decl_test_bytes!(test_input, input, ctx, ctx.input);

decl_test_u64!(test_block_index, block_index, ctx, ctx.block_height);
decl_test_u64!(test_block_timestamp, block_timestamp, ctx, ctx.block_timestamp);
decl_test_u64!(test_storage_usage, storage_usage, ctx, ctx.storage_usage);
decl_test_u64!(test_prepaid_gas, prepaid_gas, ctx, ctx.prepaid_gas);

decl_test_u128!(
    test_account_balance,
    account_balance,
    ctx,
    ctx.account_balance + ctx.attached_deposit
);
decl_test_u128!(
    test_account_locked_balance,
    account_locked_balance,
    ctx,
    ctx.account_locked_balance
);

decl_test_u128!(test_attached_deposit, attached_deposit, ctx, ctx.attached_deposit);

#[test]
fn test_attached_deposit_view() {
    #[track_caller]
    fn test_view(amount: u128) {
        let mut logic_builder = VMLogicBuilder::default();
        let context = &mut logic_builder.context;
        context.view_config =
            Some(ViewConfig { max_gas_burnt: VMLimitConfig::test().max_gas_burnt });
        context.account_balance = 0;
        context.attached_deposit = amount;
        let mut logic = logic_builder.build();

        logic.attached_deposit(0).expect("read from context should be ok");
        let buf =
            logic.internal_mem_read(0, std::mem::size_of::<u128>() as u64).try_into().unwrap();

        let res = u128::from_le_bytes(buf);
        assert_eq!(res, amount);
    }

    test_view(0);
    test_view(1);
    test_view(u128::MAX);
}
