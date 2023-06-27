use crate::logic::tests::vm_logic_builder::VMLogicBuilder;

macro_rules! test_prohibited {
    ($f: ident $(, $arg: expr )* ) => {
        let mut logic_builder = VMLogicBuilder::view();
        #[allow(unused_mut)]
        let mut logic = logic_builder.build();

        let name = stringify!($f);
        logic.$f($($arg, )*).expect_err(&format!("{} is not allowed in view calls", name))
    };
}

#[test]
fn test_prohibited_view_methods() {
    test_prohibited!(signer_account_id, 0);
    test_prohibited!(signer_account_pk, 0);
    test_prohibited!(predecessor_account_id, 0);
    test_prohibited!(prepaid_gas);
    test_prohibited!(used_gas);
    test_prohibited!(promise_create, 0, 0, 0, 0, 0, 0, 0, 0);
    test_prohibited!(promise_then, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    test_prohibited!(promise_and, 0, 0);
    test_prohibited!(promise_batch_create, 0, 0);
    test_prohibited!(promise_batch_then, 0, 0, 0);
    test_prohibited!(promise_batch_action_create_account, 0);
    test_prohibited!(promise_batch_action_deploy_contract, 0, 0, 0);
    test_prohibited!(promise_batch_action_function_call, 0, 0, 0, 0, 0, 0, 0);
    test_prohibited!(promise_batch_action_transfer, 0, 0);
    test_prohibited!(promise_batch_action_stake, 0, 0, 0, 0);
    test_prohibited!(promise_batch_action_add_key_with_full_access, 0, 0, 0, 0);
    test_prohibited!(promise_batch_action_add_key_with_function_call, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    test_prohibited!(promise_batch_action_delete_key, 0, 0, 0);
    test_prohibited!(promise_batch_action_delete_account, 0, 0, 0);
    test_prohibited!(promise_results_count);
    test_prohibited!(promise_result, 0, 0);
    test_prohibited!(promise_return, 0);
    test_prohibited!(storage_write, 0, 0, 0, 0, 0);
    test_prohibited!(storage_remove, 0, 0, 0);
}

#[test]
fn test_allowed_view_method() {
    let mut logic_builder = VMLogicBuilder::view();
    let mut logic = logic_builder.build();
    assert_eq!(logic.block_index().unwrap(), logic_builder.context.block_height);
}
