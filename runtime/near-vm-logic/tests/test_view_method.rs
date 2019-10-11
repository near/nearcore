use crate::fixtures::get_context;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{Config, VMLogic};

mod fixtures;

macro_rules! test_prohibited {
    ($f: ident $(, $arg: expr )* ) => {
        let mut ext = MockedExternal::default();
        let context = get_context(vec![], true);
        let config = Config::default();
        let promise_results = vec![];
        let mut memory = MockedMemory::default();
        let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);

        let name = stringify!($f);
        logic.$f($($arg, )*).expect_err(&format!("{} is not allowed in view calls", name))
    };
}

#[test]
fn test_prohibited_view_methods() {
    test_prohibited!(signer_account_id, 0);
    test_prohibited!(signer_account_pk, 0);
    test_prohibited!(predecessor_account_id, 0);
    test_prohibited!(attached_deposit, 0);
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
}

#[test]
fn test_allowed_view_method() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], true);
    let block_index = context.block_index;
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    assert_eq!(logic.block_index().unwrap(), block_index);
}
