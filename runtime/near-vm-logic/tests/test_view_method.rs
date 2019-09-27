use crate::fixtures::get_context;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::{Config, HostError, VMLogic};

mod fixtures;

#[test]
fn test_prohibited_view_method() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], true);
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let mut logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    match logic.signer_account_id(0) {
        Err(HostError::ProhibitedInView(s)) => assert_eq!(s, "signer_account_id"),
        _ => panic!("Incorrect error for prohibited view method"),
    }
}

#[test]
fn test_allowed_view_method() {
    let mut ext = MockedExternal::default();
    let context = get_context(vec![], true);
    let block_index = context.block_index;
    let config = Config::default();
    let promise_results = vec![];
    let mut memory = MockedMemory::default();
    let logic = VMLogic::new(&mut ext, context, &config, &promise_results, &mut memory);
    assert_eq!(logic.block_index().unwrap(), block_index);
}
