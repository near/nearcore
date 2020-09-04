use near_evm_runner::EvmContext;
use near_vm_logic::mocks::mock_external::MockedExternal;

fn accounts(num: usize) -> String {
    ["evm", "alice"][num].to_string()
}

/// Test various invalid inputs to function calls.
#[test]
fn test_invalid_input() {
    let mut fake_external = MockedExternal::new();
    let mut context = EvmContext::new(&mut fake_external, accounts(1), 0, 0);
    assert!(context.get_nonce(vec![]).is_err());
    assert!(context.get_balance(vec![]).is_err());
    assert!(context.get_code(vec![]).is_err());
    assert!(context.get_storage_at(vec![]).is_err());
    assert!(context.view_call_function(vec![]).is_err());
    assert!(context.view_call_function(vec![0u8; 20]).is_err());
    assert!(context.call_function(vec![]).is_err());
    assert!(context.call_function(vec![0u8; 20]).is_err());
    assert!(context.deposit(vec![]).is_err());
    assert!(context.withdraw(vec![]).is_err());
    assert!(context.transfer(vec![]).is_err());
}
