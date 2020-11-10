#[macro_use]
extern crate lazy_static_include;

use borsh::BorshSerialize;
use ethabi_contract::use_contract;
use ethereum_types::{Address, H256, U256};

use near_crypto::{InMemorySigner, KeyType};
use near_evm_runner::types::{TransferArgs, WithdrawArgs};
use near_evm_runner::utils::{
    address_from_arr, address_to_vec, encode_call_function_args, encode_view_call_function_args,
    near_account_id_to_evm_address, near_erc721_domain, parse_meta_call, u256_to_arr,
};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_errors::{EvmError, VMLogicError};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::VMConfig;

use crate::utils::{
    accounts, create_context, encode_meta_call_function_args, public_key_to_address, setup,
    CHAIN_ID,
};
use parity_bytes::ToPretty;

mod utils;

use_contract!(soltest, "tests/build/SolTests.abi");
use_contract!(subcontract, "tests/build/SubContract.abi");
use_contract!(create2factory, "tests/build/Create2Factory.abi");
use_contract!(selfdestruct, "tests/build/SelfDestruct.abi");

lazy_static_include_str!(TEST, "tests/build/SolTests.bin");
lazy_static_include_str!(FACTORY_TEST, "tests/build/Create2Factory.bin");
lazy_static_include_str!(DESTRUCT_TEST, "tests/build/SelfDestruct.bin");
lazy_static_include_str!(CONSTRUCTOR_TEST, "tests/build/ConstructorRevert.bin");

#[test]
fn test_funds_transfers() {
    let (mut fake_external, vm_config, fees_config) = setup();
    let context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    assert_eq!(
        context.get_balance(address_to_vec(&near_account_id_to_evm_address(&accounts(1)))).unwrap(),
        U256::from(0)
    );
    let mut context =
        create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 100);
    assert_eq!(
        context.deposit(address_to_vec(&near_account_id_to_evm_address(&accounts(1)))).unwrap(),
        U256::from(100)
    );
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    context
        .transfer(
            TransferArgs {
                address: near_account_id_to_evm_address(&accounts(2)).0,
                amount: u256_to_arr(&U256::from(50)),
            }
            .try_to_vec()
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        context.get_balance(address_to_vec(&near_account_id_to_evm_address(&accounts(2)))).unwrap(),
        U256::from(50)
    );
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(2), 0);
    context
        .withdraw(
            WithdrawArgs { account_id: accounts(2), amount: u256_to_arr(&U256::from(50)) }
                .try_to_vec()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        context.get_balance(address_to_vec(&near_account_id_to_evm_address(&accounts(2)))).unwrap(),
        U256::from(0)
    );
}

#[test]
fn test_deploy_with_nonce() {
    let (mut fake_external, vm_config, fees_config) = setup();
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    let address = near_account_id_to_evm_address(&accounts(1));
    assert_eq!(context.get_nonce(address.0.to_vec()).unwrap(), U256::from(0));
    let address1 = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();
    assert_eq!(context.get_nonce(address.0.to_vec()).unwrap(), U256::from(1));
    let address2 = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();
    assert_eq!(context.get_nonce(address.0.to_vec()).unwrap(), U256::from(2));
    assert_ne!(address1, address2);
}

#[test]
#[ignore]
fn test_failed_deploy_returns_error() {
    let (mut fake_external, vm_config, fees_config) = setup();
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    if let Err(VMLogicError::EvmError(EvmError::DeployFail(_))) =
        context.deploy_code(hex::decode(&CONSTRUCTOR_TEST).unwrap())
    {
    } else {
        panic!("Should fail");
    }
}

#[test]
fn test_internal_create() {
    let (mut fake_external, vm_config, fees_config) = setup();
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    let test_addr = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();
    assert_eq!(context.get_nonce(test_addr.0.to_vec()).unwrap(), U256::from(0));

    // This should increment the nonce of the deploying contract
    let (input, _) = soltest::functions::deploy_new_guy::call(8);
    let raw = context.call_function(encode_call_function_args(test_addr, input)).unwrap();
    assert_eq!(context.get_nonce(test_addr.0.to_vec()).unwrap(), U256::from(1));

    let sub_addr = address_from_arr(&raw[12..32]);
    let (new_input, _) = subcontract::functions::a_number::call();
    let new_raw = context.call_function(encode_call_function_args(sub_addr, new_input)).unwrap();
    let output = subcontract::functions::a_number::decode_output(&new_raw).unwrap();
    assert_eq!(output, U256::from(8));
}

#[test]
fn test_precompiles() {
    let (mut fake_external, vm_config, fees_config) = setup();
    let mut context =
        create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 100);
    let test_addr = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();

    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    let (input, _) = soltest::functions::precompile_test::call();
    let raw = context.call_function(encode_call_function_args(test_addr, input)).unwrap();
    assert_eq!(raw.len(), 0);
}

fn setup_and_deploy_test() -> (MockedExternal, Address, VMConfig, RuntimeFeesConfig) {
    let (mut fake_external, vm_config, fees_config) = setup();
    let mut context =
        create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 100);
    let test_addr = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();
    assert_eq!(context.get_balance(test_addr.0.to_vec()).unwrap(), U256::from(100));
    (fake_external, test_addr, vm_config, fees_config)
}

#[test]
fn test_deploy_and_transfer() {
    let (mut fake_external, test_addr, vm_config, fees_config) = setup_and_deploy_test();

    // This should increment the nonce of the deploying contract.
    // There is 100 attached to this that should be passed through.
    let (input, _) = soltest::functions::deploy_new_guy::call(8);
    let mut context =
        create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 100);
    let raw = context.call_function(encode_call_function_args(test_addr, input)).unwrap();
    assert!(context.logs.len() > 0);

    // The sub_addr should have been transferred 100 yoctoN.
    let sub_addr = raw[12..32].to_vec();
    assert_eq!(context.get_balance(test_addr.0.to_vec()).unwrap(), U256::from(100));
    assert_eq!(context.get_balance(sub_addr).unwrap(), U256::from(100));
}

#[test]
fn test_deploy_with_value() {
    let (mut fake_external, test_addr, vm_config, fees_config) = setup_and_deploy_test();

    // This should increment the nonce of the deploying contract
    // There is 100 attached to this that should be passed through
    let (input, _) = soltest::functions::pay_new_guy::call(8);
    let mut context =
        create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 100);
    let raw = context.call_function(encode_call_function_args(test_addr, input)).unwrap();

    // The sub_addr should have been transferred 100 tokens.
    let sub_addr = raw[12..32].to_vec();
    assert_eq!(context.get_balance(test_addr.0.to_vec()).unwrap(), U256::from(100));
    assert_eq!(context.get_balance(sub_addr).unwrap(), U256::from(100));
}

#[test]
fn test_contract_to_eoa_transfer() {
    let (mut fake_external, test_addr, vm_config, fees_config) = setup_and_deploy_test();

    let (input, _) = soltest::functions::return_some_funds::call();
    let mut context =
        create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 100);
    let raw = context.call_function(encode_call_function_args(test_addr, input)).unwrap();

    let sender_addr = raw[12..32].to_vec();
    assert_eq!(context.get_balance(test_addr.0.to_vec()).unwrap(), U256::from(150));
    assert_eq!(context.get_balance(sender_addr).unwrap(), U256::from(50));
}

#[test]
fn test_get_code() {
    let (mut fake_external, test_addr, vm_config, fees_config) = setup_and_deploy_test();
    let context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);

    assert!(context.get_code(test_addr.0.to_vec()).unwrap().len() > 1500); // contract code should roughly be over length 1500
    assert_eq!(context.get_code(vec![0u8; 20]).unwrap().len(), 0);
}

#[test]
fn test_view_call() {
    let (mut fake_external, test_addr, vm_config, fees_config) = setup_and_deploy_test();

    // This should NOT increment the nonce of the deploying contract
    // And NO CODE should be deployed
    let (input, _) = soltest::functions::deploy_new_guy::call(8);
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    let raw = context
        .view_call_function(encode_view_call_function_args(
            test_addr,
            test_addr,
            U256::from(0),
            input,
        ))
        .unwrap();
    assert_eq!(context.get_nonce(test_addr.0.to_vec()).unwrap(), U256::from(0));

    let sub_addr = raw[12..32].to_vec();
    assert_eq!(context.get_code(sub_addr).unwrap().len(), 0);

    let (input, _) = soltest::functions::return_some_funds::call();
    let raw = context
        .view_call_function(encode_view_call_function_args(
            test_addr,
            test_addr,
            U256::from(10u128.pow(27)),
            input,
        ))
        .unwrap();
    assert_eq!(raw[12..32], test_addr.0);
}

#[test]
fn test_solidity_accurate_storage_on_selfdestruct() {
    let (mut fake_external, vm_config, fees_config) = setup();
    let mut context =
        create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 100);
    assert_eq!(
        context.deposit(near_account_id_to_evm_address(&accounts(1)).0.to_vec()).unwrap(),
        U256::from(100)
    );

    // Deploy CREATE2 Factory
    let mut context = create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 0);
    let factory_addr = context.deploy_code(hex::decode(&FACTORY_TEST).unwrap()).unwrap();

    // Deploy + SelfDestruct in one transaction
    let salt = H256([0u8; 32]);
    let destruct_code = hex::decode(&DESTRUCT_TEST).unwrap();
    let input = create2factory::functions::test_double_deploy::call(salt, destruct_code.clone()).0;
    let raw = context.call_function(encode_call_function_args(factory_addr, input)).unwrap();
    assert!(create2factory::functions::test_double_deploy::decode_output(&raw).unwrap());
}

#[test]
fn test_meta_call() {
    let (mut fake_external, test_addr, vm_config, fees_config) = setup_and_deploy_test();
    let signer = InMemorySigner::from_random("doesnt".to_string(), KeyType::SECP256K1);
    let mut context =
        create_context(&mut fake_external, &vm_config, &fees_config, accounts(1), 100);
    let meta_tx = encode_meta_call_function_args(
        &signer,
        CHAIN_ID,
        U256::from(0),
        U256::from(0),
        Address::from_slice(&[0u8; 20]),
        test_addr,
        "returnSomeFunds()",
        vec![],
    );
    let _ = context.meta_call_function(meta_tx.clone()).unwrap();
    let signer_addr = public_key_to_address(signer.public_key);
    assert_eq!(context.get_balance(test_addr.0.to_vec()).unwrap(), U256::from(150));
    assert_eq!(context.get_balance(signer_addr.0.to_vec()).unwrap(), U256::from(50));
    assert_eq!(
        context.meta_call_function(meta_tx).unwrap_err().to_string(),
        "EvmError(InvalidNonce)"
    );
}

#[test]
#[ignore]
fn test_meta_call_sig_recover() {
    let meta_tx = [
        // signature: 65 bytes
        hex::decode("1cb6f28f29524cf3ae5ce49f364b5ad798af5dd8ec3563744dc62792735ce5e222285df1e91c416e430d0a38ea3b51d6677e337e1b0684d7618f5a00a26a2ee21c").unwrap(),
        // nonce: 14
        u256_to_arr(&U256::from(14)).to_vec(),
        // fee amount: 6
        u256_to_arr(&U256::from(6)).to_vec(),
        // fee token: 0x0
        vec![0; 20],
        // contract: address,
        hex::decode("Ed2a1b3Fa739DAbBf8c07a059dE1333D20e8b482").unwrap(),
        // contract method: length 1 byte + bytes for the name.
        vec![14],
        b"adopt(uint256)".to_vec(),
        // arguments
        u256_to_arr(&U256::from(9)).to_vec(),
    ].concat();
    let domain_separator = near_erc721_domain(U256::from(CHAIN_ID));
    let result = parse_meta_call(&domain_separator, &"evm".to_string(), meta_tx).unwrap();
    assert_eq!(result.sender.to_hex(), "2941022347348828A24a5ff33c775D67691681e9");
}
