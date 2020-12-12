#[macro_use]
extern crate lazy_static_include;

use borsh::BorshSerialize;
use ethabi_contract::use_contract;
use ethereum_types::{Address, H256, U256};
use keccak_hash::keccak;

use near_crypto::{InMemorySigner, KeyType};
use near_evm_runner::types::{TransferArgs, WithdrawArgs};
use near_evm_runner::utils::{
    address_from_arr, address_to_vec, ecrecover_address, encode_address, encode_call_function_args,
    encode_string, encode_view_call_function_args, near_account_id_to_evm_address,
    near_erc721_domain, parse_meta_call, u256_to_arr,
};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_errors::{EvmError, VMLogicError};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::VMConfig;

use crate::utils::{
    accounts, create_context, encode_meta_call_function_args, public_key_to_address, setup,
    CHAIN_ID,
};
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
    let (input, _) = soltest::functions::deploy_new_guy::call(9);
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
    println!("input: {:?}", hex::encode(&input));
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
fn test_meta_call_sig_and_recover() {
    let (mut _fake_external, test_addr, _vm_config, _fees_config) = setup_and_deploy_test();
    let signer = InMemorySigner::from_seed("doesnt", KeyType::SECP256K1, "a");
    let signer_addr = public_key_to_address(signer.public_key.clone());
    let domain_separator = near_erc721_domain(U256::from(CHAIN_ID));

    let meta_tx = encode_meta_call_function_args(
        &signer,
        CHAIN_ID,
        U256::from(14),
        U256::from(6),
        Address::from_slice(&[0u8; 20]),
        test_addr.clone(),
        "adopt(uint256 petId)",
        u256_to_arr(&U256::from(9)).to_vec(),
    );

    // meta_tx[0..65] is eth-sig-util format signature
    // assert signature same as eth-sig-util, which also implies msg before sign (constructed by prepare_meta_call_args, follow eip-712) same
    assert_eq!(hex::encode(&meta_tx[0..65]), "29b88cd2fab58cfd0d05eacdabaab081257d62bdafe9153922025c8e8723352d61922acbb290bd1dba8f17f174d47cd5cc41480d19a82a0bff4d0b9b9441399b1c");
    let result = parse_meta_call(&domain_separator, &"evm".to_string(), meta_tx).unwrap();
    assert_eq!(result.sender, signer_addr);

    let meta_tx2 = encode_meta_call_function_args(
        &signer,
        CHAIN_ID,
        U256::from(14),
        U256::from(6),
        Address::from_slice(&[0u8; 20]),
        test_addr.clone(),
        // must not have trailing space after comma
        "adopt(uint256 petId,string petName)",
        vec![u256_to_arr(&U256::from(9)).to_vec(), encode_string("CapsLock")].concat(),
    );
    assert_eq!(hex::encode(&meta_tx2[0..65]), "8f5e467a71327b1f23330ff0918dd55ab61daf65b4726c1457c91982964a78ee47874a32b6e1b8479da60d3e17de891e3f8c4cbc9f269da06b232862f51b0ba51b");
    let result = parse_meta_call(&domain_separator, &"evm".to_string(), meta_tx2).unwrap();
    assert_eq!(result.sender, signer_addr);

    let meta_tx3 = encode_meta_call_function_args(
        &signer,
        CHAIN_ID,
        U256::from(14),
        U256::from(6),
        Address::from_slice(&[0u8; 20]),
        test_addr,
        "adopt(uint256 petId,PetObj petObject)PetObj(string petName,address owner)",
        vec![
            u256_to_arr(&U256::from(9)).to_vec(),
            keccak(
                &vec![
                    encode_string("PetObj(string petName,address owner)"),
                    encode_string("CapsLock"),
                    encode_address(Address::from_slice(
                        &hex::decode("0123456789012345678901234567890123456789").unwrap(),
                    )),
                ]
                .concat(),
            )
            .as_bytes()
            .to_vec(),
        ]
        .concat(),
    );
    assert_eq!(hex::encode(&meta_tx3[0..65]), "0a2af43c3efab7ce535a00125b2505823c3c3218bacab1546a3e569ec15ca4557352f16ebabeeaa066a239346d7870afd49bf6e0b7b5c0d398d5cf894f3bdc8f1c");
    let result = parse_meta_call(&domain_separator, &"evm".to_string(), meta_tx3).unwrap();
    assert_eq!(result.sender, signer_addr);
}

#[test]
fn test_ecrecover() {
    let msg2 =
        hex::decode("c1719db355fee5122b0625b9274ee6d385ced2f2a530d0de2edb77b541d52c3e").unwrap();
    let mut msg = [0u8; 32];
    msg.copy_from_slice(&msg2[..32]);

    let signature2 = hex::decode("c710c068462547d3d3c452a4abc14fd91f152357c21e667ad6ac67130e76e9a1501491aa4e9d35846bff49d9c77e913217031fdc44f1dc36271a4b7d637763d01b").unwrap();
    let mut signature: [u8; 65] = [0; 65];
    // This is a sig from eth-sig-util, last one byte already added 27
    signature[0] = signature2[64];
    signature[1..].copy_from_slice(&signature2[..64]);

    assert_eq!(
        ecrecover_address(&msg, &signature).0.to_vec(),
        hex::decode("3b748cf099f8068951f87331d1970bcafda8a4db").unwrap()
    );
}
