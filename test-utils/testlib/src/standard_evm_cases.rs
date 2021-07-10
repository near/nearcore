use crate::node::Node;
use crate::runtime_utils::{alice_account, bob_account, evm_account};
use crate::user::User;
use borsh::BorshSerialize;
use ethabi_contract::use_contract;
use ethereum_types::{Address, U256};
use near_evm_runner::types::WithdrawArgs;
use near_evm_runner::utils::{
    address_from_arr, encode_call_function_args, encode_view_call_function_args, u256_to_arr,
};
use near_primitives::errors::{ActionError, ActionErrorKind, ContractCallError};
use near_primitives::views::FinalExecutionStatus;

use_contract!(cryptozombies, "../../runtime/near-evm-runner/tests/build/ZombieOwnership.abi");
use_contract!(precompiles, "../../runtime/near-evm-runner/tests/build/StandardPrecompiles.abi");
use_contract!(fibonacci, "../../runtime/near-evm-runner/tests/build/Fibonacci.abi");
use_contract!(inf_loop, "../../runtime/near-evm-runner/tests/build/Loop.abi");

lazy_static_include::lazy_static_include_bytes! {
    ZOMBIE_OWNERSHIP_BIN => "../../runtime/near-evm-runner/tests/build/ZombieOwnership.bin",
    FIBONACCI_BIN => "../../runtime/near-evm-runner/tests/build/Fibonacci.bin",
    LOOP_BIN => "../../runtime/near-evm-runner/tests/build/Loop.bin",
    STANDARD_PRECOMPILES_BIN => "../../runtime/near-evm-runner/tests/build/StandardPrecompiles.bin",
}

/// Deploy the "CryptoZombies" contract (derived from
/// https://cryptozombies.io/en/course/) to the EVM.
fn deploy_zombie_attack_contract(node: impl Node) -> Address {
    let node_user = node.user();
    let bytes = hex::decode(ZOMBIE_OWNERSHIP_BIN.to_vec()).unwrap();
    let contract_id = node_user
        .function_call(alice_account(), evm_account(), "deploy_code", bytes, 10u64.pow(14), 10)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();

    let result = node_user.view_call(&evm_account(), "get_balance", &contract_id).unwrap();
    assert_eq!(result.result, u256_to_arr(&U256::from(10)).to_vec());

    address_from_arr(&contract_id)
}

/// Deploy the "Fibonacci" contract to the EVM and attach a boat load of gas.
/// Source: https://github.com/web3j/web3j/blob/master/codegen/src/test/resources/solidity/fibonacci/Fibonacci.sol
fn deploy_fibonacci_contract(node: impl Node) -> Address {
    let node_user = node.user();
    let bytes = hex::decode(FIBONACCI_BIN.to_vec()).unwrap();
    let contract_id = node_user
        .function_call(alice_account(), evm_account(), "deploy_code", bytes, 10u64.pow(14), 0)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();

    address_from_arr(&contract_id)
}

/// Tests infinite loop gas limit.
pub fn test_evm_infinite_loop_gas_limit(node: impl Node) {
    let node_user = node.user();
    let bytes = hex::decode(LOOP_BIN.to_vec()).unwrap();
    let contract_id = node_user
        .function_call(alice_account(), evm_account(), "deploy_code", bytes, 10u64.pow(14), 0)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();

    let contract_id = address_from_arr(&contract_id);

    let (input, _decoder) = inf_loop::functions::run::call();
    let args = encode_call_function_args(contract_id, input);
    let status = node_user
        .function_call(alice_account(), evm_account(), "call", args.clone(), 300 * 10u64.pow(12), 0)
        .unwrap()
        .status;
    assert_eq!(
        status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::FunctionCallError(
                    ContractCallError::ExecutionError { msg: "EVM: OutOfGas".to_string() }.into()
                )
            }
            .into()
        )
    );
}

/// Tests deep Fibonacci gas limit.
pub fn test_evm_fibonacci_gas_limit(node: impl Node) {
    let node_user = node.user();
    let contract_id = deploy_fibonacci_contract(node);

    let (input, _decoder) = fibonacci::functions::fibonacci::call(U256::from(20));
    let args = encode_call_function_args(contract_id, input);
    let status = node_user
        .function_call(alice_account(), evm_account(), "call", args.clone(), 300 * 10u64.pow(12), 0)
        .unwrap()
        .status;
    assert_eq!(
        status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::FunctionCallError(
                    ContractCallError::ExecutionError { msg: "EVM: OutOfGas".to_string() }.into()
                )
            }
            .into()
        )
    );
}

/// Tests Fibonacci 16.
pub fn test_evm_fibonacci_16(node: impl Node) {
    let node_user = node.user();
    let contract_id = deploy_fibonacci_contract(node);

    let (input, _decoder) = fibonacci::functions::fibonacci::call(U256::from(16));
    // sender, to, attached amount, args
    let args = encode_call_function_args(contract_id, input);
    let bytes = node_user
        .function_call(alice_account(), evm_account(), "call", args.clone(), 100 * 10u64.pow(12), 0)
        .unwrap();
    println!("{:?}", bytes);
    let bytes = bytes.status.as_success_decoded().unwrap();
    let res = fibonacci::functions::fibonacci::decode_output(&bytes).unwrap();
    assert_eq!(res, U256::from(987));
}

/// Tests for deploying the "CryptoZombies" contract (derived from
/// https://cryptozombies.io/en/course/) to the EVM.
pub fn test_evm_deploy_call(node: impl Node) {
    let node_user = node.user();
    let contract_id = deploy_zombie_attack_contract(node);

    let (input, _decoder) = cryptozombies::functions::create_random_zombie::call("test");
    let args = encode_call_function_args(contract_id, input);
    assert_eq!(
        node_user
            .function_call(alice_account(), evm_account(), "call", args, 10u64.pow(14), 0)
            .unwrap()
            .status
            .as_success_decoded()
            .unwrap(),
        Vec::<u8>::new()
    );

    let alice_address = near_evm_runner::utils::near_account_id_to_evm_address(&alice_account());
    let (input, _decoder) = cryptozombies::functions::get_zombies_by_owner::call(
        near_evm_runner::utils::near_account_id_to_evm_address(&alice_account()),
    );
    // sender, to, attached amount, args
    let args = encode_view_call_function_args(alice_address, contract_id, U256::zero(), input);
    let bytes = node_user
        .function_call(alice_account(), evm_account(), "view", args.clone(), 10u64.pow(14), 0)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();
    let res = cryptozombies::functions::get_zombies_by_owner::decode_output(&bytes).unwrap();
    assert_eq!(res, vec![U256::from(0)]);

    let result = node_user.view_call(&evm_account(), "view", &args).unwrap();
    let res =
        cryptozombies::functions::get_zombies_by_owner::decode_output(&result.result).unwrap();
    assert_eq!(res, vec![U256::from(0)]);

    let result = node_user.view_call(&evm_account(), "get_balance", &contract_id.0).unwrap();
    assert_eq!(U256::from_big_endian(&result.result), U256::from(10));

    assert!(node_user
        .function_call(
            alice_account(),
            evm_account(),
            "deposit",
            alice_address.0.to_vec(),
            10u64.pow(14),
            1000,
        )
        .unwrap()
        .status
        .as_success()
        .is_some());

    let result = node_user.view_call(&evm_account(), "get_balance", &alice_address.0).unwrap();
    assert_eq!(U256::from_big_endian(&result.result), U256::from(1000));

    let result = node_user
        .function_call(
            alice_account(),
            evm_account(),
            "withdraw",
            WithdrawArgs { account_id: alice_account(), amount: U256::from(10).into() }
                .try_to_vec()
                .unwrap(),
            10u64.pow(14),
            0,
        )
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();
    assert_eq!(result.len(), 0);
}

fn alice_is_owner(node_user: &Box<dyn User>, contract_id: Address) {
    let (input, _decoder) = cryptozombies::functions::owner::call();
    let alice_address = near_evm_runner::utils::near_account_id_to_evm_address(&alice_account());
    let args = encode_view_call_function_args(alice_address, contract_id, U256::zero(), input);
    let bytes = node_user
        .function_call(alice_account(), evm_account(), "view", args.clone(), 10u64.pow(14), 0)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();
    let res = cryptozombies::functions::owner::decode_output(&bytes).unwrap();
    assert_eq!(res, alice_address);
}

/// Test ownership transfer functionality of the "CryptoZombies" contract.
pub fn test_evm_crypto_zombies_contract_ownership_transfer(node: impl Node) {
    let node_user = node.user();
    let contract_id = deploy_zombie_attack_contract(node);

    // Alice should be the owner of the contract now, let's verify that
    alice_is_owner(&node_user, contract_id);

    // transfer the contract ownership from Alice to Bob
    let bob_address = near_evm_runner::utils::near_account_id_to_evm_address(&bob_account());
    let (input, _decoder) = cryptozombies::functions::transfer_ownership::call(bob_address);
    let args = encode_call_function_args(contract_id, input);
    assert_eq!(
        node_user
            .function_call(alice_account(), evm_account(), "call", args, 10u64.pow(14), 0)
            .unwrap()
            .status
            .as_success_decoded()
            .unwrap(),
        Vec::<u8>::new()
    );

    // verify Bob is the new contract owner now
    let (input, _decoder) = cryptozombies::functions::owner::call();
    let alice_address = near_evm_runner::utils::near_account_id_to_evm_address(&alice_account());
    let args = encode_view_call_function_args(alice_address, contract_id, U256::zero(), input);
    let bytes = node_user
        .function_call(alice_account(), evm_account(), "view", args.clone(), 10u64.pow(14), 0)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();
    let res = cryptozombies::functions::owner::decode_output(&bytes).unwrap();
    assert_eq!(res, bob_address);
}

/// Create zombie.
fn create_zombie(node_user: &Box<dyn User>, contract_id: Address, name: &str) {
    let (input, _decoder) = cryptozombies::functions::create_random_zombie::call(name);
    let args = encode_call_function_args(contract_id, input);
    assert_eq!(
        node_user
            .function_call(alice_account(), evm_account(), "call", args, 10u64.pow(14), 0)
            .unwrap()
            .status
            .as_success_decoded()
            .unwrap(),
        Vec::<u8>::new()
    );
}

/// Test the level up functionality of the "CryptoZombies" contract.
pub fn test_evm_crypto_zombies_contract_level_up(node: impl Node) {
    let node_user = node.user();
    let contract_id = deploy_zombie_attack_contract(node);

    // create zombie
    create_zombie(&node_user, contract_id, "test");

    // level up the zombie
    let (input, _decoder) = cryptozombies::functions::level_up::call(U256::zero());
    let level_up_fee = 10u128.pow(15); // 0.001 Ether
    let args = encode_call_function_args(contract_id, input);
    assert_eq!(
        node_user
            .function_call(
                alice_account(),
                evm_account(),
                "call",
                args,
                10u64.pow(14),
                level_up_fee
            )
            .unwrap()
            .status
            .as_success_decoded()
            .unwrap(),
        Vec::<u8>::new()
    );

    // Alice withdraws payment(s) from contract
    let (input, _decoder) = cryptozombies::functions::withdraw::call();
    let args = encode_call_function_args(contract_id, input);
    assert_eq!(
        node_user
            .function_call(alice_account(), evm_account(), "call", args, 10u64.pow(14), 0)
            .unwrap()
            .status
            .as_success_decoded(),
        None
    );
}

/// Test transfering a ERC-721 token of the "CryptoZombies" contract.
pub fn test_evm_crypto_zombies_contract_transfer_erc721(node: impl Node) {
    let node_user = node.user();
    let contract_id = deploy_zombie_attack_contract(node);

    // create zombie
    create_zombie(&node_user, contract_id, "test");

    // transfer the zombie token ownership from Alice to Bob
    let alice_address = near_evm_runner::utils::near_account_id_to_evm_address(&alice_account());
    let bob_address = near_evm_runner::utils::near_account_id_to_evm_address(&bob_account());
    let (input, _decoder) =
        cryptozombies::functions::transfer_from::call(alice_address, bob_address, U256::zero());
    let args = encode_call_function_args(contract_id, input);
    assert_eq!(
        node_user
            .function_call(alice_account(), evm_account(), "call", args, 10u64.pow(14), 0)
            .unwrap()
            .status
            .as_success_decoded()
            .unwrap(),
        Vec::<u8>::new()
    );

    // verify Bob is the new zombie token owner now
    let (input, _decoder) = cryptozombies::functions::owner_of::call(U256::zero());
    let args = encode_view_call_function_args(alice_address, contract_id, U256::zero(), input);
    let bytes = node_user
        .function_call(alice_account(), evm_account(), "view", args.clone(), 10u64.pow(14), 0)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();
    let res = cryptozombies::functions::owner::decode_output(&bytes).unwrap();
    assert_eq!(res, bob_address);
}

pub fn test_evm_call_standard_precompiles(node: impl Node) {
    let node_user = node.user();
    let bytes = hex::decode(STANDARD_PRECOMPILES_BIN.to_vec()).unwrap();

    let contract_id = node_user
        .function_call(alice_account(), evm_account(), "deploy_code", bytes, 10u64.pow(14), 0)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();
    let contract_id = address_from_arr(&contract_id);

    let alice_address = near_evm_runner::utils::near_account_id_to_evm_address(&alice_account());

    let (input, _decoder) = precompiles::functions::test_all::call();
    let args = encode_view_call_function_args(alice_address, contract_id, U256::zero(), input);
    let bytes = node_user
        .function_call(alice_account(), evm_account(), "view", args.clone(), 10u64.pow(14), 0)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();
    let res = precompiles::functions::test_all::decode_output(&bytes).unwrap();
    assert_eq!(res, true);
}
