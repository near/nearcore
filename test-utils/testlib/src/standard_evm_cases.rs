use crate::node::Node;
use crate::runtime_utils::{alice_account, evm_account};
use borsh::BorshSerialize;
use ethabi_contract::use_contract;
use ethereum_types::U256;
use near_evm_runner::types::WithdrawArgs;
use near_evm_runner::utils::{
    address_from_arr, encode_call_function_args, encode_view_call_function_args, u256_to_arr,
};

use_contract!(cryptozombies, "../../runtime/near-evm-runner/tests/build/zombieAttack.abi");

pub fn test_evm_deploy_call(node: impl Node) {
    let node_user = node.user();
    let bytes = hex::decode(
        include_bytes!("../../../runtime/near-evm-runner/tests/build/zombieAttack.bin").to_vec(),
    )
    .unwrap();
    let contract_id = node_user
        .function_call(alice_account(), evm_account(), "deploy_code", bytes, 10u64.pow(14), 10)
        .unwrap()
        .status
        .as_success_decoded()
        .unwrap();

    let result = node_user.view_call(&evm_account(), "get_balance", &contract_id).unwrap();
    assert_eq!(result.result, u256_to_arr(&U256::from(10)).to_vec());

    let (input, _decoder) = cryptozombies::functions::create_random_zombie::call("test");
    let contract_id = address_from_arr(&contract_id);
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

pub fn test_sub_evm(node: impl Node) {
    let node_user = node.user();
    assert_eq!(
        node_user.view_account(&"sub.evm".to_string()).unwrap_err().to_string(),
        "account sub.evm does not exist while viewing"
    );
    assert_eq!(
        node_user
            .function_call(
                alice_account(),
                evm_account(),
                "create",
                b"sub.evm".to_vec(),
                10u64.pow(14),
                0,
            )
            .unwrap()
            .status
            .as_failure()
            .unwrap()
            .to_string(),
        "Action #0: EVM: InsufficientDeposit"
    );
    node_user
        .function_call(
            alice_account(),
            evm_account(),
            "create",
            b"sub.evm".to_vec(),
            10u64.pow(14),
            10u128.pow(27),
        )
        .unwrap()
        .status
        .as_success()
        .unwrap();
    assert_eq!(node_user.view_account(&"sub.evm".to_string()).unwrap().amount, 10u128.pow(27));
}
