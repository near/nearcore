use criterion::Criterion;
use ethabi_contract::use_contract;
use ethereum_types::U256;
use near_evm_runner::utils::{address_from_arr, encode_call_function_args};
use near_primitives::types::AccountId;
use near_vm_logic::mocks::mock_external::MockedExternal;

use_contract!(cryptozombies, "tests/build/ZombieOwnership.abi");

use super::evm_call;

pub fn create_random(c: &mut Criterion) {
    let mut ext = MockedExternal::new();
    let bytes =
        hex::decode(include_bytes!("../../tests/build/ZombieOwnership.bin").to_vec()).unwrap();
    let outcome = evm_call(&mut ext, "alice", "deploy_code", bytes, false).0.unwrap();
    let contract_id = address_from_arr(&outcome.return_data.as_value().unwrap());

    let (input, _decoder) = cryptozombies::functions::create_random_zombie::call("test");
    let args = encode_call_function_args(contract_id, input);

    c.bench_function("create_random", |b| {
        b.iter(|| {
            let mut ext_copy = ext.clone();
            evm_call(&mut ext_copy, "alice", "call", args.clone(), false).0.unwrap()
        })
    });
}

pub fn deploy_code(c: &mut Criterion) {
    let bytes =
        hex::decode(include_bytes!("../../tests/build/ZombieOwnership.bin").to_vec()).unwrap();
    let ext = MockedExternal::new();
    c.bench_function("deploy_code", |b| {
        b.iter(|| {
            let mut ext_copy = ext.clone();
            evm_call(&mut ext_copy, "alice", "deploy_code", bytes.clone(), false).0.unwrap()
        })
    });
}

fn alice_account() -> AccountId {
    "alice.near".to_string()
}
fn bob_account() -> AccountId {
    "bob.near".to_string()
}

pub fn transfer_erc721(c: &mut Criterion) {
    let mut ext = MockedExternal::new();
    let bytes =
        hex::decode(include_bytes!("../../tests/build/ZombieOwnership.bin").to_vec()).unwrap();
    let outcome = evm_call(&mut ext, "alice", "deploy_code", bytes, false).0.unwrap();
    let contract_id = address_from_arr(&outcome.return_data.as_value().unwrap());

    let (input, _decoder) = cryptozombies::functions::create_random_zombie::call("test");
    let args_create = encode_call_function_args(contract_id, input);

    let alice_address = near_evm_runner::utils::near_account_id_to_evm_address(&alice_account());
    let bob_address = near_evm_runner::utils::near_account_id_to_evm_address(&bob_account());

    c.bench_function("transfer_erc721", |b| {
        // first zombie ID
        let mut zombie_id = U256::zero();
        b.iter(|| {
            // create zombie
            //let mut ext_copy = ext.clone();
            evm_call(&mut ext, "alice", "call", args_create.clone(), false).0.unwrap();

            // transfer ERC-721 zombie token
            let (input, _decoder) = cryptozombies::functions::transfer_from::call(
                alice_address,
                bob_address,
                zombie_id,
            );
            let args_transfer = encode_call_function_args(contract_id, input);
            let (_outcome, err) = evm_call(&mut ext, "alice", "call", args_transfer.clone(), false);
            if let Some(vm_err) = err {
                panic!("Problem transfering ERC-721 token: {:?}", vm_err);
            }

            // increase zombie ID
            zombie_id += U256::one();
        })
    });
}
