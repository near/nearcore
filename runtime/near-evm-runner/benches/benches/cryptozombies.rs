use criterion::Criterion;
use ethabi_contract::use_contract;
use near_evm_runner::utils::{address_from_arr, encode_call_function_args};
use near_vm_logic::mocks::mock_external::MockedExternal;

use_contract!(cryptozombies, "tests/build/ZombieAttack.abi");

use super::evm_call;

pub fn create_random(c: &mut Criterion) {
    let mut ext = MockedExternal::new();
    let bytes = hex::decode(include_bytes!("../../tests/build/ZombieAttack.bin").to_vec()).unwrap();
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
    let bytes = hex::decode(include_bytes!("../../tests/build/ZombieAttack.bin").to_vec()).unwrap();
    let ext = MockedExternal::new();
    c.bench_function("deploy_code", |b| {
        b.iter(|| {
            let mut ext_copy = ext.clone();
            evm_call(&mut ext_copy, "alice", "deploy_code", bytes.clone(), false).0.unwrap()
        })
    });
}
