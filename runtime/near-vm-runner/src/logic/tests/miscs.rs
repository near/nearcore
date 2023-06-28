use crate::logic::tests::helpers::*;
use crate::logic::tests::vm_logic_builder::VMLogicBuilder;
use crate::logic::ExtCosts;
use crate::logic::HostError;
use crate::map;
use hex::FromHex;
use serde::de::Error;
use serde_json::from_slice;
use std::{fmt::Display, fs};

#[test]
fn test_sha256() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let data = logic.internal_mem_write(b"tesdsst");

    logic.sha256(data.len, data.ptr, 0).unwrap();
    logic.assert_read_register(
        &[
            18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112,
            92, 255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
        ],
        0,
    );
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: data.len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 32,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 32,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 32,
        ExtCosts::sha256_base: 1,
        ExtCosts::sha256_byte: data.len,
    });
}

#[test]
fn test_keccak256() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();

    let data = logic.internal_mem_write(b"tesdsst");
    logic.keccak256(data.len, data.ptr, 0).unwrap();
    logic.assert_read_register(
        &[
            104, 110, 58, 122, 230, 181, 215, 145, 231, 229, 49, 162, 123, 167, 177, 58, 26, 142,
            129, 173, 7, 37, 9, 26, 233, 115, 64, 102, 61, 85, 10, 159,
        ],
        0,
    );
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: data.len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 32,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 32,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 32,
        ExtCosts::keccak256_base: 1,
        ExtCosts::keccak256_byte: data.len,
    });
}

#[test]
fn test_keccak512() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();

    let data = logic.internal_mem_write(b"tesdsst");
    logic.keccak512(data.len, data.ptr, 0).unwrap();
    logic.assert_read_register(
        &[
            55, 134, 96, 137, 168, 122, 187, 95, 67, 76, 18, 122, 146, 11, 225, 106, 117, 194, 154,
            157, 48, 160, 90, 146, 104, 209, 118, 126, 222, 230, 200, 125, 48, 73, 197, 236, 123,
            173, 192, 197, 90, 153, 167, 121, 100, 88, 209, 240, 137, 86, 239, 41, 87, 128, 219,
            249, 136, 203, 220, 109, 46, 168, 234, 190,
        ],
        0,
    );
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: data.len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 64,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 64,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 64,
        ExtCosts::keccak512_base: 1,
        ExtCosts::keccak512_byte: data.len,
    });
}

#[test]
fn test_ripemd160() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();

    let data = logic.internal_mem_write(b"tesdsst");
    logic.ripemd160(data.len, data.ptr, 0).unwrap();
    logic.assert_read_register(
        &[21, 102, 156, 115, 232, 3, 58, 215, 35, 84, 129, 30, 143, 86, 212, 104, 70, 97, 14, 225],
        0,
    );
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::read_memory_base: 1,
        ExtCosts::read_memory_byte: data.len,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 20,
        ExtCosts::read_register_base: 1,
        ExtCosts::read_register_byte: 20,
        ExtCosts::write_register_base: 1,
        ExtCosts::write_register_byte: 20,
        ExtCosts::ripemd160_base: 1,
        ExtCosts::ripemd160_block: 1,
    });
}

#[derive(serde::Deserialize)]
struct EcrecoverTest {
    #[serde(with = "hex::serde")]
    m: [u8; 32],
    v: u8,
    #[serde(with = "hex::serde")]
    sig: [u8; 64],
    mc: bool,
    #[serde(deserialize_with = "deserialize_option_hex")]
    res: Option<[u8; 64]>,
}

fn deserialize_option_hex<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: FromHex,
    <T as FromHex>::Error: Display,
{
    serde::Deserialize::deserialize(deserializer)
        .map(|v: Option<&str>| v.map(FromHex::from_hex).transpose().map_err(Error::custom))
        .and_then(|v| v)
}

#[test]
fn test_ecrecover() {
    for EcrecoverTest { m, v, sig, mc, res } in from_slice::<'_, Vec<_>>(
        fs::read("src/logic/tests/ecrecover-tests.json").unwrap().as_slice(),
    )
    .unwrap()
    {
        let mut logic_builder = VMLogicBuilder::default();
        let mut logic = logic_builder.build();
        let m = logic.internal_mem_write(&m);
        let sig = logic.internal_mem_write(&sig);

        let b = logic.ecrecover(m.len, m.ptr, sig.len, sig.ptr, v as _, mc as _, 1).unwrap();
        assert_eq!(b, res.is_some() as u64);

        if let Some(res) = res {
            assert_costs(map! {
                ExtCosts::read_memory_base: 2,
                ExtCosts::read_memory_byte: 96,
                ExtCosts::write_register_base: 1,
                ExtCosts::write_register_byte: 64,
                ExtCosts::ecrecover_base: 1,
            });
            logic.assert_read_register(&res, 1);
        } else {
            assert_costs(map! {
                ExtCosts::read_memory_base: 2,
                ExtCosts::read_memory_byte: 96,
                ExtCosts::ecrecover_base: 1,
            });
        }

        reset_costs_counter();
    }
}

#[test]
fn test_hash256_register() {
    let mut logic_builder = VMLogicBuilder::default();
    let mut logic = logic_builder.build();
    let data = b"tesdsst";
    logic.wrapped_internal_write_register(1, data).unwrap();

    logic.sha256(u64::MAX, 1, 0).unwrap();
    logic.assert_read_register(
        &[
            18, 176, 115, 156, 45, 100, 241, 132, 180, 134, 77, 42, 105, 111, 199, 127, 118, 112,
            92, 255, 88, 43, 83, 147, 122, 55, 26, 36, 42, 156, 160, 158,
        ],
        0,
    );

    let len = data.len() as u64;
    assert_costs(map! {
        ExtCosts::base: 1,
        ExtCosts::write_memory_base: 1,
        ExtCosts::write_memory_byte: 32,
        ExtCosts::read_register_base: 2,
        ExtCosts::read_register_byte: 32 + len,
        ExtCosts::write_register_base: 2,
        ExtCosts::write_register_byte: 32 + len,
        ExtCosts::sha256_base: 1,
        ExtCosts::sha256_byte: len,
    });
}

#[test]
fn test_key_length_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let limit = 1024;
    logic_builder.config.limit_config.max_length_storage_key = limit;
    let mut logic = logic_builder.build();

    // Under the limit. Valid calls.
    let key = crate::logic::MemSlice { ptr: 0, len: limit };
    let val = crate::logic::MemSlice { ptr: 0, len: 5 };
    logic
        .storage_has_key(key.len, key.ptr)
        .expect("storage_has_key: key length is under the limit");
    logic
        .storage_write(key.len, key.ptr, val.len, val.ptr, 0)
        .expect("storage_write: key length is under the limit");
    logic.storage_read(key.len, key.ptr, 0).expect("storage_read: key length is under the limit");
    logic
        .storage_remove(key.len, key.ptr, 0)
        .expect("storage_remove: key length is under the limit");

    // Over the limit. Invalid calls.
    let key = crate::logic::MemSlice { ptr: 0, len: limit + 1 };
    assert_eq!(
        logic.storage_has_key(key.len, key.ptr),
        Err(HostError::KeyLengthExceeded { length: key.len, limit }.into())
    );
    assert_eq!(
        logic.storage_write(key.len, key.ptr, val.len, val.ptr, 0),
        Err(HostError::KeyLengthExceeded { length: key.len, limit }.into())
    );
    assert_eq!(
        logic.storage_read(key.len, key.ptr, 0),
        Err(HostError::KeyLengthExceeded { length: key.len, limit }.into())
    );
    assert_eq!(
        logic.storage_remove(key.len, key.ptr, 0),
        Err(HostError::KeyLengthExceeded { length: key.len, limit }.into())
    );
}

#[test]
fn test_value_length_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let limit = 1024;
    logic_builder.config.limit_config.max_length_storage_value = limit;
    let mut logic = logic_builder.build();
    let key = logic.internal_mem_write(b"hello");

    logic
        .storage_write(key.len, key.ptr, limit / 2, 0, 0)
        .expect("Value length doesn’t exceed the limit");
    logic
        .storage_write(key.len, key.ptr, limit, 0, 0)
        .expect("Value length doesn’t exceed the limit");
    assert_eq!(
        logic.storage_write(key.len, key.ptr, limit + 1, 0, 0),
        Err(HostError::ValueLengthExceeded { length: limit + 1, limit }.into())
    );
}

#[test]
fn test_num_promises() {
    let mut logic_builder = VMLogicBuilder::default();
    let num_promises = 10;
    logic_builder.config.limit_config.max_promises_per_function_call_action = num_promises;
    let mut logic = logic_builder.build();
    let account_id = logic.internal_mem_write(b"alice");
    for _ in 0..num_promises {
        logic
            .promise_batch_create(account_id.len, account_id.ptr)
            .expect("Number of promises is under the limit");
    }
    assert_eq!(
        logic.promise_batch_create(account_id.len, account_id.ptr),
        Err(HostError::NumberPromisesExceeded {
            number_of_promises: num_promises + 1,
            limit: num_promises
        }
        .into())
    );
}

#[test]
fn test_num_joined_promises() {
    let mut logic_builder = VMLogicBuilder::default();
    let num_deps = 10;
    logic_builder.config.limit_config.max_number_input_data_dependencies = num_deps;
    let mut logic = logic_builder.build();
    let account_id = logic.internal_mem_write(b"alice");
    let promise_id = logic
        .promise_batch_create(account_id.len, account_id.ptr)
        .expect("Number of promises is under the limit");
    let promises =
        logic.internal_mem_write(&promise_id.to_le_bytes().repeat(num_deps as usize + 1));
    for num in 0..num_deps {
        logic.promise_and(promises.ptr, num).expect("Number of joined promises is under the limit");
    }
    assert_eq!(
        logic.promise_and(promises.ptr, num_deps + 1),
        Err(HostError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: num_deps + 1,
            limit: num_deps,
        }
        .into())
    );
}

#[test]
fn test_num_input_dependencies_recursive_join() {
    let mut logic_builder = VMLogicBuilder::default();
    let num_steps = 10;
    logic_builder.config.limit_config.max_number_input_data_dependencies = 1 << num_steps;
    let mut logic = logic_builder.build();
    let account_id = logic.internal_mem_write(b"alice");
    let original_promise_id = logic
        .promise_batch_create(account_id.len, account_id.ptr)
        .expect("Number of promises is under the limit");
    let mut promise_id = original_promise_id;
    for _ in 1..num_steps {
        let promises_ptr = logic.internal_mem_write(&promise_id.to_le_bytes()).ptr;
        logic.internal_mem_write(&promise_id.to_le_bytes());
        promise_id = logic
            .promise_and(promises_ptr, 2)
            .expect("Number of joined promises is under the limit");
    }
    // The length of joined promises is exactly the limit (1024).
    let promises_ptr = logic.internal_mem_write(&promise_id.to_le_bytes()).ptr;
    logic.internal_mem_write(&promise_id.to_le_bytes());
    logic.promise_and(promises_ptr, 2).expect("Number of joined promises is under the limit");

    // The length of joined promises exceeding the limit by 1 (total 1025).
    let promises_ptr = logic.internal_mem_write(&promise_id.to_le_bytes()).ptr;
    logic.internal_mem_write(&promise_id.to_le_bytes());
    logic.internal_mem_write(&original_promise_id.to_le_bytes());
    assert_eq!(
        logic.promise_and(promises_ptr, 3),
        Err(HostError::NumberInputDataDependenciesExceeded {
            number_of_input_data_dependencies: logic_builder
                .config
                .limit_config
                .max_number_input_data_dependencies
                + 1,
            limit: logic_builder.config.limit_config.max_number_input_data_dependencies,
        }
        .into())
    );
}

#[test]
fn test_return_value_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let limit = 1024;
    logic_builder.config.limit_config.max_length_returned_data = limit;
    let mut logic = logic_builder.build();

    logic.value_return(limit, 0).expect("Returned value length is under the limit");
    assert_eq!(
        logic.value_return(limit + 1, 0),
        Err(HostError::ReturnedValueLengthExceeded { length: limit + 1, limit }.into())
    );
}

#[test]
fn test_contract_size_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let limit = 1024;
    logic_builder.config.limit_config.max_contract_size = limit;
    let mut logic = logic_builder.build();

    let account_id = logic.internal_mem_write(b"alice");

    let promise_id = logic
        .promise_batch_create(account_id.len, account_id.ptr)
        .expect("Number of promises is under the limit");
    logic
        .promise_batch_action_deploy_contract(promise_id, limit, 0)
        .expect("The length of the contract code is under the limit");
    assert_eq!(
        logic.promise_batch_action_deploy_contract(promise_id, limit + 1, 0),
        Err(HostError::ContractSizeExceeded { size: limit + 1, limit }.into())
    );
}
