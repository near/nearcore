mod bindings {
    use crate::Component;

    wit_bindgen::generate!({
        inline: "
package near:test;

world test {
    import near:nearcore/runtime@0.1.0;
    export ext-storage-usage: func();
    export ext-block-index: func();
    export ext-block-timestamp: func();
    export ext-prepaid-gas: func();
    export ext-random-seed: func();
    export ext-predecessor-account-id: func();
    export ext-signer-pk: func();
    export ext-signer-id: func();
    export ext-account-id: func();
    export ext-account-balance: func();
    export ext-attached-deposit: func();
    export ext-validator-total-stake: func();
    export ext-sha256: func();
    export ext-used-gas: func();
    export ext-validator-stake: func();
    export write-key-value: func();
    export write-block-height: func();
    export write-random-value: func();
    export write-one-megabyte: func();
    export read-n-megabytes: func();
    export read-value: func();
    export out-of-memory: func();
    export sanity-check: func();
}",
        ownership: Borrowing { duplicate_if_necessary: true },
        generate_all,
    });

    export!(Component);
}

use bindings::near::nearcore::runtime::*;
use bindings::Guest;
use core::array;

impl From<u128> for U128 {
    fn from(value: u128) -> Self {
        Self { lo: value as _, hi: (value >> 64) as _ }
    }
}

impl From<U128> for u128 {
    fn from(U128 { lo, hi }: U128) -> Self {
        (u128::from(hi) << 64) | u128::from(lo)
    }
}

pub fn from_base64(s: &str) -> Vec<u8> {
    let engine = &base64::engine::general_purpose::STANDARD;
    base64::Engine::decode(engine, s).unwrap()
}

pub struct Component;

impl Guest for Component {
    fn ext_storage_usage() {
        let n = storage_usage();
        value_return(ValueOrRegister::Value(&n.to_le_bytes()))
    }

    fn ext_block_index() {
        let n = block_height();
        value_return(ValueOrRegister::Value(&n.to_le_bytes()))
    }

    fn ext_block_timestamp() {
        let n = block_timestamp();
        value_return(ValueOrRegister::Value(&n.to_le_bytes()))
    }

    fn ext_prepaid_gas() {
        let n = prepaid_gas();
        value_return(ValueOrRegister::Value(&n.to_le_bytes()))
    }

    fn ext_random_seed() {
        random_seed(0);
        let data = read_register(0);
        value_return(ValueOrRegister::Value(&data))
    }

    fn ext_predecessor_account_id() {
        predecessor_account_id(0);
        let data = read_register(0);
        value_return(ValueOrRegister::Value(&data))
    }

    fn ext_signer_pk() {
        signer_account_pk(0);
        let data = read_register(0);
        value_return(ValueOrRegister::Value(&data))
    }

    fn ext_signer_id() {
        signer_account_id(0);
        let data = read_register(0);
        value_return(ValueOrRegister::Value(&data))
    }

    fn ext_account_id() {
        current_account_id(0);
        let data = read_register(0);
        value_return(ValueOrRegister::Value(&data))
    }

    fn ext_account_balance() {
        let n = account_balance();
        value_return(ValueOrRegister::Value(&u128::from(n).to_le_bytes()))
    }

    fn ext_attached_deposit() {
        let n = attached_deposit();
        value_return(ValueOrRegister::Value(&u128::from(n).to_le_bytes()))
    }

    fn ext_validator_total_stake() {
        let n = validator_total_stake();
        value_return(ValueOrRegister::Value(&u128::from(n).to_le_bytes()))
    }

    fn ext_sha256() {
        input(0);
        let bytes = read_register(0);
        sha256(ValueOrRegister::Value(&bytes), 0);
        let result = read_register(0);
        value_return(ValueOrRegister::Value(&result));
    }

    fn ext_used_gas() {
        let initial_used_gas = used_gas();
        let mut a = 1;
        let mut b = 1;
        for _ in 0..30 {
            let c = a + b;
            a = b;
            b = c;
        }
        assert_eq!(a, 1346269);
        let gas = used_gas() - initial_used_gas;
        let result = gas.to_le_bytes();
        value_return(ValueOrRegister::Value(&result));
    }

    fn ext_validator_stake() {
        input(0);
        let account_id = read_register(0);
        let result = validator_stake(ValueOrRegister::Value(&account_id));
        value_return(ValueOrRegister::Value(&u128::from(result).to_le_bytes()));
    }
    /// Write key-value pair into storage.
    /// Input is the byte array where the value is `u64` represented by last 8 bytes and key is represented by the first
    /// `register_len(0) - 8` bytes.
    fn write_key_value() {
        input(0);
        let data_len = register_len(0).unwrap() as usize;
        let value_len = size_of::<u64>();
        let data = read_register(0);
        assert_eq!(data.len(), data_len);

        let key = &data[0..data_len - value_len];
        let value = &data[data_len - value_len..];
        if storage_write(ValueOrRegister::Value(key), ValueOrRegister::Value(value), 1) {
            value_return(ValueOrRegister::Value(&1u64.to_le_bytes()));
        } else {
            value_return(ValueOrRegister::Value(&0u64.to_le_bytes()));
        }
    }

    fn write_block_height() {
        let block_height = block_height();
        let value = b"hello";
        storage_write(
            ValueOrRegister::Value(&block_height.to_le_bytes()),
            ValueOrRegister::Value(value),
            0,
        );
    }

    fn write_random_value() {
        random_seed(0);
        let data = read_register(0);
        let value = b"hello";
        storage_write(ValueOrRegister::Value(&data), ValueOrRegister::Value(value), 1);
    }

    /// Write a 1MB value under the given key.
    /// Key is of type u8. Value is made up of the key repeated a million times.
    fn write_one_megabyte() {
        input(0);
        if register_len(0) != Some(size_of::<u8>() as u64) {
            panic(None);
        }
        let key = read_register(0)[0];

        let value = vec![key; 1_000_000];
        storage_write(ValueOrRegister::Value(&[key]), ValueOrRegister::Value(&value), 0);
    }

    /// Read n megabytes of data between from..to
    /// Reads values that were written using `write_one_megabyte`.
    /// The input is a pair of u8 values `from` and `to.
    fn read_n_megabytes() {
        input(0);
        assert_eq!(register_len(0), Some(2 * size_of::<u8>() as u64));
        let input_data = read_register(0);

        let from = input_data[0];
        let to = input_data[1];

        for key in from..to {
            let result = storage_read(ValueOrRegister::Value(&[key]), 0);
            assert_eq!(result, true);
            assert_eq!(register_len(0), Some(1_000_000));
        }
    }

    fn read_value() {
        input(0);
        if register_len(0) != Some(size_of::<u64>() as u64) {
            panic(None)
        }
        let key = read_register(0);
        if storage_read(ValueOrRegister::Value(&key), 1) {
            let value = read_register(1);
            value_return(ValueOrRegister::Value(&value));
        }
    }

    fn out_of_memory() {
        let mut vec = Vec::new();
        loop {
            vec.push(vec![0; 1024]);
        }
    }

    /// Calls all host functions, either directly or via callback.
    fn sanity_check() {
        fn insert_account_id_prefix(
            prefix: &str,
            account_id: Vec<u8>,
        ) -> Result<Vec<u8>, std::string::FromUtf8Error> {
            let mut id = String::from_utf8(account_id)?;
            id.insert_str(0, prefix);
            Ok(id.as_bytes().to_vec())
        }

        // #############
        // # Registers #
        // #############
        input(0);
        let input_data = read_register(0);
        let input_args: serde_json::Value = serde_json::from_slice(&input_data).unwrap();

        // ###############
        // # Context API #
        // ###############
        current_account_id(1);
        let account_id = read_register(1);

        signer_account_pk(1);
        let account_public_key = read_register(1);

        signer_account_id(1);
        predecessor_account_id(1);

        // input() already called when reading the input of the contract call
        let _ = block_height();
        let _ = block_timestamp();
        let _ = epoch_height();
        let _ = storage_usage();

        // #################
        // # Economics API #
        // #################
        let _balance = account_balance();
        let _balance = attached_deposit();
        let available_gas = prepaid_gas() - used_gas();

        // ############
        // # Math API #
        // ############
        random_seed(1);
        let value = "hello";
        sha256(ValueOrRegister::Value(value.as_bytes()), 1);

        // #####################
        // # Miscellaneous API #
        // #####################
        value_return(ValueOrRegister::Value(value.as_bytes()));

        // Calling host functions that terminate execution via promises.
        let method_name_panic = b"sanity_check_panic";
        let args_panic = b"";
        let gas_per_promise = available_gas / 50;
        let promise = Promise::new(ValueOrRegister::Value(&account_id));
        promise.function_call(
            ValueOrRegister::Value(method_name_panic),
            ValueOrRegister::Value(args_panic),
            0u128.into(),
            gas_per_promise,
            0,
        );

        log(value);

        // ################
        // # Promises API #
        // ################
        let method_name_noop = b"noop";
        let args_noop = b"";
        let promise = Promise::new(ValueOrRegister::Value(&account_id));
        promise.function_call(
            ValueOrRegister::Value(method_name_noop),
            ValueOrRegister::Value(args_noop),
            0u128.into(),
            gas_per_promise,
            0,
        );
        let promises_then: [_; 2] = array::from_fn(|_| {
            let promise = promise.then(ValueOrRegister::Value(&account_id));
            promise.function_call(
                ValueOrRegister::Value(method_name_noop),
                ValueOrRegister::Value(args_noop),
                0u128.into(),
                gas_per_promise,
                0,
            );
            promise
        });
        let _and = Promise::and(&[&promises_then[0], &promises_then[1]]);

        _ = promises_then[1].then(ValueOrRegister::Value(&account_id));

        // #######################
        // # Promise API actions #
        // #######################
        let new_account_id = insert_account_id_prefix("foo.", account_id.clone()).unwrap();
        let amount_non_zero = 50_000_000_000_000_000_000_000u128;
        let contract_code = from_base64(input_args["contract_code"].as_str().unwrap());
        let method_deployed_contract = input_args["method_name"].as_str().unwrap().as_bytes();
        let args_deployed_contract = from_base64(input_args["method_args"].as_str().unwrap());
        let promise = Promise::new(ValueOrRegister::Value(&new_account_id));
        promise.create_account();
        promise.transfer(amount_non_zero.into());
        promise.deploy_contract(ValueOrRegister::Value(&contract_code));
        promise.deploy_global_contract(ValueOrRegister::Value(&contract_code));
        promise.deploy_global_contract_by_account_id(ValueOrRegister::Value(&contract_code));
        promise.function_call(
            ValueOrRegister::Value(method_deployed_contract),
            ValueOrRegister::Value(&args_deployed_contract),
            0u128.into(),
            gas_per_promise,
            0,
        );
        promise.function_call(
            ValueOrRegister::Value(method_deployed_contract),
            ValueOrRegister::Value(&args_deployed_contract),
            0u128.into(),
            0,
            1,
        );
        promise.add_key_with_full_access(ValueOrRegister::Value(&account_public_key), 0);
        promise.delete_key(ValueOrRegister::Value(&account_public_key));
        promise.add_key_with_function_call(
            ValueOrRegister::Value(&account_public_key),
            1,
            0u128.into(),
            ValueOrRegister::Value(&new_account_id),
            ValueOrRegister::Value(method_deployed_contract),
        );
        promise.delete_account(ValueOrRegister::Value(&account_id));

        // Create a new account as `DeleteAccountAction` fails after `StakeAction`.
        let new_account_id = insert_account_id_prefix("bar.", account_id.clone()).unwrap();
        let amount_stake = 30_000_000_000_000_000_000_000u128;
        let promise = Promise::new(ValueOrRegister::Value(&new_account_id));
        promise.create_account();
        promise.transfer(amount_non_zero.into());
        promise.stake(amount_stake.into(), ValueOrRegister::Value(&account_public_key));

        // #######################
        // # Promise API results #
        // #######################
        // Invoking `promise_results_count` and `promise_result` via a callback to
        // ensure there is a promise whose result can be accessed.
        let promise = Promise::new(ValueOrRegister::Value(&account_id));
        promise.function_call(
            ValueOrRegister::Value(method_name_noop),
            ValueOrRegister::Value(args_noop),
            0u128.into(),
            gas_per_promise,
            0,
        );
        let method_name_promise_results = b"sanity_check_promise_results";
        let args_promise_results = b"";
        let then = promise.then(ValueOrRegister::Value(&account_id));
        promise.function_call(
            ValueOrRegister::Value(method_name_promise_results),
            ValueOrRegister::Value(args_promise_results),
            0u128.into(),
            gas_per_promise,
            0,
        );
        then.return_();

        // ###############
        // # Storage API #
        // ###############
        // For storage funcs, cover both cases of key being unused and being used.

        let key = "hi";
        assert_eq!(
            storage_write(
                ValueOrRegister::Value(key.as_bytes()),
                ValueOrRegister::Value(value.as_bytes()),
                1,
            ),
            false,
        );
        assert_eq!(
            storage_write(
                ValueOrRegister::Value(key.as_bytes()),
                ValueOrRegister::Value(value.as_bytes()),
                1,
            ),
            true,
        );

        let unused_key = "abcdefg";
        assert_eq!(storage_read(ValueOrRegister::Value(unused_key.as_bytes()), 1), false);
        assert_eq!(storage_read(ValueOrRegister::Value(key.as_bytes()), 1), true);

        assert_eq!(storage_has_key(ValueOrRegister::Value(unused_key.as_bytes())), false);
        assert_eq!(storage_has_key(ValueOrRegister::Value(key.as_bytes())), true);

        assert_eq!(storage_remove(ValueOrRegister::Value(unused_key.as_bytes()), 1), false);
        assert_eq!(storage_remove(ValueOrRegister::Value(key.as_bytes()), 1), true);

        // #################
        // # Validator API #
        // #################
        let validator_id = input_args["validator_id"].as_str().unwrap();
        let _stake = validator_stake(ValueOrRegister::Value(validator_id.as_bytes()));
        let _stake = validator_total_stake();

        // ###################
        // # Math Extensions #
        // ###################
        let buffer = [65u8; 10];
        ripemd160(ValueOrRegister::Value(&buffer), 1);

        // #################
        // # alt_bn128 API #
        // #################
        let buffer: [u8; 96] = [
            16, 238, 91, 161, 241, 22, 172, 158, 138, 252, 202, 212, 136, 37, 110, 231, 118, 220,
            8, 45, 14, 153, 125, 217, 227, 87, 238, 238, 31, 138, 226, 8, 238, 185, 12, 155, 93,
            126, 144, 248, 200, 177, 46, 245, 40, 162, 169, 80, 150, 211, 157, 13, 10, 36, 44, 232,
            173, 32, 32, 115, 123, 2, 9, 47, 190, 148, 181, 91, 69, 6, 83, 40, 65, 222, 251, 70,
            81, 73, 60, 142, 130, 217, 176, 20, 69, 75, 40, 167, 41, 180, 244, 5, 142, 215, 135,
            35,
        ];
        alt_bn128_g1_multiexp(ValueOrRegister::Value(&buffer), 1);
        let buffer: [u8; 65] = [
            0, 11, 49, 94, 29, 152, 111, 116, 138, 248, 2, 184, 8, 159, 80, 169, 45, 149, 48, 32,
            49, 37, 6, 133, 105, 171, 194, 120, 44, 195, 17, 180, 35, 137, 154, 4, 192, 211, 244,
            93, 200, 2, 44, 0, 64, 26, 108, 139, 147, 88, 235, 242, 23, 253, 52, 110, 236, 67, 99,
            176, 2, 186, 198, 228, 25,
        ];
        alt_bn128_g1_sum(ValueOrRegister::Value(&buffer), 1);
        let buffer: [u8; 192] = [
            80, 12, 4, 181, 61, 254, 153, 52, 127, 228, 174, 24, 144, 95, 235, 26, 197, 188, 219,
            91, 4, 47, 98, 98, 202, 199, 94, 67, 211, 223, 197, 21, 65, 221, 184, 75, 69, 202, 13,
            56, 6, 233, 217, 146, 159, 141, 116, 208, 81, 224, 146, 124, 150, 114, 218, 196, 192,
            233, 253, 31, 130, 152, 144, 29, 34, 54, 229, 82, 80, 13, 200, 53, 254, 193, 250, 1,
            205, 60, 38, 172, 237, 29, 18, 82, 187, 98, 113, 152, 184, 251, 223, 42, 104, 148, 253,
            25, 79, 39, 165, 18, 195, 165, 215, 155, 168, 251, 250, 2, 215, 214, 193, 172, 187, 84,
            54, 168, 27, 100, 161, 155, 144, 95, 199, 238, 88, 238, 202, 46, 247, 97, 33, 56, 78,
            174, 171, 15, 245, 5, 121, 144, 88, 81, 102, 133, 118, 222, 81, 214, 74, 169, 27, 91,
            27, 23, 80, 55, 43, 97, 101, 24, 168, 29, 75, 136, 229, 2, 55, 77, 60, 200, 227, 210,
            172, 194, 232, 45, 151, 46, 248, 206, 193, 250, 145, 84, 78, 176, 74, 210, 0, 106, 168,
            30,
        ];
        alt_bn128_pairing_check(ValueOrRegister::Value(&buffer));
    }
}
