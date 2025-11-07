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
    export log-something: func();
    export loop-forever: func();
    export panic-with-message: func();
    export panic-after-logging: func();
    export run-test: func();
    export run-test-with-storage-change: func();
    export sum-with-input: func();
    export benchmark-storage8b: func();
    export benchmark-storage10kib: func();
    export pass-through: func();
    export sum-n: func();
    export fibonacci: func();
    export insert-strings: func();
    export delete-strings: func();
    export recurse: func();
    export out-of-memory: func();
    export max-self-recursion-delay: func();
    export call-promise: func();
    export check-promise-result-return-value: func();
    export check-promise-result-write-status: func();
    export call-yield-create-return-promise: func();
    export call-yield-create-return-data-id: func();
    export call-yield-resume: func();
    export call-yield-resume-read-data-id-from-storage: func();
    export call-yield-create-and-resume: func();
    export attach-unspent-gas-but-use-all-gas: func();
    export do-ripemd: func();
    export noop: func();
    export sanity-check: func();
    export sanity-check-promise-results: func();
    export sanity-check-panic: func();
    export sanity-check-panic-string: func();
    export generate-large-receipt: func();
    export do-function-call-with-args-of-size: func();
    export max-receipt-size-promise-return-method1: func();
    export max-receipt-size-promise-return-method2: func();
    export mark-test-completed: func();
    export assert-test-completed: func();
    export return-large-value: func();
    export max-receipt-size-value-return-method: func();
    export yield-with-large-args: func();
    export resume-with-large-payload: func();
}",
        ownership: Borrowing { duplicate_if_necessary: true },
        generate_all,
    });

    export!(Component);
}

use bindings::near::nearcore::runtime::*;
use bindings::Guest;
use core::array;

const TGAS: u64 = 1_000_000_000_000;

impl From<u128> for U128 {
    fn from(value: u128) -> Self {
        Self { lo: value as _, hi: (value >> 64) as _ }
    }
}

impl From<U128> for u128 {
    fn from(U128 { lo, hi }: U128) -> Self {
        u128::from(lo) | (u128::from(hi) << 64)
    }
}

impl From<U128> for [u8; 16] {
    fn from(U128 { lo, hi }: U128) -> Self {
        let lo = lo.to_ne_bytes();
        let hi = hi.to_ne_bytes();
        array::from_fn(|i| if i < 8 { lo[i] } else { hi[i - 8] })
    }
}

impl From<[u8; 16]> for U128 {
    fn from(bytes: [u8; 16]) -> Self {
        u128::from_ne_bytes(bytes).into()
    }
}

impl From<U256> for [u8; 32] {
    fn from(U256 { lo, hi }: U256) -> Self {
        let lo: [u8; 16] = lo.into();
        let hi: [u8; 16] = hi.into();
        array::from_fn(|i| if i < 16 { lo[i] } else { hi[i - 16] })
    }
}

impl From<[u8; 32]> for U256 {
    fn from(bytes: [u8; 32]) -> Self {
        let (lo, hi) = bytes.split_first_chunk().unwrap();
        let (hi, []) = hi.split_first_chunk().unwrap() else {
            unreachable!();
        };
        Self { lo: (*lo).into(), hi: (*hi).into() }
    }
}

impl From<U512> for [u8; 64] {
    fn from(U512 { lo, hi }: U512) -> Self {
        let lo: [u8; 32] = lo.into();
        let hi: [u8; 32] = hi.into();
        array::from_fn(|i| if i < 32 { lo[i] } else { hi[i - 32] })
    }
}

impl From<[u8; 64]> for U512 {
    fn from(bytes: [u8; 64]) -> Self {
        let (lo, hi) = bytes.split_first_chunk().unwrap();
        let (hi, []) = hi.split_first_chunk().unwrap() else {
            unreachable!();
        };
        Self { lo: (*lo).into(), hi: (*hi).into() }
    }
}

pub fn from_base64(s: &str) -> Vec<u8> {
    let engine = &base64::engine::general_purpose::STANDARD;
    base64::Engine::decode(engine, s).unwrap()
}

#[inline]
fn generate_data(data: &mut [u8]) {
    for i in 0..data.len() {
        data[i] = (i % u8::MAX as usize) as u8;
    }
}

fn parse_public_key(bytes: &[u8]) -> PublicKey {
    match bytes {
        [0, key @ ..] if key.len() == 32 => {
            PublicKey::Ed25519(U256::from_ne_bytes(key.try_into().unwrap()))
        }
        [1, key @ ..] if key.len() == 64 => {
            PublicKey::Secp256k1(U512::from_ne_bytes(key.try_into().unwrap()))
        }
        _ => {
            panic(Some("invalid key"));
            unreachable!()
        }
    }
}

pub struct Component;

impl U128 {
    pub fn to_ne_bytes(self) -> [u8; 16] {
        self.into()
    }

    pub fn from_ne_bytes(bytes: [u8; 16]) -> Self {
        bytes.into()
    }
}

impl U256 {
    pub fn to_ne_bytes(self) -> [u8; 32] {
        self.into()
    }

    pub fn from_ne_bytes(bytes: [u8; 32]) -> Self {
        bytes.into()
    }
}

impl U512 {
    pub fn to_ne_bytes(self) -> [u8; 64] {
        self.into()
    }

    pub fn from_ne_bytes(bytes: [u8; 64]) -> Self {
        bytes.into()
    }
}

impl Guest for Component {
    fn ext_storage_usage() {
        let n = storage_usage();
        value_return(&n.to_le_bytes())
    }

    fn ext_block_index() {
        let n = block_height();
        value_return(&n.to_le_bytes())
    }

    fn ext_block_timestamp() {
        let n = block_timestamp();
        value_return(&n.to_le_bytes())
    }

    fn ext_prepaid_gas() {
        let n = prepaid_gas();
        value_return(&n.to_le_bytes())
    }

    fn ext_random_seed() {
        let data = random_seed();
        value_return(&data)
    }

    fn ext_predecessor_account_id() {
        let data = predecessor_account_id();
        value_return(data.to_string().as_bytes())
    }

    fn ext_signer_pk() {
        match signer_account_pk() {
            PublicKey::Ed25519(key) => {
                let data = key.to_ne_bytes();
                let data: [u8; 33] = array::from_fn(|i| if i == 0 { 0 } else { data[i - 1] });
                value_return(&data);
            }
            PublicKey::Secp256k1(key) => {
                let data = key.to_ne_bytes();
                let data: [u8; 65] = array::from_fn(|i| if i == 0 { 1 } else { data[i - 1] });
                value_return(&data);
            }
        }
    }

    fn ext_signer_id() {
        let data = signer_account_id();
        value_return(data.to_string().as_bytes())
    }

    fn ext_account_id() {
        let data = current_account_id();
        value_return(data.to_string().as_bytes())
    }

    fn ext_account_balance() {
        let n = account_balance();
        value_return(&u128::from(n).to_le_bytes())
    }

    fn ext_attached_deposit() {
        let n = attached_deposit();
        value_return(&u128::from(n).to_le_bytes())
    }

    fn ext_validator_total_stake() {
        let n = validator_total_stake();
        value_return(&u128::from(n).to_le_bytes())
    }

    fn ext_sha256() {
        let bytes = input();
        let result = sha256(&bytes);
        value_return(&result.to_ne_bytes());
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
        value_return(&result);
    }

    fn ext_validator_stake() {
        let account_id = String::from_utf8(input()).unwrap();
        let account_id = AccountId::from_string(&account_id).unwrap();
        let result = validator_stake(&account_id);
        value_return(&u128::from(result).to_le_bytes());
    }
    /// Write key-value pair into storage.
    /// Input is the byte array where the value is `u64` represented by last 8 bytes and key is represented by the first
    /// `register_len(0) - 8` bytes.
    fn write_key_value() {
        let data = input();
        let data_len = data.len();
        let value_len = size_of::<u64>();

        let key = &data[0..data_len - value_len];
        let value = &data[data_len - value_len..];
        if storage_write(key, value, 1) {
            value_return(&1u64.to_le_bytes());
        } else {
            value_return(&0u64.to_le_bytes());
        }
    }

    fn write_block_height() {
        let block_height = block_height();
        let value = b"hello";
        storage_write(&block_height.to_le_bytes(), value, 0);
    }

    fn write_random_value() {
        let data = random_seed();
        let value = b"hello";
        storage_write(&data, value, 1);
    }

    /// Write a 1MB value under the given key.
    /// Key is of type u8. Value is made up of the key repeated a million times.
    fn write_one_megabyte() {
        let key = input();
        assert_eq!(key.len(), size_of::<u8>());
        let key = key[0];

        let value = vec![key; 1_000_000];
        storage_write(&[key], &value, 0);
    }

    /// Read n megabytes of data between from..to
    /// Reads values that were written using `write_one_megabyte`.
    /// The input is a pair of u8 values `from` and `to.
    fn read_n_megabytes() {
        let input_data = input();
        assert_eq!(input_data.len(), 2 * size_of::<u8>());

        let from = input_data[0];
        let to = input_data[1];

        for key in from..to {
            let result = storage_read(&[key]).unwrap();
            assert_eq!(result.len(), 1_000_000);
        }
    }

    fn read_value() {
        let key = input();
        assert_eq!(key.len(), size_of::<u64>());
        if let Some(value) = storage_read(&key) {
            value_return(&value);
        }
    }

    fn log_something() {
        log("hello");
    }

    fn loop_forever() {
        loop {}
    }

    fn panic_with_message() {
        panic(Some("WAT?"));
    }

    fn panic_after_logging() {
        log("hello");
        panic(Some("WAT?"));
    }

    fn run_test() {
        value_return(&10i32.to_le_bytes());
    }

    fn run_test_with_storage_change() {
        let key = b"hello";
        let value = b"world";
        storage_write(key, value, 0);
    }

    fn sum_with_input() {
        let data = input();
        assert_eq!(data.len(), 2 * size_of::<u64>());

        let mut key = [0u8; size_of::<u64>()];
        let mut value = [0u8; size_of::<u64>()];
        key.copy_from_slice(&data[..size_of::<u64>()]);
        value.copy_from_slice(&data[size_of::<u64>()..]);
        let key = u64::from_le_bytes(key);
        let value = u64::from_le_bytes(value);
        let result = key + value;
        value_return(&result.to_le_bytes());
    }

    /// Writes and reads some data into/from storage. Uses 8-bit key/values.
    fn benchmark_storage8b() {
        let data = input();
        let Ok(data) = data.try_into() else {
            panic(None);
            unreachable!()
        };
        let n: u64 = u64::from_le_bytes(data);

        let mut sum = 0u64;
        for i in 0..n {
            let el = i.to_le_bytes();
            storage_write(&el, &el, 0);

            if let Some(value) = storage_read(&el) {
                sum += u64::from_le_bytes(value.try_into().unwrap());
            }
        }

        value_return(&sum.to_le_bytes());
    }

    /// Writes and reads some data into/from storage. Uses 10KiB key/values.
    fn benchmark_storage10kib() {
        let data = input();
        let Ok(data) = data.try_into() else {
            panic(None);
            unreachable!()
        };
        let n: u64 = u64::from_le_bytes(data);

        let mut el = [0u8; 10 << 10];
        generate_data(&mut el);

        let mut sum = 0u64;
        for i in 0..n {
            el[..size_of::<u64>()].copy_from_slice(&i.to_le_bytes());
            storage_write(&el, &el, 0);

            if let Some(value) = storage_read(&el) {
                sum += u64::from_le_bytes(value[0..size_of::<u64>()].try_into().unwrap());
            }
        }

        value_return(&sum.to_le_bytes());
    }

    fn pass_through() {
        let data = input();
        if data.len() != size_of::<u64>() {
            panic(None);
            unreachable!();
        }
        value_return(&data);
    }

    fn sum_n() {
        let data = input();
        let Ok(data) = data.try_into() else {
            panic(None);
            unreachable!()
        };
        let n = u64::from_le_bytes(data);

        let mut sum = 0u64;
        for i in 0..n {
            // LLVM optimizes sum += i into O(1) computation, use volatile to thwart
            // that.
            let new_sum = unsafe { std::ptr::read_volatile(&sum) }.wrapping_add(i);
            unsafe { std::ptr::write_volatile(&mut sum, new_sum) };
        }

        let data = sum.to_le_bytes();
        value_return(&data);
    }

    /// Calculates Fibonacci numbers in inefficient way.  Used to burn gas for the
    /// sanity/max_gas_burnt_view.py test.  The implementation has exponential
    /// complexity (1.62^n to be exact) so even small increase in argument result in
    /// large increase in gas use.
    fn fibonacci() {
        fn fib(n: u8) -> u64 {
            if n < 2 {
                n as u64
            } else {
                fib(n - 2) + fib(n - 1)
            }
        }

        let data = input();
        let Ok([n]): Result<[u8; 1], _> = data.try_into() else {
            panic(None);
            unreachable!()
        };
        let data = fib(n).to_le_bytes();
        value_return(&data);
    }

    fn insert_strings() {
        let data = input();
        let Ok(data): Result<[u8; 2 * size_of::<u64>()], _> = data.try_into() else {
            panic(None);
            unreachable!()
        };

        let mut from = [0u8; size_of::<u64>()];
        let mut to = [0u8; size_of::<u64>()];
        from.copy_from_slice(&data[..size_of::<u64>()]);
        to.copy_from_slice(&data[size_of::<u64>()..]);
        let from = u64::from_le_bytes(from);
        let to = u64::from_le_bytes(to);
        let s = vec![b'a'; to as usize];
        for i in from..to {
            let mut key = s[(to - i) as usize..].to_vec();
            key.push(b'b');
            let value = b"x";
            storage_write(&key, value, 0);
        }
    }

    fn delete_strings() {
        let data = input();
        let Ok(data): Result<[u8; 2 * size_of::<u64>()], _> = data.try_into() else {
            panic(None);
            unreachable!()
        };

        let mut from = [0u8; size_of::<u64>()];
        let mut to = [0u8; size_of::<u64>()];
        from.copy_from_slice(&data[..size_of::<u64>()]);
        to.copy_from_slice(&data[size_of::<u64>()..]);
        let from = u64::from_le_bytes(from);
        let to = u64::from_le_bytes(to);
        let s = vec![b'a'; to as usize];
        for i in from..to {
            let mut key = s[(to - i) as usize..].to_vec();
            key.push(b'b');
            storage_remove(&key, 0);
        }
    }

    fn recurse() {
        /// Rust compiler is getting smarter and starts to optimize my deep recursion.
        /// We're going to fight it with a more obscure implementations.
        #[unsafe(no_mangle)]
        #[inline(never)]
        fn internal_recurse(n: u64) -> u64 {
            if n <= 1 {
                n
            } else {
                let a = internal_recurse(n - 1) + 1;
                if a % 2 == 1 {
                    (a + n) / 2
                } else {
                    a
                }
            }
        }

        let data = input();
        let Ok(data) = data.try_into() else {
            panic(None);
            unreachable!()
        };
        let n = u64::from_le_bytes(data);
        let res = internal_recurse(n);
        let data = res.to_le_bytes();
        value_return(&data);
    }

    fn out_of_memory() {
        let mut vec = Vec::new();
        loop {
            vec.push(vec![0; 1024]);
        }
    }

    /// Delay completion of the receipt for as long as possible through self cross-contract calls.
    ///
    /// This contract keeps the recursion depth and returns it when less than 5Tgas remains, which is
    /// most likely is no longer sufficient for another cross-contract call.
    ///
    /// This is a stable alternative to yield/resume proposal at the time of writing.
    fn max_self_recursion_delay() {
        let bytes = input().try_into().unwrap();
        let recursion = u32::from_be_bytes(bytes);
        let available_gas = prepaid_gas() - used_gas();
        if available_gas < 5_000_000_000_000 {
            return value_return(&bytes);
        }
        let id = current_account_id();
        let method_name = "max-self-recursion-delay";
        let promise = Promise::new(&id);
        let amount = 1u128;
        let gas_fixed = 0;
        let gas_weight = 1;
        let argument_bytes = recursion.saturating_add(1).to_be_bytes();
        promise.function_call(
            method_name.as_bytes(),
            &argument_bytes,
            amount.into(),
            gas_fixed,
            gas_weight,
        );
        promise.return_();
    }

    fn call_promise() {
        let data = input();
        let input_args: serde_json::Value = serde_json::from_slice(&data).unwrap();
        for arg in input_args.as_array().unwrap() {
            let p = if let Some(create) = arg.get("create") {
                let account_id = create["account_id"].as_str().unwrap();
                let method_name = create["method_name"].as_str().unwrap().as_bytes();
                let arguments = serde_json::to_vec(&create["arguments"]).unwrap();
                let amount = create["amount"].as_str().unwrap().parse::<u128>().unwrap();
                let gas = create["gas"].as_i64().unwrap() as u64;

                let account_id = AccountId::from_string(account_id).unwrap();
                let p = Promise::new(&account_id);
                p.function_call(method_name, &arguments, amount.into(), gas, 0);
                p
            } else if let Some(then) = arg.get("then") {
                let promise_index = then["promise_index"].as_u64().unwrap() as u64;
                let account_id = then["account_id"].as_str().unwrap();
                let method_name = then["method_name"].as_str().unwrap().as_bytes();
                let arguments = serde_json::to_vec(&then["arguments"]).unwrap();
                let amount = then["amount"].as_str().unwrap().parse::<u128>().unwrap();
                let gas = then["gas"].as_i64().unwrap() as u64;
                let account_id = AccountId::from_string(account_id).unwrap();
                let p = Promise::from_index(promise_index);
                let p = p.then(&account_id);
                p.function_call(method_name, &arguments, amount.into(), gas, 0);
                p
            } else if let Some(and) = arg.get("and") {
                let and = and.as_array().unwrap();
                let and: Vec<_> = and
                    .into_iter()
                    .map(|v| Promise::from_index(v.as_i64().unwrap() as u64))
                    .collect();
                let and: Vec<_> = and.iter().collect();
                Promise::and(&and)
            } else if let Some(batch_create) = arg.get("batch_create") {
                let account_id = batch_create["account_id"].as_str().unwrap();
                let account_id = AccountId::from_string(account_id).unwrap();
                Promise::new(&account_id)
            } else if let Some(batch_then) = arg.get("batch_then") {
                let promise_index = batch_then["promise_index"].as_i64().unwrap() as u64;
                let account_id = batch_then["account_id"].as_str().unwrap();
                let account_id = AccountId::from_string(account_id).unwrap();
                Promise::from_index(promise_index).then(&account_id)
            } else if let Some(action) = arg.get("action_create_account") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let p = Promise::from_index(promise_index);
                p.create_account();
                p
            } else if let Some(action) = arg.get("action_deploy_contract") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let code = from_base64(action["code"].as_str().unwrap());
                let p = Promise::from_index(promise_index);
                p.deploy_contract(&code);
                p
            } else if let Some(action) = arg.get("action_function_call") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let method_name = action["method_name"].as_str().unwrap().as_bytes();
                let arguments = serde_json::to_vec(&action["arguments"]).unwrap();
                let amount = action["amount"].as_str().unwrap().parse::<u128>().unwrap();
                let gas = action["gas"].as_i64().unwrap() as u64;
                let p = Promise::from_index(promise_index);
                p.function_call(method_name, &arguments, amount.into(), gas, 0);
                p
            } else if let Some(action) = arg.get("action_transfer") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let amount = action["amount"].as_str().unwrap().parse::<u128>().unwrap();
                let p = Promise::from_index(promise_index);
                p.transfer(amount.into());
                p
            } else if let Some(action) = arg.get("action_stake") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let amount = action["amount"].as_str().unwrap().parse::<u128>().unwrap();
                let public_key = from_base64(action["public_key"].as_str().unwrap());
                let p = Promise::from_index(promise_index);
                p.stake(amount.into(), parse_public_key(&public_key));
                p
            } else if let Some(action) = arg.get("action_add_key_with_full_access") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let public_key = from_base64(action["public_key"].as_str().unwrap());
                let nonce = action["nonce"].as_i64().unwrap() as u64;
                let p = Promise::from_index(promise_index);
                p.add_key_with_full_access(parse_public_key(&public_key), nonce);
                p
            } else if let Some(action) = arg.get("action_add_key_with_function_call") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let public_key = from_base64(action["public_key"].as_str().unwrap());
                let nonce = action["nonce"].as_i64().unwrap() as u64;
                let allowance = action["allowance"].as_str().unwrap().parse::<u128>().unwrap();
                let receiver_id = action["receiver_id"].as_str().unwrap();
                let method_names = action["method_names"].as_str().unwrap().as_bytes();

                let receiver_id = AccountId::from_string(receiver_id).unwrap();
                let method_names: Vec<_> = method_names.split(|c| *c == b',').collect();
                let p = Promise::from_index(promise_index);
                p.add_key_with_function_call(
                    parse_public_key(&public_key),
                    nonce,
                    allowance.into(),
                    &receiver_id,
                    &method_names,
                );
                p
            } else if let Some(action) = arg.get("action_delete_key") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let public_key = from_base64(action["public_key"].as_str().unwrap());
                let p = Promise::from_index(promise_index);
                p.delete_key(parse_public_key(&public_key));
                p
            } else if let Some(action) = arg.get("action_delete_account") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let beneficiary_id = action["beneficiary_id"].as_str().unwrap();
                let beneficiary_id = AccountId::from_string(beneficiary_id).unwrap();
                let p = Promise::from_index(promise_index);
                p.delete_account(&beneficiary_id);
                p
            } else if let Some(action) = arg.get("set_refund_to") {
                let promise_index = action["promise_index"].as_i64().unwrap() as u64;
                let beneficiary_id = action["beneficiary_id"].as_str().unwrap();
                let beneficiary_id = AccountId::from_string(beneficiary_id).unwrap();
                let p = Promise::from_index(promise_index);
                p.set_refund_to(&beneficiary_id);
                p
            } else {
                unimplemented!()
            };
            let expected_id = arg["id"].as_i64().unwrap() as u64;
            assert_eq!(p.to_index(), expected_id);
            if let Some(ret) = arg.get("return") {
                if ret.as_bool().unwrap() == true {
                    p.return_()
                }
            }
        }
    }

    /// Used as the yield callback in tests of yield create / yield resume.
    /// The function takes an argument indicating the expected yield payload (promise input).
    /// It panics if executed with the wrong payload.
    /// Returns the payload length.
    fn check_promise_result_return_value() {
        let expected = input();

        assert_eq!(Promise::get_results_count(), 1);

        let payload_len = match Promise::get_result(0, 0).unwrap() {
            Ok(()) => {
                let payload = read_register(0);
                assert_eq!(expected, payload);

                payload.len()
            }
            Err(()) => {
                assert_eq!(expected.len(), 0);

                0
            }
        };

        value_return(&[payload_len as u8]);
    }

    /// Function which expects to receive exactly one promise result,
    /// the contents of which should match the function's input.
    ///
    /// Used as the yield callback in tests of yield create / yield resume.
    /// Writes the status of the promise result to storage.
    fn check_promise_result_write_status() {
        let expected = input();

        assert_eq!(Promise::get_results_count(), 1);
        let status = match Promise::get_result(0, 0).unwrap() {
            Ok(()) => {
                let result = read_register(0);
                assert_eq!(expected, result);
                "Resumed "
            }
            Err(()) => {
                assert_eq!(expected.len(), 0);
                "Timeout "
            }
        };

        // Write promise status to state.
        // Used in tests to determine whether this function has been executed.
        let key = 123u64.to_le_bytes();
        storage_write(&key, status.as_bytes(), 0);
    }

    /// Call promise_yield_create, specifying `check_promise_result` as the yield callback.
    /// Given input is passed as the argument to the `check_promise_result` function call.
    /// Sets the yield callback's output as the return value.
    fn call_yield_create_return_promise() {
        let payload = input();

        // Create a promise yield with callback `check_promise_result`,
        // passing the expected payload as an argument to the function.
        let method_name = "check-promise-result-return-value";
        let gas_fixed = 0;
        let gas_weight = 1;
        let data_id_register = 0;
        let promise = Promise::yield_create(
            method_name.as_bytes(),
            &payload,
            gas_fixed,
            gas_weight,
            data_id_register,
        );

        // Write the data id to state for convenience in testing.
        let key = 42u64.to_le_bytes();
        let data_id = read_register(data_id_register);
        storage_write(&key, &data_id, 0);

        promise.return_();
    }

    /// Call promise_yield_create, specifying `check_promise_result` as the yield callback.
    /// Given input is passed as the argument to the `check_promise_result` function call.
    /// Returns the data id produced by promise_yield_create.
    fn call_yield_create_return_data_id() {
        let payload = input();

        // Create a promise yield with callback `check_promise_result`,
        // passing the expected payload as an argument to the function.
        let method_name = "check-promise-result-write-status";
        let gas_fixed = 0;
        let gas_weight = 1;
        let data_id_register = 0;
        Promise::yield_create(
            method_name.as_bytes(),
            &payload,
            gas_fixed,
            gas_weight,
            data_id_register,
        );

        let data_id = read_register(data_id_register);

        value_return(&data_id);
    }

    /// Call promise_yield_resume.
    /// Input is the byte array with `data_id` represented by last 32 bytes and `payload`
    /// represented by the first `register_len(0) - 32` bytes.
    fn call_yield_resume() {
        let data = input();

        let data_id = &data[data.len() - 32..];
        let payload = &data[0..data.len() - 32];

        let success =
            Promise::yield_resume(U256::from_ne_bytes(data_id.try_into().unwrap()), payload);

        let result = [success as u8];
        value_return(&result);
    }

    /// Call promise_yield_resume.
    /// Input is the payload to be passed to `promise_yield_resume`.
    /// The data_id is read from storage.
    fn call_yield_resume_read_data_id_from_storage() {
        let payload = input();

        let data_id_key = 42u64.to_le_bytes();
        let data_id = storage_read(&data_id_key).unwrap();

        let success =
            Promise::yield_resume(U256::from_ne_bytes(data_id.try_into().unwrap()), &payload);

        let result = [success as u8];
        value_return(&result);
    }

    /// Call promise_yield_create and promise_yield_resume within the same function.
    fn call_yield_create_and_resume() {
        let payload = input();

        // Create a promise yield with callback `check_promise_result`,
        // passing the expected payload as an argument to the function.
        let method_name = "check-promise-result-return-value";
        let gas_fixed = 0;
        let gas_weight = 1;
        let data_id_register = 0;
        let promise = Promise::yield_create(
            method_name.as_bytes(),
            &payload,
            gas_fixed,
            gas_weight,
            data_id_register,
        );

        let data_id = read_register(data_id_register);

        // Resolve the promise yield with the expected payload
        let success =
            Promise::yield_resume(U256::from_ne_bytes(data_id.try_into().unwrap()), &payload);
        assert_eq!(success, true);

        // This function's return value will resolve to the value returned by the
        // `check_promise_result` callback
        promise.return_();
    }

    fn attach_unspent_gas_but_use_all_gas() {
        let account_id = AccountId::from_string("alice.near").unwrap();
        let promise = Promise::new(&account_id);

        let method_name = "f";
        let amount = 1u128;
        let gas_fixed = 0;
        let gas_weight = 1;
        promise.function_call(method_name.as_bytes(), &[], amount.into(), gas_fixed, gas_weight);

        let promise = Promise::new(&account_id);

        let gas_fixed = 10u64.pow(14);
        let gas_weight = 0;
        promise.function_call(method_name.as_bytes(), &[], amount.into(), gas_fixed, gas_weight);
    }

    fn do_ripemd() {
        _ = ripemd160(b"tesdsst");
    }

    fn noop() {}

    /// Calls all host functions, either directly or via callback.
    fn sanity_check() {
        fn insert_account_id_prefix(prefix: &str, account_id: &AccountId) -> String {
            let mut id = account_id.to_string();
            id.insert_str(0, prefix);
            id
        }

        // #############
        // # Registers #
        // #############
        let input_data = input();
        let input_args: serde_json::Value = serde_json::from_slice(&input_data).unwrap();

        // ###############
        // # Context API #
        // ###############
        let account_id = current_account_id();

        let account_public_key = signer_account_pk();

        _ = signer_account_id();
        _ = predecessor_account_id();

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
        _ = random_seed();
        let value = "hello";
        _ = sha256(value.as_bytes());

        // #####################
        // # Miscellaneous API #
        // #####################
        value_return(value.as_bytes());

        // Calling host functions that terminate execution via promises.
        let method_name_panic = b"sanity-check-panic";
        let args_panic = b"";
        let gas_per_promise = available_gas / 50;
        let promise = Promise::new(&account_id);
        promise.function_call(method_name_panic, args_panic, 0u128.into(), gas_per_promise, 0);
        let method_name_panic_string = b"sanity-check-panic-string";
        let args_panic_string = b"";
        let promise = Promise::new(&account_id);
        promise.function_call(
            method_name_panic_string,
            args_panic_string,
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
        let promise = Promise::new(&account_id);
        promise.function_call(method_name_noop, args_noop, 0u128.into(), gas_per_promise, 0);
        let promises_then: [_; 2] = array::from_fn(|_| {
            let promise = promise.then(&account_id);
            promise.function_call(method_name_noop, args_noop, 0u128.into(), gas_per_promise, 0);
            promise
        });
        _ = Promise::and(&[&promises_then[0], &promises_then[1]]);
        _ = Promise::new(&account_id);
        _ = promises_then[1].then(&account_id);

        // #######################
        // # Promise API actions #
        // #######################
        let new_account_id =
            AccountId::from_string(&insert_account_id_prefix("foo.", &account_id)).unwrap();
        let amount_non_zero = 50_000_000_000_000_000_000_000u128;
        let contract_code = from_base64(input_args["contract_code"].as_str().unwrap());
        let method_deployed_contract = input_args["method_name"].as_str().unwrap().as_bytes();
        let args_deployed_contract = from_base64(input_args["method_args"].as_str().unwrap());
        let promise = Promise::new(&new_account_id);
        promise.create_account();
        promise.transfer(amount_non_zero.into());
        promise.deploy_contract(&contract_code);
        promise.deploy_global_contract(&contract_code);
        promise.deploy_global_contract_by_account_id(&contract_code);
        promise.function_call(
            method_deployed_contract,
            &args_deployed_contract,
            0u128.into(),
            gas_per_promise,
            0,
        );
        promise.function_call(
            method_deployed_contract,
            &args_deployed_contract,
            0u128.into(),
            0,
            1,
        );
        promise.add_key_with_full_access(account_public_key, 0);
        promise.delete_key(account_public_key);
        promise.add_key_with_function_call(
            account_public_key,
            1,
            0u128.into(),
            &new_account_id,
            &[method_deployed_contract],
        );
        promise.delete_account(&account_id);

        // Create a new account as `DeleteAccountAction` fails after `StakeAction`.
        let new_account_id =
            AccountId::from_string(&insert_account_id_prefix("bar.", &account_id)).unwrap();
        let amount_stake = 30_000_000_000_000_000_000_000u128;
        let promise = Promise::new(&new_account_id);
        promise.create_account();
        promise.transfer(amount_non_zero.into());
        promise.stake(amount_stake.into(), account_public_key);

        // #######################
        // # Promise API results #
        // #######################
        // Invoking `promise_results_count` and `promise_result` via a callback to
        // ensure there is a promise whose result can be accessed.
        let promise = Promise::new(&account_id);
        promise.function_call(method_name_noop, args_noop, 0u128.into(), gas_per_promise, 0);
        let method_name_promise_results = b"sanity-check-promise-results";
        let args_promise_results = b"";
        let then = promise.then(&account_id);
        then.function_call(
            method_name_promise_results,
            args_promise_results,
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
        assert_eq!(storage_write(key.as_bytes(), value.as_bytes(), 1,), false);
        assert_eq!(storage_write(key.as_bytes(), value.as_bytes(), 1,), true);

        let unused_key = "abcdefg";
        assert_eq!(storage_read(unused_key.as_bytes()), None);
        assert_eq!(storage_read(key.as_bytes()), Some(value.as_bytes().into()));

        assert_eq!(storage_has_key(unused_key.as_bytes()), false);
        assert_eq!(storage_has_key(key.as_bytes()), true);

        assert_eq!(storage_remove(unused_key.as_bytes(), 1), false);
        assert_eq!(storage_remove(key.as_bytes(), 1), true);

        // #################
        // # Validator API #
        // #################
        let validator_id = input_args["validator_id"].as_str().unwrap();
        let validator_id = AccountId::from_string(validator_id).unwrap();
        let _stake = validator_stake(&validator_id);
        let _stake = validator_total_stake();

        // ###################
        // # Math Extensions #
        // ###################
        let buffer = [65u8; 10];
        ripemd160(&buffer);

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
        _ = alt_bn128_g1_multiexp(&buffer);
        let buffer: [u8; 65] = [
            0, 11, 49, 94, 29, 152, 111, 116, 138, 248, 2, 184, 8, 159, 80, 169, 45, 149, 48, 32,
            49, 37, 6, 133, 105, 171, 194, 120, 44, 195, 17, 180, 35, 137, 154, 4, 192, 211, 244,
            93, 200, 2, 44, 0, 64, 26, 108, 139, 147, 88, 235, 242, 23, 253, 52, 110, 236, 67, 99,
            176, 2, 186, 198, 228, 25,
        ];
        _ = alt_bn128_g1_sum(&buffer);
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
        alt_bn128_pairing_check(&buffer);
    }

    /// Callback for a promise created in `sanity_check`. It calls host functions
    /// which use the results of earlier promises.
    fn sanity_check_promise_results() {
        assert_eq!(Promise::get_results_count(), 1);
        Promise::get_result(0, 0);
    }

    fn sanity_check_panic() {
        panic(None);
    }

    fn sanity_check_panic_string() {
        panic(Some("xyz"));
    }

    /// Generate a single large receipt that has many FunctionCall actions with large args.
    /// Accepts json parameters:
    /// account_id - the account id to send the FunctionCall actions to.
    /// method_name - the method name to call in FunctionCalls.
    /// total_args_size - the total size of the arguments to send in the FunctionCalls.
    fn generate_large_receipt() {
        let data = input();
        let input_args: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let account_id = input_args["account_id"].as_str().unwrap();
        let method_name = input_args["method_name"].as_str().unwrap().as_bytes();
        let mut total_size_to_send = input_args["total_args_size"].as_u64().unwrap();

        // Up to 500 kB of arguments in a single function call.
        let single_call_size = 500_000;

        let account_id = AccountId::from_string(&account_id).unwrap();
        let promise = Promise::new(&account_id);
        while total_size_to_send > 0 {
            let args_size = std::cmp::min(total_size_to_send, single_call_size);
            let args = vec![0u8; args_size as usize];
            let amount = 0u128;
            let gas_fixed = 0;
            let gas_weight = 1;
            promise.function_call(method_name, &args, amount.into(), gas_fixed, gas_weight);
            total_size_to_send = total_size_to_send.checked_sub(args_size).unwrap();
        }
    }

    /// Produces a function_call receipt to another account with the given method
    /// and arguments of the given size. Used to send large receipts between shards.
    /// Attaches only 1 Gas to the receipt to minimize congestion.
    fn do_function_call_with_args_of_size() {
        let data = input();
        let input_args: serde_json::Value = serde_json::from_slice(&data).unwrap();
        let account_id = input_args["account_id"].as_str().unwrap();
        let method_name = input_args["method_name"].as_str().unwrap().as_bytes();
        let args_size = input_args["args_size"].as_u64().unwrap();
        let args = vec![0u8; args_size as usize];
        let amount = 0u128;
        let gas_fixed = 1; // Attach only 1 Gas to the receipt to keep congestion low
        let gas_weight = 0;
        let account_id = AccountId::from_string(&account_id).unwrap();
        let promise = Promise::new(&account_id);
        promise.function_call(method_name, &args, amount.into(), gas_fixed, gas_weight);
    }

    /// Used by the `max_receipt_size_promise_return` test.
    /// Create promise DAG:
    /// A[self.max_receipt_size_promise_return_method2()] -then-> B[self.mark_test_completed()]
    fn max_receipt_size_promise_return_method1() {
        let args = input();

        let current_account = current_account_id();

        let method2 = b"max-receipt-size-promise-return-method2";
        let promise_a = Promise::new(&current_account);
        promise_a.function_call(
            method2,
            &args, // Forward the args
            0.into(),
            200 * TGAS,
            0,
        );

        let empty_args: &[u8] = &[];
        let test_completed_method = b"mark-test-completed";
        let promise_b = promise_a.then(&current_account);
        promise_b.function_call(test_completed_method, empty_args, 0.into(), 20 * TGAS, 0);
    }

    /// Do a promise_return with a large receipt.
    /// The receipt has a single FunctionCall action with large args.
    /// Creates DAG:
    /// C[self.noop(large_args)] -then-> B[self.mark_test_completed()]
    fn max_receipt_size_promise_return_method2() {
        let args = input();
        let input_args_json: serde_json::Value = serde_json::from_slice(&args).unwrap();
        let args_size = input_args_json["args_size"].as_u64().unwrap();

        let current_account = current_account_id();

        let large_args = vec![0u8; args_size as usize];
        let noop_method = b"noop";
        let promise_c = Promise::new(&current_account);
        promise_c.function_call(noop_method, &large_args, 0.into(), 20 * TGAS, 0);

        promise_c.return_()
    }

    /// Mark a test as completed
    fn mark_test_completed() {
        let key = b"test_completed";
        let value = b"true";
        storage_write(key, value, 0);
    }

    // Assert that the test has been marked as completed.
    // (Make sure that the method mark_test_completed was executed)
    fn assert_test_completed() {
        let key = b"test_completed";
        let Some(value) = storage_read(key) else {
            let panic_msg = "assert_test_completed failed - can't read test_completed marker";
            panic(Some(panic_msg));
            unreachable!();
        };
        if value != b"true" {
            let panic_msg = "assert_test_completed failed - test_completed value is not true";
            panic(Some(panic_msg));
        }
    }

    /// Returns a value of size "value_size".
    /// Accepts json args, e.g {"value_size": 1000}
    fn return_large_value() {
        let args = input();
        let input_args_json: serde_json::Value = serde_json::from_slice(&args).unwrap();
        let args_size = input_args_json["value_size"].as_u64().unwrap();

        let large_value = vec![0u8; args_size as usize];
        value_return(&large_value);
    }

    /// Used in the `max_receipt_size_value_return` test.
    fn max_receipt_size_value_return_method() {
        let args = input();

        let current_account = current_account_id();

        let large_value_method = b"return-large-value";
        let promise_a = Promise::new(&current_account);
        promise_a.function_call(large_value_method, &args, 0.into(), 250 * TGAS, 0);

        let test_completed_method = b"mark-test-completed";
        let empty_args: &[u8] = &[];
        let promise_b = promise_a.then(&current_account);
        promise_b.function_call(test_completed_method, empty_args, 0.into(), 20 * TGAS, 0);
    }

    fn yield_with_large_args() {
        let args = input();
        let input_args_json: serde_json::Value = serde_json::from_slice(&args).unwrap();
        let args_size = input_args_json["args_size"].as_u64().unwrap();

        let large_args = vec![0u8; args_size as usize];
        let method_name = b"noop";
        let data_id_register = 0;
        Promise::yield_create(method_name, &large_args, 0, 1, data_id_register);
    }

    fn resume_with_large_payload() {
        let args = input();
        let input_args_json: serde_json::Value = serde_json::from_slice(&args).unwrap();
        let payload_size = input_args_json["payload_size"].as_u64().unwrap();

        let empty_args: &[u8] = &[];
        let method_name = b"noop";
        let data_id_register = 0;
        Promise::yield_create(method_name, empty_args, 20 * TGAS, 0, data_id_register);

        let data_id = read_register(data_id_register);

        let resolve_data = vec![0u8; payload_size as usize];
        let success =
            Promise::yield_resume(U256::from_ne_bytes(data_id.try_into().unwrap()), &resolve_data);
        assert_eq!(success, true);
    }
}
