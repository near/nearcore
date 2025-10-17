mod bindings {
    use crate::Component;

    wit_bindgen::generate!({
        inline: "
package near:test;

world test {
    import near:nearcore/runtime@0.1.0;
    export write-key-value: func();
    export read-value: func();
    export sanity-check: func();
}",
        ownership: Borrowing { duplicate_if_necessary: true },
        generate_all,
    });

    export!(Component);
}

use bindings::near::nearcore::runtime::*;
use bindings::Guest;

pub struct Component;

impl Guest for Component {
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

    /// Calls all host functions, either directly or via callback.
    fn sanity_check() {
        // #############
        // # Registers #
        // #############
        input(0);
        let input_data = read_register(0);
        let _input_args: serde_json::Value = serde_json::from_slice(&input_data).unwrap();

        // ###############
        // # Context API #
        // ###############
        current_account_id(1);
        let _account_id = read_register(1);

        signer_account_pk(1);
        let _account_public_key = read_register(1);

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
        let _available_gas = prepaid_gas() - used_gas();

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

        log(value);

        // TODO: Expand the test
    }
}
