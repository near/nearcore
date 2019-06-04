use primitives::types::{AccountId, Balance, PromiseId, ReceiptId, StorageUsage};
use std::collections::btree_map::BTreeMap;
use std::fs;
use std::path::PathBuf;
use wasm::executor;
use wasm::ext::{Error as ExtError, External, Result as ExtResult};
use wasm::types::{Config, ContractCode, Error, ReturnData, RuntimeContext, RuntimeError};

#[derive(Default)]
struct MyExt {
    storage: BTreeMap<Vec<u8>, Vec<u8>>,
    num_receipts: u32,
}

fn generate_promise_id(index: u32) -> ReceiptId {
    [index as u8; 32].to_vec()
}

impl External for MyExt {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> ExtResult<Option<Vec<u8>>> {
        println!("PUT '{:?}' -> '{:?}'", key, value);
        let evicted = self.storage.insert(Vec::from(key), Vec::from(value));
        if let Some(evicted) = evicted.as_ref() {
            println!("EVICTED '{:?}' -> '{:?}'", key, evicted);
        }
        Ok(evicted)
    }

    fn storage_get(&self, key: &[u8]) -> ExtResult<Option<Vec<u8>>> {
        let value = self.storage.get(key);
        match value {
            Some(buf) => {
                println!("GET '{:?}' -> '{:?}'", key, buf);
                Ok(Some(buf.to_vec()))
            }
            None => {
                println!("GET '{:?}' -> EMPTY", key);
                Ok(None)
            }
        }
    }

    fn storage_remove(&mut self, key: &[u8]) -> ExtResult<Option<Vec<u8>>> {
        let removed = self.storage.remove(key);
        if let Some(removed) = removed.as_ref() {
            println!("REMOVE '{:?}' -> '{:?}'", key, removed);
        } else {
            println!("REMOVE '{:?}' -> EMPTY", key);
        }
        Ok(removed)
    }

    fn storage_iter(&mut self, _prefix: &[u8]) -> ExtResult<u32> {
        Err(ExtError::NotImplemented)
    }

    fn storage_range(&mut self, _start: &[u8], _end: &[u8]) -> ExtResult<u32> {
        Err(ExtError::NotImplemented)
    }

    fn storage_iter_next(&mut self, _iter: u32) -> ExtResult<Option<Vec<u8>>> {
        Err(ExtError::NotImplemented)
    }

    fn storage_iter_peek(&mut self, _iter: u32) -> ExtResult<Option<Vec<u8>>> {
        Err(ExtError::NotImplemented)
    }

    fn storage_iter_remove(&mut self, _iter: u32) {}

    fn promise_create(
        &mut self,
        account_id: AccountId,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _amount: Balance,
    ) -> ExtResult<PromiseId> {
        match self.num_receipts {
            0 => assert_eq!(&account_id, &"test1".to_string()),
            1 => assert_eq!(&account_id, &"test2".to_string()),
            _ => (),
        };
        self.num_receipts += 1;
        Ok(PromiseId::Receipt(generate_promise_id(self.num_receipts - 1)))
    }

    fn promise_then(
        &mut self,
        promise_id: PromiseId,
        _method_name: Vec<u8>,
        _arguments: Vec<u8>,
        _amount: Balance,
    ) -> ExtResult<PromiseId> {
        match promise_id {
            PromiseId::Receipt(_) => Err(ExtError::WrongPromise),
            PromiseId::Joiner(v) => {
                assert_eq!(v[0], generate_promise_id(0));
                assert_eq!(v[1], generate_promise_id(1));
                Ok(PromiseId::Callback(b"call_it_please".to_vec()))
            }
            _ => Err(ExtError::WrongPromise),
        }
    }

    fn check_ethash(
        &mut self,
        _block_number: u64,
        _header_hash: &[u8],
        _nonce: u64,
        _mix_hash: &[u8],
        _difficulty: u64,
    ) -> bool {
        false
    }
}

fn runtime_context(
    balance: Balance,
    amount: Balance,
    storage_usage: StorageUsage,
) -> RuntimeContext {
    RuntimeContext::new(
        balance,
        amount,
        &"alice.near".to_string(),
        &"bob".to_string(),
        storage_usage,
        123,
        b"yolo".to_vec(),
        false,
    )
}

#[test]
fn test_rust_api() {
    let num_simulations = 3;
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("./test-contract/pkg/test_contract_bg.wasm");

    let wasm_binary = fs::read(path).expect("Unable to read file");
    let code = ContractCode::new(wasm_binary);

    let mut ext = MyExt::default();
    let config = Config::default();
    let context = runtime_context(0, 1_000_000_000, 0);

    let method_name = b"add_agent";
    let result =
        executor::execute(&code, method_name, &[], &[], &mut ext, &config, &context).unwrap();

    for _ in 0..num_simulations {
        let args = br#"{"account_id":"alice.near"}"#;
        let method_name = b"simulate";
        executor::execute(&code, method_name, args, &[], &mut ext, &config, &context).unwrap();
    }

    let args = br#"{"account_id":"alice.near","asset":"MissionTime"}"#;
    let method_name = b"assets_quantity";
    let result = executor::execute(&code, method_name, args, &[], &mut ext, &config, &context);
    if let ReturnData::Value(v) = result.unwrap().return_data.unwrap() {
        assert_eq!(format!("{}", num_simulations + 1), String::from_utf8(v).unwrap());
    } else {
        panic!()
    }
}

// Procedural macros is expected to generate the code similar to the following:
//#[no_mangle]
//pub extern "C" fn add_agent() {
//    let mut contract: MissionControl = read_state().unwrap_or_default();
//    let _result = contract.add_agent();
//    write_state(&contract);
//}
//
//#[no_mangle]
//pub extern "C" fn assets_quantity() {
//    let args: Value = serde_json::from_slice(&input_read()).unwrap();
//    let account_id: String = serde_json::from_value(args["account_id"].clone()).unwrap();
//    let asset: Asset = serde_json::from_value(args["asset"].clone()).unwrap();
//
//    let contract: MissionControl = read_state().unwrap_or_default();
//    let result = contract.assets_quantity(account_id, asset);
//    write_state(&contract);
//    let result = serde_json::to_vec(&result).unwrap();
//    unsafe {
//        return_value(result.len() as _, result.as_ptr());
//    }
//}
//
//#[no_mangle]
//pub extern "C" fn simulate() {
//    #[derive(Serialize, Deserialize)]
//    struct Args {
//        account_id: String,
//    }
//    let args: Args = serde_json::from_slice(&input_read()).unwrap();
//
//    let mut contract: MissionControl = read_state().unwrap_or_default();
//    let result = contract.simulate(args.account_id);
//    write_state(&contract);
//    let result = serde_json::to_vec(&result).unwrap();
//    unsafe {
//        return_value(result.len() as _, result.as_ptr());
//    }
//}
