use crate::asset::Asset;
use crate::binding::{input_read, return_value, storage_has_key, storage_read, storage_write};
use crate::mission_control::MissionControl;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

const SERIALIZED_STATE: &str = "STATE";

fn read_state<T: DeserializeOwned>() -> Option<T> {
    let arr = SERIALIZED_STATE.as_bytes();
    if !unsafe { storage_has_key(arr.len() as _, arr.as_ptr()) } {
        return None;
    }
    let data = storage_read(arr.len() as _, arr.as_ptr());
    bincode::deserialize(&data).ok()
}

fn write_state<T: Serialize>(state: &T) {
    let arr = SERIALIZED_STATE.as_bytes();
    let data = bincode::serialize(state).unwrap();
    unsafe {
        storage_write(arr.len() as _, arr.as_ptr(), data.len() as _, data.as_ptr());
    }
}

#[no_mangle]
pub extern "C" fn add_agent() {
    let mut contract: MissionControl = read_state().unwrap_or_default();
    let _result = contract.add_agent();
    write_state(&contract);
}

#[no_mangle]
pub extern "C" fn assets_quantity() {
    let args: Value = serde_json::from_slice(&input_read()).unwrap();
    let account_id: String = serde_json::from_value(args["account_id"].clone()).unwrap();
    let asset: Asset = serde_json::from_value(args["asset"].clone()).unwrap();

    let contract: MissionControl = read_state().unwrap_or_default();
    let result = contract.assets_quantity(account_id, asset);
    write_state(&contract);
    let result = serde_json::to_vec(&result).unwrap();
    unsafe {
        return_value(result.len() as _, result.as_ptr());
    }
}

#[no_mangle]
pub extern "C" fn simulate() {
    #[derive(Serialize, Deserialize)]
    struct Args {
        account_id: String,
    }
    let args: Args = serde_json::from_slice(&input_read()).unwrap();

    let mut contract: MissionControl = read_state().unwrap_or_default();
    let result = contract.simulate(args.account_id);
    write_state(&contract);
    let result = serde_json::to_vec(&result).unwrap();
    unsafe {
        return_value(result.len() as _, result.as_ptr());
    }
}
