const SERIALIZED_STATE: &str = "STATE";

fn read_state<T: serde::de::DeserializeOwned>() -> Option<T> {
    let arr = SERIALIZED_STATE.as_bytes();
    if !unsafe { storage_has_key(arr.len() as _, arr.as_ptr()) } {
        return None;
    }
    let data = storage_read(arr.len() as _, arr.as_ptr());
    bincode::deserialize(&data).ok()
}

fn write_state<T: serde::Serialize>(state: &T) {
    let arr = SERIALIZED_STATE.as_bytes();
    let data = bincode::serialize(state).unwrap();
    unsafe {
        storage_write(arr.len() as _, arr.as_ptr(), data.len() as _, data.as_ptr());
    }
}
