use super::*;
use std::thread;

#[test]
fn test_runtime_data() {
    let runtime_data = RuntimeData::new();
    runtime_data.insert("key1".to_string(), "value1".to_string());
    assert_eq!(runtime_data.get("key1"), Some("value1".to_string()));
}

#[test]
fn test_prevent_race_condition() {
    let runtime_data = RuntimeData::new();
    let key = "key2";
    let value = "value2";
    let handle = thread::spawn(move || {
        prevent_race_condition(&runtime_data, key, value);
    });
    handle.join().unwrap();
    assert_eq!(runtime_data.get(key), Some(value.to_string()));
}