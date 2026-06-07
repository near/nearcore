use std::sync::{Arc, Mutex};
use std::collections::HashMap;

// Define a thread-safe data structure for storing runtime data
pub struct RuntimeData {
    pub data: Arc<Mutex<HashMap<String, String>>>,
}

impl RuntimeData {
    pub fn new() -> Self {
        RuntimeData {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn insert(&self, key: String, value: String) {
        let mut data = self.data.lock().unwrap();
        data.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let data = self.data.lock().unwrap();
        data.get(key).cloned()
    }
}

// Define a function to prevent race conditions
pub fn prevent_race_condition(data: &RuntimeData, key: &str, value: &str) {
    let mut data_lock = data.data.lock().unwrap();
    if !data_lock.contains_key(key) {
        data_lock.insert(key.to_string(), value.to_string());
    }
}

// Example usage:
pub fn main() {
    let runtime_data = RuntimeData::new();
    runtime_data.insert("key1".to_string(), "value1".to_string());
    println!("{:?}", runtime_data.get("key1"));
    prevent_race_condition(&runtime_data, "key2", "value2");
    println!("{:?}", runtime_data.get("key2"));
}