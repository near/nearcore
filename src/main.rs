mod runtime;

use runtime::*;

fn main() {
    let runtime_data = RuntimeData::new();
    runtime_data.insert("key1".to_string(), "value1".to_string());
    println!("{:?}", runtime_data.get("key1"));
    prevent_race_condition(&runtime_data, "key2", "value2");
    println!("{:?}", runtime_data.get("key2"));
}