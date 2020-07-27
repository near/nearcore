use std::cell::RefCell;
use std::collections::BTreeMap;

use proc_macro::TokenStream;
use serde::{Deserialize, Serialize};
#[cfg(feature = "dump_errors_schema")]
use serde_json::Value;
use syn::{parse_macro_input, DeriveInput};

use near_rpc_error_core::{parse_error_type, ErrorType};

thread_local!(static SCHEMA: RefCell<Schema> = RefCell::new(Schema::default()));

#[derive(Default, Debug, Deserialize, Serialize)]
struct Schema {
    pub schema: BTreeMap<String, ErrorType>,
}

#[cfg(feature = "dump_errors_schema")]
fn merge(a: &mut Value, b: &Value) {
    match (a, b) {
        (&mut Value::Object(ref mut a), &Value::Object(ref b)) => {
            for (k, v) in b {
                merge(a.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (a, b) => {
            *a = b.clone();
        }
    }
}

#[cfg(feature = "dump_errors_schema")]
impl Drop for Schema {
    fn drop(&mut self) {
        // std::env::var("CARGO_TARGET_DIR") doesn't exists
        let filename = "./target/rpc_errors_schema.json";
        let schema_json = serde_json::to_value(self).expect("Schema serialize failed");
        let new_schema_json = if let Ok(data) = std::fs::read(filename) {
            // merge to the existing file
            let mut existing_schema = serde_json::from_slice::<Value>(&data)
                .expect("cannot deserialize target/existing_schema.json");
            merge(&mut existing_schema, &schema_json);
            existing_schema
        } else {
            schema_json
        };
        let new_schema_json_string = serde_json::to_string_pretty(&new_schema_json)
            .expect("error schema serialization failed");
        std::fs::write(filename, new_schema_json_string)
            .expect("Unable to save the errors schema file");
    }
}

#[proc_macro_derive(RpcError)]
pub fn rpc_error(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    SCHEMA.with(|s| {
        parse_error_type(&mut s.borrow_mut().schema, &input);
    });
    TokenStream::new()
}
