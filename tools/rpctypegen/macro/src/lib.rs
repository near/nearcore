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
    /// `rpc_error` wants to collect **all** invocations of the macro across the
    /// project and merge them into a single file. These kinds of macros are not
    /// supported at all by Rust macro infrastructure, so we use gross hacks
    /// here.
    ///
    /// Every macro invocation merges its results into the
    /// rpc_errors_schema.json file, with the file playing the role of global
    /// mutable state which can be accessed from different processes. To avoid
    /// race conditions, we use file-locking. File locking isn't a very robust
    /// thing, but it should ok be considering the level of hack here.
    fn drop(&mut self) {
        use fs2::FileExt;
        use std::fs::File;
        use std::io::{Read, Seek, SeekFrom, Write};

        struct Guard {
            file: File,
        }
        impl Guard {
            fn new(path: &str) -> Self {
                let file = File::options()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .or_else(|_| File::options().read(true).write(true).open(path))
                    .unwrap_or_else(|err| panic!("can't open {path}: {err}"));
                file.lock_exclusive().unwrap_or_else(|err| panic!("can't lock {path}: {err}"));
                Guard { file }
            }
        }
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = self.file.unlock();
            }
        }

        let schema_json = serde_json::to_value(self).expect("Schema serialize failed");

        // std::env::var("CARGO_TARGET_DIR") doesn't exists
        let filename = "./target/rpc_errors_schema.json";
        let mut guard = Guard::new(filename);

        let existing_schema: Option<Value> = {
            let mut buf = Vec::new();
            guard
                .file
                .read_to_end(&mut buf)
                .unwrap_or_else(|err| panic!("can't read {filename}: {err}"));
            if buf.is_empty() {
                None
            } else {
                let json = serde_json::from_slice(&buf)
                    .unwrap_or_else(|err| panic!("can't deserialize {filename}: {err}"));
                Some(json)
            }
        };

        let new_schema_json = match existing_schema {
            None => schema_json,
            Some(mut existing_schema) => {
                merge(&mut existing_schema, &schema_json);
                existing_schema
            }
        };

        let new_schema_json_string = serde_json::to_string_pretty(&new_schema_json)
            .expect("error schema serialization failed");

        guard.file.set_len(0).unwrap_or_else(|err| panic!("can't truncate {filename}: {err}"));
        guard
            .file
            .seek(SeekFrom::Start(0))
            .unwrap_or_else(|err| panic!("can't seek {filename}: {err}"));
        guard
            .file
            .write_all(new_schema_json_string.as_bytes())
            .unwrap_or_else(|err| panic!("can't write {filename}: {err}"));
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
