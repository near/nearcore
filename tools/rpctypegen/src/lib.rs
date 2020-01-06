extern crate proc_macro;
extern crate proc_macro2;
use proc_macro::TokenStream;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use syn::{
    parse_macro_input, Data, DataEnum, DataStruct, DeriveInput, Fields, FieldsNamed, FieldsUnnamed,
    Lit, Meta, MetaNameValue,
};

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

#[derive(Default, Debug, Deserialize, Serialize)]
struct Schema {
    pub schema: HashMap<String, ErrorType>,
}

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

#[derive(Default, Debug, Deserialize, Serialize)]
struct ErrorType {
    /// A type name of the error
    pub name: String,
    /// Names of subtypes of the error
    pub subtypes: Vec<String>,
    /// An error input name and a type
    pub props: HashMap<String, String>,
}

thread_local!(static SCHEMA: RefCell<Schema> = RefCell::new(Schema::default()));

fn parse_rpc_error_variant(input: &DeriveInput) -> Option<String> {
    input.attrs.iter().find_map(|attr| {
        if !attr.path.is_ident("rpc_error_variant") {
            return None;
        }
        match attr.parse_meta().unwrap() {
            Meta::NameValue(MetaNameValue { lit: Lit::Str(lit), .. }) => Some(lit.value()),
            _ => None,
        }
    })
}

fn error_type_name<'a>(
    schema: &'a mut HashMap<String, ErrorType>,
    name: String,
) -> &'a mut ErrorType {
    let error_type = ErrorType { name: name.clone(), ..Default::default() };
    schema.entry(name.clone()).or_insert(error_type)
}

fn parse_error_type(schema: &mut HashMap<String, ErrorType>, input: &DeriveInput) {
    let name = parse_rpc_error_variant(input).expect("should have a rpc_error_variant with value");
    match &input.data {
        Data::Enum(DataEnum { ref variants, .. }) => {
            let error_type = error_type_name(schema, name);
            let mut direct_error_types = vec![];
            for variant in variants {
                error_type.subtypes.push(variant.ident.to_string());
                match &variant.fields {
                    Fields::Unnamed(FieldsUnnamed { ref unnamed, .. }) => {
                        // Subtype
                        if unnamed.iter().count() > 1 {
                            panic!(
                                "Error types doesn't support tuple variants with multiple fields"
                            );
                        }
                    }
                    Fields::Named(FieldsNamed { ref named, .. }) => {
                        // If variant is Enum with a named fields - create a new type for each variant with named props
                        let mut error_type = ErrorType::default();
                        error_type.name = variant.ident.to_string();
                        for field in named {
                            error_type.props.insert(
                                field
                                    .ident
                                    .as_ref()
                                    .expect("named fields must have ident")
                                    .to_string(),
                                "".to_owned(),
                            ); // TODO: add type?
                        }
                        direct_error_types.push(error_type);
                    }
                    Fields::Unit => {
                        direct_error_types.push(ErrorType {
                            name: variant.ident.to_string(),
                            ..Default::default()
                        });
                    }
                }
            }
            for e in direct_error_types {
                let mut error_type = error_type_name(schema, e.name.clone());
                error_type.name = e.name;
                error_type.props = e.props;
            }
        }
        Data::Struct(DataStruct { ref fields, .. }) => {
            let error_type = error_type_name(schema, name);
            match fields {
                Fields::Named(FieldsNamed { ref named, .. }) => {
                    for field in named {
                        let field_name =
                            field.ident.as_ref().expect("named fields must have ident").to_string();
                        if field_name == "kind" {
                            continue;
                        }
                        error_type.props.insert(field_name, "".to_owned()); // TODO: add prop type
                    }
                }
                _ => {
                    panic!("RpcError supports structs with the named fields only");
                }
            }
        }
        Data::Union(_) => {
            panic!("Unions are not supported");
        }
    }
}

#[proc_macro_derive(RpcError, attributes(rpc_error_variant))]
pub fn rpc_error(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    SCHEMA.with(|s| {
        parse_error_type(&mut s.borrow_mut().schema, &input);
    });
    TokenStream::new()
}
