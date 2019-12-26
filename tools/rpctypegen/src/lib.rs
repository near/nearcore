extern crate proc_macro;
extern crate proc_macro2;
use proc_macro::TokenStream;
use proc_macro2::Literal;
use quote::quote;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, DeriveInput, Lit, Meta, MetaNameValue, Data, DataEnum, DataStruct, Fields, FieldsUnnamed, FieldsNamed};

#[derive(Default, Debug, Deserialize, Serialize)]
struct ErrorType {
    /// A type name of the error
    pub name: String,
    /// Names of subtypes of the error
    pub subtypes: Vec<String>,
    // /// An error input name and type
    pub props: HashMap<String, String>,
}

thread_local!(static SCHEMA: RefCell<HashMap<String, ErrorType>> = RefCell::new(HashMap::new()));

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

fn error_type_unique_insert<'a>(schema: &'a mut HashMap<String, ErrorType>, name: String) -> &'a mut ErrorType {
        if schema.contains_key(&name) {
            panic!(format!("Error variant duplication ({})", name))
        }
        let error_type = ErrorType{ name: name.clone(), ..Default::default() };
        schema.insert(name.clone(), error_type);
        schema.get_mut(&name).unwrap()
}

fn parse_error_type(schema: &mut HashMap<String, ErrorType>, input: &DeriveInput) {
    let name = parse_rpc_error_variant(input).expect("should have a rpc_error_variant with value");
    let mut error_type = error_type_unique_insert(schema, name);
    let mut direct_error_types = vec![];
    match &input.data {
        // If Variant is a NewType add to subtypes
        // - if Variant is a struct-variant, create a new
        Data::Enum(DataEnum{ ref variants, .. }) => {
            for variant in variants {
                match &variant.fields {
                    Fields::Unnamed( FieldsUnnamed { ref unnamed, .. } ) => {
                        // Subtype
                        if unnamed.iter().count() > 1 {
                            panic!("Error types doesn't support tuple variants with multiple fields");
                        }
                        error_type.subtypes.push(variant.ident.to_string());
                    }
                    Fields::Named(FieldsNamed { ref named, .. }) => {
                        // If variant is Enum with a named fields - create a new type for each variant with named props
                        let mut error_type = ErrorType::default();
                        error_type.name = variant.ident.to_string();
                        for field in named {
                            error_type.props.insert(field.ident.as_ref().expect("named fields must have ident").to_string(), "".to_owned()); // TODO: add type
                        }
                        direct_error_types.push(error_type);
                    }
                    Fields::Unit => {
                        direct_error_types.push(ErrorType{name: variant.ident.to_string(), ..Default::default()});
                    }
                }
            }
        },
        Data::Struct(DataStruct{ fields, .. }) => {

        },
        Data::Union(_) => {
            panic!("Unions are not supported");
        }
    }
    for e in direct_error_types {
        let mut error_type = error_type_unique_insert(schema, e.name.clone());
        error_type.name = e.name;
        error_type.props = e.props;
    }
}

#[proc_macro_derive(RpcError, attributes(rpc_error_variant))]
pub fn rpc_error(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    SCHEMA.with(|s| {
        parse_error_type(&mut s.borrow_mut(), &input);
    });
    // SCHEMA.with(|s| {
    //     s.borrow_mut().insert(format!("{}", parent_name.unwrap()), Default::default());
    // });

    // SCHEMA.with(|s| {
    //     println!("{:?}", s.borrow());
    // });

    // println!("{:?}", parse_parent_name(&input));

    TokenStream::new()
}
