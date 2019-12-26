extern crate proc_macro;
extern crate proc_macro2;
use proc_macro::TokenStream;
use proc_macro2::Literal;
use quote::quote;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, DeriveInput, Lit, Meta, MetaNameValue, Data, DataEnum, Fields, FieldsUnnamed};

#[derive(Default, Debug, Deserialize, Serialize)]
struct ErrorType {
    /// A type name of the error
    pub name: String,
    /// Names of subtypes of the error
    pub subtypes: Vec<String>,
    // /// An error input
    pub props: HashMap<String, String>,
}

thread_local!(static SCHEMA: RefCell<HashMap<String, Vec<ErrorType>>> = RefCell::new(HashMap::new()));

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

fn parse_error_type(input: &DeriveInput) -> ErrorType {
    let mut error_type = ErrorType::default();
    error_type.name = parse_rpc_error_variant(input).expect("should have a rpc_error_variant with value");
    match input.data {
        // If Variant is a NewType add to subtypes
        // - if Variant is a struct-variant, create a new
        Data::Enum(DataEnum{ variants, .. }) => {
            for variant in variants {
                match variant.fields {
                    Fields::Unnamed(  ) => {

                    }
                }
            }
            error_type.subtypes = variants.iter().map(|variant| {
                variant.fields.iter
                match variant.fields {

                }
            })
        },
        Body::Struct(VariantData::Struct( fields )) => {

            // for field in fields {
            //     println!("{:?}", field.);
            // }
            // error_type.props = fields.iter().collect::<Option<Vec<_>>>();
        }
    }
    error_type
}

#[proc_macro_derive(RpcError, attributes(rpc_error_variant))]
pub fn rpc_error(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    SCHEMA.with(|s| {
        s.borrow_mut().insert(format!("{}", parent_name.unwrap()), Default::default());
    });

    SCHEMA.with(|s| {
        println!("{:?}", s.borrow());
    });

    println!("{:?}", parse_parent_name(&input));

    TokenStream::new()
}
