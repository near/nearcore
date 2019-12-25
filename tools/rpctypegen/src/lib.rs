extern crate proc_macro;
extern crate proc_macro2;
use proc_macro::TokenStream;
use proc_macro2::Literal;
use quote::quote;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, DeriveInput, Lit, Meta, MetaNameValue};

struct ErrorVariants {
    name: String,
}

#[derive(Default, Debug, Deserialize, Serialize)]
struct ErrorType {
    /// A type name of the error
    name: String,
    /// Names of subtypes of the error
    kinds: Vec<String>,
    // /// An error input
    props: Vec<String>,
}

thread_local!(static SCHEMA: RefCell<HashMap<String, Vec<ErrorType>>> = RefCell::new(HashMap::new()));

fn parse_parent_name(input: &DeriveInput) -> Option<String> {
    input.attrs.iter().find_map(|attr| {
        if !attr.path.is_ident("rpc_error_parent") {
            return None;
        }
        match attr.parse_meta().unwrap() {
            Meta::NameValue(MetaNameValue { lit: Lit::Str(lit), .. }) => Some(lit.value()),
            _ => None,
        }
    })
}

#[proc_macro_derive(RpcError, attributes(rpc_error_parent))]
pub fn rpc_error(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let parent_name = parse_parent_name(input);
    SCHEMA.with(|s| {
        s.borrow_mut().insert(format!("{}", parent_name), Default::default());
    });

    SCHEMA.with(|s| {
        println!("{:?}", s.borrow());
    });

    println!("{:?}", parse_parent_name(&input));

    TokenStream::new()
}
