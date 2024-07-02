mod protocol_info;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemStruct};

#[proc_macro_attribute]
pub fn protocol_struct(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemStruct);
    let expanded = protocol_struct_impl(input);
    TokenStream::from(expanded)
}

#[cfg(feature = "check_protocol_structs")]
fn protocol_struct_impl(input: ItemStruct) -> proc_macro2::TokenStream {
    let name = &input.ident;
    let name_str = name.to_string();
    let info_ident = syn::Ident::new(&format!("__PROTOCOL_STRUCT_INFO_{}", name), name.span());

    quote! {
        #input

        #[cfg(feature = "check_protocol_structs")]
        #[doc(hidden)]
        #[allow(non_upper_case_globals)]
        static #info_ident: protocol_macros::protocol_info::ProtocolStructInfo = protocol_macros::protocol_info::ProtocolStructInfo {
            name: #name_str,
            hash: protocol_macros::protocol_info::calculate_struct_hash::<#name>(),
        };

        #[cfg(feature = "check_protocol_structs")]
        inventory::submit!(&#info_ident);
    }
}

#[cfg(not(feature = "check_protocol_structs"))]
fn protocol_struct_impl(input: ItemStruct) -> proc_macro2::TokenStream {
    quote! { #input }
}

// Re-export ProtocolStructInfo and calculate_struct_hash
pub use protocol_info::*;
