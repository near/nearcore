#![recursion_limit = "128"]
extern crate proc_macro;
use proc_macro::TokenStream;
use quote::TokenStreamExt;
use quote::{quote, ToTokens};
use syn::export::TokenStream2;
use syn::{parse_macro_input, File, FnArg, ImplItem, ItemImpl, Pat, ReturnType, Visibility};

// For debugging.
#[proc_macro_attribute]
pub fn show_streams(attr: TokenStream, item: TokenStream) -> TokenStream {
    println!("attr: \"{}\"", attr.to_string());
    println!("item: \"{}\"", item.to_string());
    item
}

#[proc_macro_attribute]
pub fn near_contract(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input: ItemImpl = parse_macro_input!(item as ItemImpl);
    let binding = binding_file();
    let header = header_file();
    let mut output = quote! {
        #input
        #binding
        #header
    };
    //    let mut wrappers = vec![];
    // Type for which impl is called.
    let impl_type = input.self_ty.as_ref();
    for subitem in &input.items {
        if let ImplItem::Method(m) = subitem {
            if let Visibility::Public(_) = m.vis {
                // Method name.
                let method_name = &m.sig.ident;
                let mut out_args = quote! {};
                let mut method_args = quote! {};
                let mut is_mut = None;
                for arg in &m.sig.decl.inputs {
                    match arg {
                        FnArg::SelfRef(r) => {
                            is_mut = Some(r.mutability.is_some());
                        }
                        FnArg::Captured(c) => {
                            let ident = if let Pat::Ident(ident) = &c.pat {
                                ident
                            } else {
                                panic!("Unsupported argument type")
                            };
                            let ident_quoted = quote! { #ident }.to_string();
                            let out_arg = quote! {
                                let #c = serde_json::from_value(args[#ident_quoted].clone()).unwrap();
                            };
                            out_args = quote! { #out_args #out_arg };
                            method_args = quote! { #method_args #ident ,};
                        }
                        _ => panic!("Unsupported argument type"),
                    }
                }
                // If any args were found then add the parsing function.
                let args_parsing = if !out_args.is_empty() {
                    quote! {
                    let args: serde_json::Value = serde_json::from_slice(&input_read()).unwrap();
                    }
                } else {
                    quote! {}
                };
                let method_call = quote! {
                    let mut contract: #impl_type = read_state().unwrap_or_default();
                    let result = contract.#method_name(#method_args);
                };

                // Only process methods that are &self or &mut self.
                let is_mut = if let Some(is_mut) = is_mut { is_mut } else { continue };
                // If method mutates the state then record it.
                let write_state = if is_mut {
                    quote! { write_state(&contract); }
                } else {
                    quote! {}
                };

                // If the function returns something then return it.
                let return_value = if let ReturnType::Type(_, _) = &m.sig.decl.output {
                    quote! {
                     let result = serde_json::to_vec(&result).unwrap();
                     unsafe {
                         return_value(result.len() as _, result.as_ptr());
                     }
                    }
                } else {
                    quote! {}
                };
                let body = quote! {
                 #args_parsing
                 #out_args
                 #method_call
                 #write_state
                 #return_value
                };
                let full_method = quote! {
                    #[no_mangle]
                    pub extern "C" fn #method_name() {
                    #body
                    }
                };
                output = quote! { #output #full_method };
            }
        }
    }
    TokenStream::from(output)
}

fn binding_file() -> File {
    let data = include_bytes!("../res/binding.rs");
    let data = std::str::from_utf8(data).unwrap();
    syn::parse_file(data).unwrap()
}

fn header_file() -> File {
    let data = include_bytes!("../res/header.rs");
    let data = std::str::from_utf8(data).unwrap();
    syn::parse_file(data).unwrap()
}
