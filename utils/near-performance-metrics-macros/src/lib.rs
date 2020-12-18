extern crate proc_macro;
extern crate syn;

use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_attribute]
pub fn perf(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: syn::Item = syn::parse(item).expect("failed to parse input");

    if let syn::Item::Fn(mut func) = item.clone() {
        let block = func.clone().block;

        let function_body = quote! { #block };

        let new_body: TokenStream = quote! {
            fn xxx() {
                use near_performance_metrics::stats::measure_performance;
                near_performance_metrics::stats::measure_performance(std::any::type_name::<Self>(), msg, move |msg| {
                    #function_body
                })
            }
        }.into();

        if let syn::Item::Fn(func2) = syn::parse(new_body).expect("failed to parse input") {
            func.block = func2.block;
        } else {
            panic!("failed to parse example function");
        }

        let result_item = syn::Item::Fn(func);
        let output = quote! { #result_item };
        output.into()
    } else {
        panic!("not a function {:?}");
    }
}
