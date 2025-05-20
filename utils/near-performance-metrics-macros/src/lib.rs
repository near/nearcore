extern crate proc_macro;
extern crate syn;

use proc_macro::TokenStream;
use quote::quote;

/// Wrap the method call with near_performance_metrics::stats::measure_performance function.
///
/// This derive can be used to provide performance metrics to method calls with Actors. Currently
/// we print performance stats per thread every minute, and we print a warning whenever a function
/// call exceeds took more than given time limit. It should have no performance impact unless
/// `performance_stats` feature is enabled.
///
/// This function assumes it wraps around a method with `&mut self, msg: NetworkClientMessages,
/// ctx: &mut Self::Context<Self>` as arguments. There is currently a requirement that the second
/// argument is called msg.
///
/// # Examples
/// ```ignore
///
/// pub enum MyMessage {
///      ExampleMessage()
/// }
///
/// pub struct ExampleResponse {}
/// use actix::Context;
/// impl Handler<NetworkClientMessages> for ClientActor {
///    type Result = ExampleResponse;
///
///    #[perf]
///    fn handle(&mut self, msg: NetworkClientMessages, ctx: &mut Self::Context<Self>) -> Self::Result {
///        ExampleResponse{}
///    }
/// }
/// ```
#[proc_macro_attribute]
pub fn perf(_attr: TokenStream, item: TokenStream) -> TokenStream {
    perf_internal(_attr, item, false)
}

/// Wrap the method call with near_performance_metrics::stats::measure_performance_with_debug function.
///
/// This derive can be used to provide performance metrics to method calls with Actors. Currently
/// we print performance stats per thread every minute, and we print a warning whenever a function
/// call exceeds took more than given time limit. It should have no performance impact unless
/// `performance_stats` feature is enabled. In addition to prints provided by `perf`,
/// `perf_with_debug` prints enum variant type of the message.
///
/// This function assumes it wraps around a method with `&mut self, msg: NetworkClientMessages,
/// ctx: &mut Self::Context<Self>` as arguments. There is currently a requirement that the second
/// argument is called msg. There is an assumption that the argument called `msg` is an enum, which
/// has `#[derive(AsRefStr)]`.
///
/// # Examples
/// ```ignore
/// #[derive(strum::AsRefStr)]
/// pub enum MyMessage {
///      ExampleMessage()
/// }
///
/// pub struct ExampleResponse {}
/// impl Handler<NetworkClientMessages> for ClientActor {
///    type Result = ExampleResponse;
///
///    #[perf_with_debug]
///    fn handle(&mut self, msg: NetworkClientMessages, ctx: &mut Self::Context<Self>) -> Self::Result {
///        ExampleResponse{}
///    }
/// }
/// ```
#[proc_macro_attribute]
pub fn perf_with_debug(_attr: TokenStream, item: TokenStream) -> TokenStream {
    perf_internal(_attr, item, true)
}

fn perf_internal(_attr: TokenStream, item: TokenStream, debug: bool) -> TokenStream {
    let item: syn::Item = syn::parse(item).expect("failed to parse input");

    if let syn::Item::Fn(mut func) = item {
        let function_body = func.block;

        let new_body: TokenStream = if debug {
            quote! (
                {
                    near_performance_metrics::stats::measure_performance_with_debug(std::any::type_name::<Self>(), msg, move |msg| {
                        #function_body
                    })
                }
            ).into()
        } else {
            quote! (
                {
                    near_performance_metrics::stats::measure_performance(std::any::type_name::<Self>(), (), move |_| {
                        #function_body
                    })
                }
             ).into()
        };
        func.block = syn::parse::<syn::Block>(new_body).expect("failed to parse input").into();

        quote! ( #func ).into()
    } else {
        panic!("not a function");
    }
}
