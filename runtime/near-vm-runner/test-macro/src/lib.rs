use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

/// Attribute macro that generates two test functions from a single test
/// implementation: one using the legacy VMLogic backend and one using the
/// wasmtime backend.
///
/// The function must take a single `backend: Backend` parameter.
///
/// Usage:
/// ```ignore
/// #[vm_test]
/// fn test_sha256(backend: Backend) {
///     let mut logic_builder = VMLogicBuilder::default();
///     let mut logic = logic_builder.build(backend);
///     let data = logic.internal_mem_write(b"tesdsst");
///     logic.sha256(data.len, data.ptr, 0).unwrap();
/// }
/// ```
///
/// Expands to:
/// ```ignore
/// fn test_sha256_impl(backend: Backend) { ... }
///
/// #[test]
/// fn test_sha256() {
///     test_sha256_impl(Backend::Legacy);
/// }
///
/// #[test]
/// fn test_sha256_wasmtime() {
///     test_sha256_impl(Backend::Wasmtime);
/// }
/// ```
#[proc_macro_attribute]
pub fn vm_test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);
    let name = &input.sig.ident;
    let impl_name = syn::Ident::new(&format!("{}_impl", name), name.span());
    let wasmtime_name = syn::Ident::new(&format!("{}_wasmtime", name), name.span());
    let body = &input.block;
    let attrs = &input.attrs;
    let params = &input.sig.inputs;

    let expanded = quote! {
        #(#attrs)*
        fn #impl_name(#params) #body

        #(#attrs)*
        #[test]
        fn #name() {
            #impl_name(Backend::Legacy);
        }

        #(#attrs)*
        #[test]
        fn #wasmtime_name() {
            #impl_name(Backend::Wasmtime);
        }
    };

    TokenStream::from(expanded)
}
