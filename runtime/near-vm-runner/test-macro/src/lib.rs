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
    let params = &input.sig.inputs;

    // Split attributes: test-specific attrs like #[should_panic] and #[ignore]
    // go only on the #[test] fns, while general attrs like #[allow(...)] go on
    // both the impl fn and the test fns.
    let mut impl_attrs = Vec::new();
    let mut test_attrs = Vec::new();
    for attr in &input.attrs {
        let is_test_attr = attr.path().is_ident("should_panic") || attr.path().is_ident("ignore");
        if is_test_attr {
            test_attrs.push(attr);
        } else {
            impl_attrs.push(attr);
            test_attrs.push(attr);
        }
    }

    let expanded = quote! {
        #(#impl_attrs)*
        fn #impl_name(#params) #body

        #(#test_attrs)*
        #[test]
        fn #name() {
            #impl_name(Backend::Legacy);
        }

        #(#test_attrs)*
        #[test]
        fn #wasmtime_name() {
            #impl_name(Backend::Wasmtime);
        }
    };

    TokenStream::from(expanded)
}
