#[proc_macro_attribute]
pub fn must_complete(
    _args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item = syn::parse::<syn::ItemFn>(item).unwrap();
    let syn::Signature {
        output,
        inputs,
        asyncness,
        constness,
        unsafety,
        abi,
        ident,
        generics: syn::Generics {
           params,
           where_clause,
           ..
        },
        ..
    } = item.sig;
    let vis = item.vis;
    let block = item.block;
    let output = match output {
        syn::ReturnType::Default => Box::new(syn::Type::from(syn::TypeTuple{
            paren_token: syn::token::Paren{span:proc_macro2::Span::call_site()},
            elems: syn::punctuated::Punctuated::new(),
        })),
        syn::ReturnType::Type(_,t) => t,
    };
    if asyncness.is_none() {
        panic!("#[must_complete] can only be applied to async functions");
    }
    proc_macro::TokenStream::from(quote::quote!(
        #vis #constness #unsafety #abi fn #ident<#params>(#inputs) -> impl std::future::Future<Output=#output>
        #where_clause
        {
            near_concurrency::must_complete(async move #block)
        }
    ))
}

