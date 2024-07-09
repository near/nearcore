use proc_macro::TokenStream;
use quote::ToTokens;
use quote::{format_ident, quote};
use syn::{parse_macro_input, Data, DeriveInput, Fields, FieldsNamed, FieldsUnnamed, Variant};

#[proc_macro_derive(ProtocolStruct)]
pub fn protocol_struct(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let info_name = format_ident!("{}_INFO", name);

    let info = match &input.data {
        Data::Struct(data_struct) => {
            let fields = extract_struct_fields(&data_struct.fields);
            quote! {
                near_structs_checker_core::ProtocolStructInfo::Struct {
                    name: stringify!(#name),
                    fields: &[#(#fields),*],
                }
            }
        }
        Data::Enum(data_enum) => {
            let variants = extract_enum_variants(&data_enum.variants);
            quote! {
                near_structs_checker_core::ProtocolStructInfo::Enum {
                    name: stringify!(#name),
                    variants: &[#(#variants),*],
                }
            }
        }
        Data::Union(_) => panic!("Unions are not supported"),
    };

    let expanded = quote! {
        #[allow(non_upper_case_globals)]
        static #info_name: near_structs_checker_core::ProtocolStructInfo = #info;

        inventory::submit!(#info_name);

        impl near_structs_checker_core::ProtocolStruct for #name {}
    };

    TokenStream::from(expanded)
}

fn extract_struct_fields(fields: &Fields) -> Vec<proc_macro2::TokenStream> {
    match fields {
        Fields::Named(FieldsNamed { named, .. }) => named
            .iter()
            .map(|f| {
                let name = &f.ident;
                let ty = &f.ty;
                quote! { (stringify!(#name), stringify!(#ty)) }
            })
            .collect(),
        Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => unnamed
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let ty = &f.ty;
                quote! { (stringify!(#i), stringify!(#ty)) }
            })
            .collect(),
        Fields::Unit => vec![],
    }
}

fn extract_enum_variants(
    variants: &syn::punctuated::Punctuated<Variant, syn::token::Comma>,
) -> Vec<proc_macro2::TokenStream> {
    variants
        .iter()
        .map(|v| {
            let name = &v.ident;
            let fields = match &v.fields {
                Fields::Named(FieldsNamed { named, .. }) => {
                    let fields: Vec<_> = named
                        .iter()
                        .map(|f| {
                            let name = &f.ident;
                            let ty = &f.ty;
                            quote! { (stringify!(#name), stringify!(#ty)) }
                        })
                        .collect();
                    quote! { Some(&[#(#fields),*]) }
                }
                Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => {
                    let fields: Vec<_> = unnamed
                        .iter()
                        .enumerate()
                        .map(|(i, f)| {
                            let ty = &f.ty;
                            quote! { (stringify!(#i), stringify!(#ty)) }
                        })
                        .collect();
                    quote! { Some(&[#(#fields),*]) }
                }
                Fields::Unit => quote! { None },
            };
            quote! { (stringify!(#name), #fields) }
        })
        .collect()
}
