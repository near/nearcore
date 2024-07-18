use proc_macro::TokenStream;

#[proc_macro_derive(ProtocolStruct)]
pub fn protocol_struct(input: TokenStream) -> TokenStream {
    helper::protocol_struct_impl(input)
}

#[cfg(all(enable_const_type_id, feature = "protocol_schema"))]
mod helper {
    use proc_macro::TokenStream;
    use proc_macro2::TokenStream as TokenStream2;
    use quote::quote;
    use syn::{parse_macro_input, Data, DeriveInput, Fields, FieldsNamed, FieldsUnnamed, Variant};

    pub fn protocol_struct_impl(input: TokenStream) -> TokenStream {
        let input = parse_macro_input!(input as DeriveInput);
        let name = &input.ident;
        let info_name = quote::format_ident!("{}_INFO", name);

        let type_id = quote! { std::any::TypeId::of::<#name>() };
        let info = match &input.data {
            Data::Struct(data_struct) => {
                let fields = extract_struct_fields(&data_struct.fields);
                quote! {
                    near_structs_checker_lib::ProtocolStructInfo::Struct {
                        name: stringify!(#name),
                        type_id: #type_id,
                        fields: #fields,
                    }
                }
            }
            Data::Enum(data_enum) => {
                let variants = extract_enum_variants(&data_enum.variants);
                quote! {
                    near_structs_checker_lib::ProtocolStructInfo::Enum {
                        name: stringify!(#name),
                        type_id: #type_id,
                        variants: #variants,
                    }
                }
            }
            Data::Union(_) => panic!("Unions are not supported"),
        };

        let expanded = quote! {
            #[allow(non_upper_case_globals)]
            pub static #info_name: near_structs_checker_lib::ProtocolStructInfo = #info;

            near_structs_checker_lib::inventory::submit! {
                #info_name
            }

            impl near_structs_checker_lib::ProtocolStruct for #name {}
        };

        TokenStream::from(expanded)
    }

    fn extract_struct_fields(fields: &Fields) -> TokenStream2 {
        match fields {
            Fields::Named(FieldsNamed { named, .. }) => {
                let fields = extract_from_named_fields(named);
                quote! { &[#(#fields),*] }
            }
            Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => {
                let fields = extract_from_unnamed_fields(unnamed);
                quote! { &[#(#fields),*] }
            }
            Fields::Unit => quote! { &[] },
        }
    }

    fn extract_enum_variants(
        variants: &syn::punctuated::Punctuated<Variant, syn::token::Comma>,
    ) -> TokenStream2 {
        let variants = variants.iter().map(|v| {
            let name = &v.ident;
            let fields = match &v.fields {
                Fields::Named(FieldsNamed { named, .. }) => {
                    let fields = extract_from_named_fields(named);
                    quote! { Some(&[#(#fields),*]) }
                }
                Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => {
                    let fields = extract_from_unnamed_fields(unnamed);
                    quote! { Some(&[#(#fields),*]) }
                }
                Fields::Unit => quote! { None },
            };
            quote! { (stringify!(#name), #fields) }
        });
        quote! { &[#(#variants),*] }
    }

    fn extract_from_named_fields(
        named: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
    ) -> std::iter::Map<
        syn::punctuated::Iter<syn::Field>,
        fn(&syn::Field) -> proc_macro2::TokenStream,
    > {
        named.iter().map(|f| {
            let name = &f.ident;
            let ty = &f.ty;
            quote! { (stringify!(#name), std::any::TypeId::of::<#ty>()) }
        })
    }

    fn extract_from_unnamed_fields(
        unnamed: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
    ) -> std::iter::Map<
        std::iter::Enumerate<syn::punctuated::Iter<syn::Field>>,
        fn((usize, &syn::Field)) -> proc_macro2::TokenStream,
    > {
        unnamed.iter().enumerate().map(|(i, f)| {
            let index = syn::Index::from(i);
            let ty = &f.ty;
            quote! { (stringify!(#index), std::any::TypeId::of::<#ty>()) }
        })
    }
}

#[cfg(not(all(enable_const_type_id, feature = "protocol_schema")))]
mod helper {
    use proc_macro::TokenStream;

    pub fn protocol_struct_impl(_input: TokenStream) -> TokenStream {
        TokenStream::new()
    }
}
