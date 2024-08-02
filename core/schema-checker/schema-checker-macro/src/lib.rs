use proc_macro::TokenStream;

#[proc_macro_derive(ProtocolSchema)]
pub fn protocol_schema(input: TokenStream) -> TokenStream {
    helper::protocol_schema_impl(input)
}

#[cfg(all(enable_const_type_id, feature = "protocol_schema"))]
mod helper {
    use proc_macro::TokenStream;
    use proc_macro2::TokenStream as TokenStream2;
    use quote::quote;
    use syn::{parse_macro_input, Data, DeriveInput, Fields, FieldsNamed, FieldsUnnamed, Variant};

    pub fn protocol_schema_impl(input: TokenStream) -> TokenStream {
        let input = parse_macro_input!(input as DeriveInput);
        let name = &input.ident;
        let info_name = quote::format_ident!("{}_INFO", name);

        let type_id = quote! { std::any::TypeId::of::<#name>() };
        let info = match &input.data {
            Data::Struct(data_struct) => {
                let fields = extract_struct_fields(&data_struct.fields);
                quote! {
                    near_schema_checker_lib::ProtocolSchemaInfo::Struct {
                        name: stringify!(#name),
                        type_id: #type_id,
                        fields: #fields,
                    }
                }
            }
            Data::Enum(data_enum) => {
                let variants = extract_enum_variants(&data_enum.variants);
                quote! {
                    near_schema_checker_lib::ProtocolSchemaInfo::Enum {
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
            pub static #info_name: near_schema_checker_lib::ProtocolSchemaInfo = #info;

            near_schema_checker_lib::inventory::submit! {
                #info_name
            }

            impl near_schema_checker_lib::ProtocolSchema for #name {}
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

    /// Extracts type ids from the type and **all** its underlying generic
    /// parameters, recursively.
    /// For example, for `Vec<Vec<u32>>` it will return `[Vec, Vec, u32]`.
    fn extract_type_ids_from_type(ty: &syn::Type) -> Vec<TokenStream2> {
        let mut result = vec![quote! { std::any::TypeId::of::<#ty>() }];
        let type_path = match ty {
            syn::Type::Path(type_path) => type_path,
            _ => return result,
        };

        // TODO (#11755): last segment does not necessarily cover all generics.
        // For example, consider `<Apple as Fruit<Round>>::AssocType`. Here
        // `AssocType` in `impl Fruit<Round>` for `Apple` can be a `Vec<Round>`
        // or any other instantiation of a generic type.
        // Not urgent because protocol structs are expected to be simple.
        let generic_params = &type_path.path.segments.last().unwrap().arguments;
        let params = match generic_params {
            syn::PathArguments::AngleBracketed(params) => params,
            _ => return result,
        };

        let inner_type_ids = params
            .args
            .iter()
            .map(|arg| {
                if let syn::GenericArgument::Type(ty) = arg {
                    extract_type_ids_from_type(ty)
                } else {
                    vec![]
                }
            })
            .flatten()
            .collect::<Vec<_>>();
        result.extend(inner_type_ids);
        result
    }

    fn extract_type_info(ty: &syn::Type) -> TokenStream2 {
        let type_path = match ty {
            syn::Type::Path(type_path) => type_path,
            syn::Type::Array(array) => {
                let elem = &array.elem;
                let len = &array.len;
                return quote! {
                    {
                        const fn create_array() -> [std::any::TypeId; 1] {
                            [std::any::TypeId::of::<#elem>()]
                        }
                        (stringify!([#elem; #len]), &create_array())
                    }
                };
            }
            _ => {
                println!("Unsupported type: {:?}", ty);
                return quote! { (stringify!(#ty), &[std::any::TypeId::of::<#ty>()]) };
            }
        };

        let type_name = quote::format_ident!("{}", type_path.path.segments.last().unwrap().ident);
        let type_ids = extract_type_ids_from_type(ty);
        let type_ids_count = type_ids.len();

        quote! {
            {
                const TYPE_IDS_COUNT: usize = #type_ids_count;
                const fn create_array() -> [std::any::TypeId; TYPE_IDS_COUNT] {
                    [#(#type_ids),*]
                }
                (stringify!(#type_name), &create_array())
            }
        }
    }

    fn extract_from_named_fields(
        named: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
    ) -> impl Iterator<Item = TokenStream2> + '_ {
        named.iter().map(|f| {
            let name = &f.ident;
            let ty = &f.ty;
            let type_info = extract_type_info(ty);
            quote! { (stringify!(#name), #type_info) }
        })
    }

    fn extract_from_unnamed_fields(
        unnamed: &syn::punctuated::Punctuated<syn::Field, syn::token::Comma>,
    ) -> impl Iterator<Item = TokenStream2> + '_ {
        unnamed.iter().enumerate().map(|(i, f)| {
            let index = syn::Index::from(i);
            let ty = &f.ty;
            let type_info = extract_type_info(ty);
            quote! { (stringify!(#index), #type_info) }
        })
    }
}

#[cfg(not(all(enable_const_type_id, feature = "protocol_schema")))]
mod helper {
    use proc_macro::TokenStream;

    pub fn protocol_schema_impl(_input: TokenStream) -> TokenStream {
        TokenStream::new()
    }
}
