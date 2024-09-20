use proc_macro::TokenStream;

#[proc_macro_derive(ProtocolSchema)]
pub fn protocol_schema(input: TokenStream) -> TokenStream {
    helper::protocol_schema_impl(input)
}

#[cfg(all(enable_const_type_id, feature = "protocol_schema"))]
mod helper {
    use proc_macro::TokenStream;
    use proc_macro2::TokenStream as TokenStream2;
    use quote::{format_ident, quote};
    use syn::{
        parse_macro_input, Data, DeriveInput, Field, Fields, FieldsNamed, FieldsUnnamed,
        GenericArgument, GenericParam, Generics, Index, Path, PathArguments, PathSegment, Type,
        TypePath, Variant,
    };

    pub fn protocol_schema_impl(input: TokenStream) -> TokenStream {
        let input = parse_macro_input!(input as DeriveInput);
        let name = &input.ident;
        let info_name = format_ident!("{}_INFO", name);
        let generics = &input.generics;

        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        // Create a version of ty_generics without lifetimes for TypeId
        let ty_generics_without_lifetimes = remove_lifetimes(generics);

        let type_id = quote! { std::any::TypeId::of::<#name #ty_generics_without_lifetimes>() };
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

            impl #impl_generics near_schema_checker_lib::ProtocolSchema for #name #ty_generics #where_clause {
                fn ensure_registration() {}
            }
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
    fn extract_type_ids_from_type(ty: &Type) -> Vec<TokenStream2> {
        let mut result = vec![quote! { std::any::TypeId::of::<#ty>() }];
        let type_path = match ty {
            Type::Path(type_path) => type_path,
            _ => return result,
        };

        // TODO (#11755): last segment does not necessarily cover all generics.
        // For example, consider `<Apple as Fruit<Round>>::AssocType`. Here
        // `AssocType` in `impl Fruit<Round>` for `Apple` can be a `Vec<Round>`
        // or any other instantiation of a generic type.
        // Not urgent because protocol structs are expected to be simple.
        let generic_params = &type_path.path.segments.last().unwrap().arguments;
        let params = match generic_params {
            PathArguments::AngleBracketed(params) => params,
            _ => return result,
        };

        let inner_type_ids = params
            .args
            .iter()
            .map(|arg| {
                if let GenericArgument::Type(ty) = arg {
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

    fn extract_type_info(ty: &Type) -> TokenStream2 {
        match ty {
            Type::Path(type_path) => {
                let type_name = &type_path.path.segments.last().unwrap().ident;
                let type_without_lifetimes = remove_lifetimes_from_type(type_path);
                let type_ids = extract_type_ids_from_type(&type_without_lifetimes);
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
            Type::Reference(type_ref) => {
                let elem = &type_ref.elem;
                extract_type_info(elem)
            }
            Type::Array(array) => {
                let elem = &array.elem;
                let len = &array.len;
                quote! {
                    {
                        const fn create_array() -> [std::any::TypeId; 1] {
                            [std::any::TypeId::of::<#elem>()]
                        }
                        (stringify!([#elem; #len]), &create_array())
                    }
                }
            }
            _ => {
                println!("Unsupported type: {:?}", ty);
                quote! { (stringify!(#ty), &[std::any::TypeId::of::<#ty>()]) }
            }
        }
    }

    fn remove_lifetimes_from_type(type_path: &TypePath) -> Type {
        let segments = type_path.path.segments.iter().map(|segment| {
            let mut new_segment =
                PathSegment { ident: segment.ident.clone(), arguments: PathArguments::None };

            if let PathArguments::AngleBracketed(args) = &segment.arguments {
                let new_args: Vec<_> = args
                    .args
                    .iter()
                    .filter_map(|arg| match arg {
                        GenericArgument::Type(ty) => {
                            Some(GenericArgument::Type(remove_lifetimes_from_type_recursive(ty)))
                        }
                        GenericArgument::Const(c) => Some(GenericArgument::Const(c.clone())),
                        _ => None,
                    })
                    .collect();

                if !new_args.is_empty() {
                    new_segment.arguments =
                        PathArguments::AngleBracketed(syn::AngleBracketedGenericArguments {
                            colon2_token: args.colon2_token,
                            lt_token: args.lt_token,
                            args: new_args.into_iter().collect(),
                            gt_token: args.gt_token,
                        });
                }
            }

            new_segment
        });

        Type::Path(TypePath {
            qself: type_path.qself.clone(),
            path: Path {
                leading_colon: type_path.path.leading_colon,
                segments: segments.collect(),
            },
        })
    }

    fn remove_lifetimes_from_type_recursive(ty: &Type) -> Type {
        match ty {
            Type::Path(type_path) => remove_lifetimes_from_type(type_path),
            Type::Reference(type_ref) => Type::Reference(syn::TypeReference {
                and_token: type_ref.and_token,
                lifetime: None,
                mutability: type_ref.mutability,
                elem: Box::new(remove_lifetimes_from_type_recursive(&type_ref.elem)),
            }),
            _ => ty.clone(),
        }
    }

    fn extract_from_named_fields(
        named: &syn::punctuated::Punctuated<Field, syn::token::Comma>,
    ) -> impl Iterator<Item = TokenStream2> + '_ {
        named.iter().map(|f| {
            let name = &f.ident;
            let ty = &f.ty;
            let type_info = extract_type_info(ty);
            quote! { (stringify!(#name), #type_info) }
        })
    }

    fn extract_from_unnamed_fields(
        unnamed: &syn::punctuated::Punctuated<Field, syn::token::Comma>,
    ) -> impl Iterator<Item = TokenStream2> + '_ {
        unnamed.iter().enumerate().map(|(i, f)| {
            let index = Index::from(i);
            let ty = &f.ty;
            let type_info = extract_type_info(ty);
            quote! { (stringify!(#index), #type_info) }
        })
    }

    fn remove_lifetimes(generics: &Generics) -> proc_macro2::TokenStream {
        let params: Vec<_> = generics
            .params
            .iter()
            .filter_map(|param| match param {
                GenericParam::Type(type_param) => Some(quote! { #type_param }),
                GenericParam::Const(const_param) => Some(quote! { #const_param }),
                GenericParam::Lifetime(_) => None,
            })
            .collect();

        if !params.is_empty() {
            quote! { <#(#params),*> }
        } else {
            quote! {}
        }
    }
}

#[cfg(not(all(enable_const_type_id, feature = "protocol_schema")))]
mod helper {
    use proc_macro::TokenStream;

    pub fn protocol_schema_impl(_input: TokenStream) -> TokenStream {
        TokenStream::new()
    }
}
