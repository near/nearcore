#![recursion_limit = "128"]
extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::export::TokenStream2;
use syn::{Attribute, Fields, Ident, ItemEnum, ItemStruct, ItemUnion, Meta, NestedMeta};

#[proc_macro_derive(nbor, attributes(nbor_skip, nbor_init))]
pub fn nbor(input: TokenStream) -> TokenStream {
    if let Ok(input) = syn::parse::<ItemStruct>(input.clone()) {
        let mut res = nbor_struct_ser(&input);
        res.extend(nbor_struct_de(&input));
        TokenStream::from(res)
    } else if let Ok(input) = syn::parse::<ItemEnum>(input.clone()) {
        let mut res = nbor_enum_ser(&input);
        res.extend(nbor_enum_de(&input));
        TokenStream::from(res)
    } else if let Ok(input) = syn::parse::<ItemUnion>(input.clone()) {
        let mut res = nbor_union_ser(&input);
        res.extend(nbor_union_de(&input));
        TokenStream::from(res)
    } else {
        // Derive macros can only be defined on structs, enums, and unions.
        unreachable!()
    }
}

// Check if the field is marked with `#[nbor(skip)]` or `#[.., nbor(.., skip, ...), ...]`.
fn contains_skip(attrs: &[Attribute]) -> bool {
    for attr in attrs.iter() {
        if let Ok(Meta::Word(ident)) = attr.parse_meta() {
            if ident.to_string().as_str() == "nbor_skip" {
                return true;
            }
        }
    }
    false
}

// Checks if struct should be initialized with some method after the object is constructed.
// Should be marked as #[nbor_init(<name_of_the_method_to_use>)]
fn contains_initialize_with(attrs: &[Attribute]) -> Option<Ident> {
    for attr in attrs.iter() {
        if let Ok(Meta::List(meta_list)) = attr.parse_meta() {
            if meta_list.ident.to_string().as_str() == "nbor_init" {
                assert_eq!(
                    meta_list.nested.len(),
                    1,
                    "nbor_init requires exactly one initialization method."
                );
                let nested_meta = meta_list.nested.iter().next().unwrap();
                if let NestedMeta::Meta(Meta::Word(ident)) = nested_meta {
                    return Some(ident.clone());
                }
            }
        }
    }
    None
}

fn nbor_struct_ser(input: &ItemStruct) -> TokenStream2 {
    let name = &input.ident;
    let mut body = TokenStream2::new();
    match &input.fields {
        Fields::Named(fields) => {
            for field in &fields.named {
                if contains_skip(&field.attrs) {
                    continue;
                }
                let field_name = field.ident.as_ref().unwrap();
                let delta = quote! {
                    nbor::Serializable::write(&self.#field_name, writer)?;
                };
                body.extend(delta);
            }
        }
        Fields::Unnamed(fields) => {
            for field_idx in 0..fields.unnamed.len() {
                let delta = quote! {
                    nbor::Serializable::write(&self.#field_idx, writer)?;
                };
                body.extend(delta);
            }
        }
        Fields::Unit => {}
    }
    quote! {
        impl nbor::ser::Serializable for #name {
            fn write<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
                #body
                Ok(())
            }
        }
    }
}

fn nbor_enum_ser(input: &ItemEnum) -> TokenStream2 {
    let name = &input.ident;
    let mut body = TokenStream2::new();
    for (variant_idx, variant) in input.variants.iter().enumerate() {
        let variant_idx = variant_idx as u32;
        let variant_ident = &variant.ident;
        let mut variant_header = TokenStream2::new();
        let mut variant_body = TokenStream2::new();
        match &variant.fields {
            Fields::Named(fields) => {
                for field in &fields.named {
                    let field_name = field.ident.as_ref().unwrap();
                    if contains_skip(&field.attrs) {
                        variant_header.extend(quote! { _#field_name, });
                        continue;
                    } else {
                        variant_header.extend(quote! { #field_name, });
                    }
                    variant_body.extend(quote! {
                         nbor::Serializable::write(#field_name, writer)?;
                    })
                }
                variant_header = quote! { { #variant_header }};
            }
            Fields::Unnamed(fields) => {
                for (field_idx, field) in fields.unnamed.iter().enumerate() {
                    let field_idx = field_idx as u32;
                    if contains_skip(&field.attrs) {
                        let field_ident =
                            Ident::new(format!("_id{}", field_idx).as_str(), Span::call_site());
                        variant_header.extend(quote! { #field_ident, });
                        continue;
                    } else {
                        let field_ident =
                            Ident::new(format!("id{}", field_idx).as_str(), Span::call_site());
                        variant_header.extend(quote! { #field_ident, });
                    }
                    variant_body.extend(quote! {
                         nbor::Serializable::write(&#field_idx, writer)?;
                    })
                }
                variant_header = quote! { ( #variant_header )};
            }
            Fields::Unit => {}
        }
        body.extend(quote!(
            #name::#variant_ident #variant_header => {
                let variant_idx: u32 = #variant_idx;
                writer.write(&variant_idx.to_le_bytes())?;
                #variant_body
            }
        ))
    }
    let res = quote! {
        impl nbor::ser::Serializable for #name {
            fn write<W: std::io::Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
                match self {
                    #body
                }
                Ok(())
            }
        }
    };
    println!("{}", res.to_string());
    res
}

fn nbor_union_ser(_input: &ItemUnion) -> TokenStream2 {
    unimplemented!()
}

fn nbor_struct_de(input: &ItemStruct) -> TokenStream2 {
    let name = &input.ident;
    let init_method = contains_initialize_with(&input.attrs);
    let return_value = match &input.fields {
        Fields::Named(fields) => {
            let mut body = TokenStream2::new();
            for field in &fields.named {
                let field_name = field.ident.as_ref().unwrap();
                let delta = if contains_skip(&field.attrs) {
                    quote! {
                        #field_name: Default::default(),
                    }
                } else {
                    quote! {
                        #field_name: nbor::Deserializable::read(reader)?,
                    }
                };
                body.extend(delta);
            }
            quote! {
                Self { #body }
            }
        }
        Fields::Unnamed(fields) => {
            let mut body = TokenStream2::new();
            for _ in 0..fields.unnamed.len() {
                let delta = quote! {
                    nbor::Deserializable::read(reader)?,
                };
                body.extend(delta);
            }
            quote! {
                Self( #body )
            }
        }
        Fields::Unit => {
            quote! {
                Self {}
            }
        }
    };
    if let Some(method_ident) = init_method {
        quote! {
            impl nbor::de::Deserializable for #name {
                fn read<R: std::io::Read>(reader: &mut R) -> Result<Self, std::io::Error> {
                    let mut return_value = #return_value;
                    return_value.#method_ident();
                    Ok(return_value)
                }
            }
        }
    } else {
        quote! {
            impl nbor::de::Deserializable for #name {
                fn read<R: std::io::Read>(reader: &mut R) -> Result<Self, std::io::Error> {
                    Ok(#return_value)
                }
            }
        }
    }
}
fn nbor_enum_de(input: &ItemEnum) -> TokenStream2 {
    let name = &input.ident;
    let init_method = contains_initialize_with(&input.attrs);
    let mut variant_arms = TokenStream2::new();
    for (variant_idx, variant) in input.variants.iter().enumerate() {
        let variant_idx = variant_idx as u32;
        let variant_ident = &variant.ident;
        let mut variant_header = TokenStream2::new();
        match &variant.fields {
            Fields::Named(fields) => {
                for field in &fields.named {
                    let field_name = field.ident.as_ref().unwrap();
                    if contains_skip(&field.attrs) {
                        variant_header.extend(quote! {
                            #field_name: Default::default(),
                        });
                    } else {
                        variant_header.extend(quote! {
                            #field_name: nbor::Deserializable::read(reader)?,
                        });
                    }
                }
                variant_header = quote! { { #variant_header }};
            }
            Fields::Unnamed(fields) => {
                for field in fields.unnamed.iter() {
                    if contains_skip(&field.attrs) {
                        variant_header.extend(quote! { Default::default(), });
                    } else {
                        variant_header.extend(quote! { nbor::Deserializable::read(reader)?, });
                    }
                }
                variant_header = quote! { ( #variant_header )};
            }
            Fields::Unit => {}
        }
        variant_arms.extend(quote! {
            #variant_idx => #name::#variant_ident #variant_header ,
        });
    }
    let variant_idx = quote! {
        let mut variant_idx = [0u8; std::mem::size_of::<u32>()];
        reader.read(&mut variant_idx)?;
        let variant_idx = u32::from_le_bytes(variant_idx);
    };
    if let Some(method_ident) = init_method {
        quote! {
            impl nbor::de::Deserializable for #name {
                fn read<R: std::io::Read>(reader: &mut R) -> Result<Self, std::io::Error> {
                    #variant_idx
                    let mut return_value = match variant_idx {
                        #variant_arms
                        _ => panic!(format!("Unexpeted variant index: {:?}", variant_idx)),
                    };
                    return_value.#method_ident();
                    Ok(return_value)
                }
            }
        }
    } else {
        quote! {
            impl nbor::de::Deserializable for #name {
                fn read<R: std::io::Read>(reader: &mut R) -> Result<Self, std::io::Error> {
                    #variant_idx
                    let return_value = match variant_idx {
                        #variant_arms
                        _ => panic!(format!("Unexpeted variant index: {:?}", variant_idx)),
                    };
                    Ok(return_value)
                }
            }
        }
    }
}

fn nbor_union_de(_input: &ItemUnion) -> TokenStream2 {
    unimplemented!()
}
