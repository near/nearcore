use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(ProtocolStruct)]
pub fn protocol_struct(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // let fields = match &input.data {
    //     Data::Struct(data) => &data.fields,
    //     Data::Enum(_) => {
    //         return TokenStream::from(quote! {
    //             impl near_structs_checker_core::ProtocolStruct for #name {}
    //         })
    //     }
    //     Data::Union(_) => panic!("ProtocolStruct cannot be derived for unions"),
    // };
    //
    // let (field_infos, field_hashes) = match fields {
    //     Fields::Named(fields) => {
    //         fields.named.iter().map(|field| {
    //             let field_name = field.ident.as_ref().unwrap().to_string();
    //             let field_type = &field.ty;
    //             let field_info = quote! {
    //                 near_structs_checker_core::FieldInfo {
    //                     name: #field_name.to_string(),
    //                     type_name: <#field_type as near_structs_checker_core::ProtocolStruct>::type_name(),
    //                     hash: <#field_type as near_structs_checker_core::ProtocolStruct>::calculate_hash(),
    //                 }
    //             };
    //             let field_hash = quote! {
    //                 <#field_type as near_structs_checker_core::ProtocolStruct>::calculate_hash().hash(&mut hasher);
    //             };
    //             (field_info, field_hash)
    //         }).unzip()
    //     },
    //     Fields::Unnamed(_) => (vec![], vec![]),
    //     Fields::Unit => (vec![], vec![]),
    // };
    //
    // let expanded = quote! {
    //     impl near_structs_checker_core::ProtocolStruct for #name {
    //         fn calculate_hash() -> u64 {
    //             let mut hasher = std::collections::hash_map::DefaultHasher::new();
    //             Self::type_name().hash(&mut hasher);
    //             #(#field_hashes)*
    //             hasher.finish()
    //         }
    //
    //         fn get_info() -> near_structs_checker_core::ProtocolStructInfo {
    //             near_structs_checker_core::ProtocolStructInfo {
    //                 name: Self::type_name(),
    //                 hash: Self::calculate_hash(),
    //                 fields: vec![
    //                     #(#field_infos),*
    //                 ],
    //             }
    //         }
    //     }
    //
    //     near_structs_checker_core::register_protocol_struct(<#name as near_structs_checker_core::ProtocolStruct>::get_info());
    // };

    let expanded = quote! {
        impl near_structs_checker_core::ProtocolStruct for #name {}
    };

    TokenStream::from(expanded)
}
