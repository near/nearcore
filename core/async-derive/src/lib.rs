use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::Meta;

#[proc_macro_derive(MultiSenderFrom)]
pub fn derive_into_multi_sender(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();
    let struct_name = ast.ident.clone();
    let input = match ast.data {
        syn::Data::Struct(input) => input,
        _ => {
            panic!("MultiSenderFrom can only be derived for structs");
        }
    };

    let mut type_bounds = Vec::new();
    let mut initializers = Vec::new();
    let mut cfg_attrs = Vec::new();
    let mut names = Vec::<syn::Ident>::new();
    for (i, field) in input.fields.into_iter().enumerate() {
        let field_name = field
            .ident
            .as_ref()
            .map(|ident| ident.to_string())
            .unwrap_or_else(|| format!("#{}", i));
        cfg_attrs.push(extract_cfg_attributes(&field.attrs));
        match &field.ty {
            syn::Type::Path(path) => {
                let last_segment = path.path.segments.last().unwrap();
                let arguments = match last_segment.arguments.clone() {
                    syn::PathArguments::AngleBracketed(arguments) => {
                        arguments.args.into_iter().collect::<Vec<_>>()
                    }
                    _ => panic!("Field {} must be either a Sender or an AsyncSender", field_name),
                };
                if last_segment.ident == "Sender" {
                    type_bounds.push(quote!(near_async::messaging::CanSend<#(#arguments),*>));
                } else if last_segment.ident == "AsyncSender" {
                    type_bounds.push(quote!(
                            near_async::messaging::CanSend<near_async::messaging::MessageExpectingResponse<#(#arguments),*>>));
                } else if last_segment.ident == "ActixAsyncSender" {
                    let message_type = arguments[0].clone();
                    let result_type = quote!(std::result::Result<near_async::actix::ActixResult<#message_type>, near_async::actix::AsyncSendError>);
                    type_bounds.push(quote!(
                            near_async::messaging::CanSend<near_async::messaging::MessageExpectingResponse<#message_type, #result_type>>));
                } else {
                    panic!(
                        "Field {} must be either a Sender, AsyncSender, or ActixAsyncSender",
                        field_name
                    );
                }
                initializers.push(quote!(near_async::messaging::IntoSender::as_sender(&input)));
                if let Some(name) = &field.ident {
                    names.push(name.clone());
                }
            }
            _ => panic!("Field {} must be either a Sender or an AsyncSender", field_name),
        }
    }

    assert!(!type_bounds.is_empty(), "Must have at least one field");

    let initializer = if names.is_empty() {
        quote!(#struct_name(#(#(#cfg_attrs)* #initializers,)*))
    } else {
        quote!(#struct_name {
            #(#(#cfg_attrs)* #names: #initializers,)*
        })
    };

    quote! {
        impl<A: #(#type_bounds)+*> near_async::messaging::MultiSenderFrom<A> for #struct_name {
            fn multi_sender_from(input: std::sync::Arc<A>) -> Self {
                #initializer
            }
        }
    }
    .into()
}

#[proc_macro_derive(MultiSend)]
pub fn derive_multi_send(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();
    let struct_name = ast.ident.clone();
    let input = match ast.data {
        syn::Data::Struct(input) => input,
        _ => {
            panic!("MultiSend can only be derived for structs");
        }
    };

    let mut impls = Vec::new();
    for (i, field) in input.fields.into_iter().enumerate() {
        let field_name = field.ident.as_ref().map(|ident| quote!(#ident)).unwrap_or_else(|| {
            let index = syn::Index::from(i);
            quote!(#index)
        });
        let cfg_attrs = extract_cfg_attributes(&field.attrs);
        if let syn::Type::Path(path) = &field.ty {
            let last_segment = path.path.segments.last().unwrap();
            let arguments = match last_segment.arguments.clone() {
                syn::PathArguments::AngleBracketed(arguments) => {
                    arguments.args.into_iter().collect::<Vec<_>>()
                }
                _ => {
                    continue;
                }
            };
            if last_segment.ident == "Sender" {
                let message_type = arguments[0].clone();
                impls.push(quote! {
                    #(#cfg_attrs)*
                    impl near_async::messaging::CanSend<#message_type> for #struct_name {
                        fn send(&self, message: #message_type) {
                            self.#field_name.send(message);
                        }
                    }
                });
            } else if last_segment.ident == "AsyncSender" {
                let message_type = arguments[0].clone();
                let result_type = arguments[1].clone();
                let outer_msg_type = quote!(near_async::messaging::MessageExpectingResponse<#message_type, #result_type>);
                impls.push(quote! {
                    #(#cfg_attrs)*
                    impl near_async::messaging::CanSend<#outer_msg_type> for #struct_name {
                        fn send(&self, message: #outer_msg_type) {
                            self.#field_name.send(message)
                        }
                    }
                });
            } else if last_segment.ident == "ActixAsyncSender" {
                let message_type = arguments[0].clone();
                let result_type = quote!(std::result::Result<near_async::actix::ActixResult<#message_type>, near_async::actix::AsyncSendError>);
                let outer_msg_type = quote!(near_async::messaging::MessageExpectingResponse<#message_type, #result_type>);
                impls.push(quote! {
                    #(#cfg_attrs)*
                    impl near_async::messaging::CanSend<#outer_msg_type> for #struct_name {
                        fn send(&self, message: #outer_msg_type) {
                            self.#field_name.send(message)
                        }
                    }
                });
            }
        }
    }

    quote! {#(#impls)*}.into()
}

fn extract_cfg_attributes(attrs: &[syn::Attribute]) -> Vec<syn::Attribute> {
    attrs.iter().filter(|attr| attr.path().is_ident("cfg")).cloned().collect()
}

#[proc_macro_derive(
    MultiSendMessage,
    attributes(multi_send_message_derive, multi_send_input_derive)
)]
pub fn derive_multi_send_message(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();
    let struct_name = ast.ident.clone();
    let message_enum_name = syn::Ident::new(&format!("{}Message", struct_name), Span::call_site());
    let input_enum_name = syn::Ident::new(&format!("{}Input", struct_name), Span::call_site());
    let input = match ast.data {
        syn::Data::Struct(input) => input,
        _ => {
            panic!("MultiSendMessage can only be derived for structs");
        }
    };

    let mut field_names = Vec::new();
    let mut message_types = Vec::new();
    let mut input_types = Vec::new();
    let mut discriminator_names = Vec::new();
    let mut input_extractors = Vec::new();
    for (i, field) in input.fields.into_iter().enumerate() {
        let field_name = field.ident.as_ref().map(|ident| quote!(#ident)).unwrap_or_else(|| {
            let index = syn::Index::from(i);
            quote!(#index)
        });
        field_names.push(field_name.clone());
        discriminator_names.push(syn::Ident::new(&format!("_{}", field_name), Span::call_site()));
        if let syn::Type::Path(path) = &field.ty {
            let last_segment = path.path.segments.last().unwrap();
            let arguments = match last_segment.arguments.clone() {
                syn::PathArguments::AngleBracketed(arguments) => {
                    arguments.args.into_iter().collect::<Vec<_>>()
                }
                _ => {
                    continue;
                }
            };
            if last_segment.ident == "Sender" {
                let message_type = arguments[0].clone();
                message_types.push(quote!(#message_type));
                input_types.push(quote!(#message_type));
                input_extractors.push(quote!(msg));
            } else if last_segment.ident == "AsyncSender" {
                let message_type = arguments[0].clone();
                let result_type = arguments[1].clone();
                message_types.push(quote!(near_async::messaging::MessageExpectingResponse<#message_type, #result_type>));
                input_types.push(quote!(#message_type));
                input_extractors.push(quote!(msg.message));
            } else if last_segment.ident == "ActixAsyncSender" {
                let message_type = arguments[0].clone();
                let result_type = quote!(std::result::Result<near_async::actix::ActixResult<#message_type>, near_async::actix::AsyncSendError>);
                message_types.push(quote!(near_async::messaging::MessageExpectingResponse<#message_type, #result_type>));
                input_types.push(quote!(#message_type));
                input_extractors.push(quote!(msg.message));
            }
        }
    }

    let mut message_derives = proc_macro2::TokenStream::new();
    let mut input_derives = proc_macro2::TokenStream::new();
    for attr in ast.attrs {
        if attr.path().is_ident("multi_send_message_derive") {
            let Meta::List(metalist) = attr.meta else {
                panic!("multi_send_message_derive must be a list");
            };
            message_derives = metalist.tokens;
        } else if attr.path().is_ident("multi_send_input_derive") {
            let Meta::List(metalist) = attr.meta else {
                panic!("multi_send_input_derive must be a list");
            };
            input_derives = metalist.tokens;
        }
    }

    quote! {
        #[derive(#message_derives)]
        pub enum #message_enum_name {
            #(#discriminator_names(#message_types),)*
        }

        #[derive(#input_derives)]
        pub enum #input_enum_name {
            #(#discriminator_names(#input_types),)*
        }

        impl near_async::messaging::CanSend<#message_enum_name> for #struct_name {
            fn send(&self, message: #message_enum_name) {
                match message {
                    #(#message_enum_name::#discriminator_names(message) => self.#field_names.send(message),)*
                }
            }
        }

        #(impl From<#message_types> for #message_enum_name {
            fn from(message: #message_types) -> Self {
                #message_enum_name::#discriminator_names(message)
            }
        })*

        impl #message_enum_name {
            pub fn into_input(self) -> #input_enum_name {
                match self {
                    #(Self::#discriminator_names(msg) => #input_enum_name::#discriminator_names(#input_extractors),)*
                }
            }
        }
    }
    .into()
}
