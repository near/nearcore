//! This crate provides a set of procedural macros for deriving traits for
//! multi-sender types, which are structs that contain multiple `Sender` and
//! `AsyncSender` fields. The structs can either have named fields or be a
//! tuple struct.
//!
//! The derive macros provided by this crate allows multi-senders to be
//! created from anything that behaves like all of the individual senders,
//! and allows the multi-sender to be used like any of the individual senders.
//! This can be very useful when one component needs to send multiple kinds of
//! messages to another component; for example the networking layer needs to
//! send multiple types of messages to the ClientActor, each expecting a
//! different response; it would be very cumbersome to have to construct the
//! PeerManagerActor by passing in 10 different sender objects, so instead we
//! create a multi-sender interface of all the senders and pass that in instead.
//!
//! To better understand these macros,
//!  - Look at the tests in this crate for examples of what the macros generate.
//!  - Search for usages of the derive macros in the codebase.
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::Meta;

/// Derives the ability to convert an object into this struct of Sender and
/// AsyncSenders, as long as the object can be converted into each individual
/// Sender or AsyncSender.
/// The conversion is done by calling `.as_multi_sender()` or `.into_multi_sender()`.
#[proc_macro_derive(MultiSenderFrom)]
pub fn derive_multi_sender_from(input: TokenStream) -> TokenStream {
    derive_multi_sender_from_impl(input.into()).into()
}

fn derive_multi_sender_from_impl(input: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let ast: syn::DeriveInput = syn::parse2(input).unwrap();
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
                            near_async::messaging::CanSend<near_async::messaging::MessageWithCallback<#(#arguments),*>>));
                } else {
                    panic!("Field {} must be either a Sender or an AsyncSender", field_name);
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
}

/// Derives the ability to use this struct of `Sender`s and `AsyncSender`s to
/// call `.send` or `.send_async` directly as if using one of the included
/// `Sender`s or `AsyncSender`s.
#[proc_macro_derive(MultiSend)]
pub fn derive_multi_send(input: TokenStream) -> TokenStream {
    derive_multi_send_impl(input.into()).into()
}

fn derive_multi_send_impl(input: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let ast: syn::DeriveInput = syn::parse2(input).unwrap();
    let struct_name = ast.ident.clone();
    let input = match ast.data {
        syn::Data::Struct(input) => input,
        _ => {
            panic!("MultiSend can only be derived for structs");
        }
    };

    let mut tokens = Vec::new();
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
                tokens.push(quote! {
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
                let outer_msg_type =
                    quote!(near_async::messaging::MessageWithCallback<#message_type, #result_type>);
                tokens.push(quote! {
                    #(#cfg_attrs)*
                    impl near_async::messaging::CanSend<#outer_msg_type> for #struct_name {
                        fn send(&self, message: #outer_msg_type) {
                            self.#field_name.send(message);
                        }
                    }
                });
            }
        }
    }

    quote! {#(#tokens)*}
}

fn extract_cfg_attributes(attrs: &[syn::Attribute]) -> Vec<syn::Attribute> {
    attrs.iter().filter(|attr| attr.path().is_ident("cfg")).cloned().collect()
}

/// Derives two enums, whose names are based on this struct by appending
/// `Message` and `Input`. Each enum has a case for each `Sender` or
/// `AsyncSender` in this struct. The `Message` enum contains the raw message
/// being sent, which is `X` for `Sender<X>` and `MessageWithCallback<X, Y>`
/// for `AsyncSender<X, Y>`. The `Input` enum contains the same for `Sender` but
/// only the input, `X` for `AsyncSender<X, Y>`.
///
/// Additionally, this struct can then be used to `.send` using the derived
/// `Message` enum. This is useful for packaging a multi-sender as a singular
/// `Sender` that can then be embedded into another multi-sender. The `Input`
/// enum is useful for capturing messages for testing purposes.
#[proc_macro_derive(
    MultiSendMessage,
    attributes(multi_send_message_derive, multi_send_input_derive)
)]
pub fn derive_multi_send_message(input: TokenStream) -> TokenStream {
    derive_multi_send_message_impl(input.into()).into()
}

fn derive_multi_send_message_impl(input: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    let ast: syn::DeriveInput = syn::parse2(input).unwrap();
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
                message_types.push(
                    quote!(near_async::messaging::MessageWithCallback<#message_type, #result_type>),
                );
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
}

#[cfg(test)]
mod tests {
    use quote::quote;

    #[test]
    fn test_derive_into_multi_send() {
        let input = quote! {
            struct TestSenders {
                sender: Sender<String>,
                async_sender: AsyncSender<String, u32>,
                qualified_sender: near_async::messaging::Sender<i32>,
                qualified_async_sender: near_async::messaging::AsyncSender<i32, String>,
            }
        };
        let expected = quote! {
            impl<A:
                near_async::messaging::CanSend<String>
                + near_async::messaging::CanSend<near_async::messaging::MessageWithCallback<String, u32>>
                + near_async::messaging::CanSend<i32>
                + near_async::messaging::CanSend<near_async::messaging::MessageWithCallback<i32, String>>
                > near_async::messaging::MultiSenderFrom<A> for TestSenders {
                fn multi_sender_from(input: std::sync::Arc<A>) -> Self {
                    TestSenders {
                        sender: near_async::messaging::IntoSender::as_sender(&input),
                        async_sender: near_async::messaging::IntoSender::as_sender(&input),
                        qualified_sender: near_async::messaging::IntoSender::as_sender(&input),
                        qualified_async_sender: near_async::messaging::IntoSender::as_sender(&input),
                    }
                }
            }
        };
        let actual = super::derive_multi_sender_from_impl(input);
        pretty_assertions::assert_str_eq!(actual.to_string(), expected.to_string());
    }

    #[test]
    fn test_derive_multi_send() {
        let input = quote! {
            struct TestSenders {
                sender: Sender<String>,
                async_sender: AsyncSender<String, u32>,
                qualified_sender: near_async::messaging::Sender<i32>,
                qualified_async_sender: near_async::messaging::AsyncSender<i32, String>,
            }
        };
        let expected = quote! {
            impl near_async::messaging::CanSend<String> for TestSenders {
                fn send(&self, message: String) {
                    self.sender.send(message);
                }
            }
            impl near_async::messaging::CanSend<near_async::messaging::MessageWithCallback<String, u32> > for TestSenders {
                fn send(&self, message: near_async::messaging::MessageWithCallback<String, u32>) {
                    self.async_sender.send(message);
                }
            }
            impl near_async::messaging::CanSend<i32> for TestSenders {
                fn send(&self, message: i32) {
                    self.qualified_sender.send(message);
                }
            }
            impl near_async::messaging::CanSend<near_async::messaging::MessageWithCallback<i32, String> > for TestSenders {
                fn send(&self, message: near_async::messaging::MessageWithCallback<i32, String>) {
                    self.qualified_async_sender.send(message);
                }
            }
        };
        let actual = super::derive_multi_send_impl(input);
        pretty_assertions::assert_str_eq!(actual.to_string(), expected.to_string());
    }

    #[test]
    fn test_derive_multi_send_message() {
        let input = quote! {
            #[multi_send_message_derive(X, Y)]
            #[multi_send_input_derive(Z, W)]
            struct TestSenders {
                sender: Sender<A>,
                async_sender: AsyncSender<B, C>,
                qualified_sender: near_async::messaging::Sender<D>,
                qualified_async_sender: near_async::messaging::AsyncSender<E, F>,
            }
        };

        let expected = quote! {
            #[derive(X, Y)]
            pub enum TestSendersMessage {
                _sender(A),
                _async_sender(near_async::messaging::MessageWithCallback<B, C>),
                _qualified_sender(D),
                _qualified_async_sender(near_async::messaging::MessageWithCallback<E, F>),
            }

            #[derive(Z, W)]
            pub enum TestSendersInput {
                _sender(A),
                _async_sender(B),
                _qualified_sender(D),
                _qualified_async_sender(E),
            }

            impl near_async::messaging::CanSend<TestSendersMessage> for TestSenders {
                fn send(&self, message: TestSendersMessage) {
                    match message {
                        TestSendersMessage::_sender(message) => self.sender.send(message),
                        TestSendersMessage::_async_sender(message) => self.async_sender.send(message),
                        TestSendersMessage::_qualified_sender(message) => self.qualified_sender.send(message),
                        TestSendersMessage::_qualified_async_sender(message) => self.qualified_async_sender.send(message),
                    }
                }
            }

            impl From<A> for TestSendersMessage {
                fn from(message: A) -> Self {
                    TestSendersMessage::_sender(message)
                }
            }

            impl From<near_async::messaging::MessageWithCallback<B, C> > for TestSendersMessage {
                fn from(message: near_async::messaging::MessageWithCallback<B, C>) -> Self {
                    TestSendersMessage::_async_sender(message)
                }
            }

            impl From<D> for TestSendersMessage {
                fn from(message: D) -> Self {
                    TestSendersMessage::_qualified_sender(message)
                }
            }

            impl From<near_async::messaging::MessageWithCallback<E, F> > for TestSendersMessage {
                fn from(message: near_async::messaging::MessageWithCallback<E, F>) -> Self {
                    TestSendersMessage::_qualified_async_sender(message)
                }
            }

            impl TestSendersMessage {
                pub fn into_input(self) -> TestSendersInput {
                    match self {
                        Self::_sender(msg) => TestSendersInput::_sender(msg),
                        Self::_async_sender(msg) => TestSendersInput::_async_sender(msg.message),
                        Self::_qualified_sender(msg) => TestSendersInput::_qualified_sender(msg),
                        Self::_qualified_async_sender(msg) => TestSendersInput::_qualified_async_sender(msg.message),
                    }
                }
            }
        };
        let actual = super::derive_multi_send_message_impl(input);
        pretty_assertions::assert_str_eq!(actual.to_string(), expected.to_string());
    }
}
