use std::collections::BTreeMap;
use syn::{Data, DataEnum, DataStruct, DeriveInput, Fields, FieldsNamed, FieldsUnnamed};

#[derive(Default, Debug, serde::Deserialize, serde::Serialize)]
pub struct ErrorType {
    /// A type name of the error
    pub name: String,
    /// Names of subtypes of the error
    pub subtypes: Vec<String>,
    /// An error input name and a type
    pub props: BTreeMap<String, String>,
}

fn parse_rpc_error_variant(input: &DeriveInput) -> String {
    let type_name = input.ident.to_string();
    let type_kind: Vec<&str> = type_name.split("Kind").collect();
    type_kind[0].to_string()
}

fn error_type_name(schema: &mut BTreeMap<String, ErrorType>, name: String) -> &mut ErrorType {
    let error_type = ErrorType { name: name.clone(), ..Default::default() };
    schema.entry(name).or_insert(error_type)
}

pub fn parse_error_type(schema: &mut BTreeMap<String, ErrorType>, input: &DeriveInput) {
    let name = parse_rpc_error_variant(input);
    match &input.data {
        Data::Enum(DataEnum { ref variants, .. }) => {
            // TODO: check for uniqueness
            let error_type = error_type_name(schema, name);
            let mut direct_error_types = vec![];
            for variant in variants {
                error_type.subtypes.push(variant.ident.to_string());
                match &variant.fields {
                    Fields::Unnamed(FieldsUnnamed { ref unnamed, .. }) => {
                        // Subtype
                        if unnamed.iter().count() > 1 {
                            panic!(
                                "Error types doesn't support tuple variants with multiple fields"
                            );
                        }
                    }
                    Fields::Named(FieldsNamed { ref named, .. }) => {
                        // If variant is Enum with a named fields - create a new type for each variant with named props
                        let mut error_type =
                            ErrorType { name: variant.ident.to_string(), ..Default::default() };
                        for field in named {
                            error_type.props.insert(
                                field
                                    .ident
                                    .as_ref()
                                    .expect("named fields must have ident")
                                    .to_string(),
                                "".to_owned(),
                            );
                        }
                        direct_error_types.push(error_type);
                    }
                    Fields::Unit => {
                        direct_error_types.push(ErrorType {
                            name: variant.ident.to_string(),
                            ..Default::default()
                        });
                    }
                }
            }
            for e in direct_error_types {
                let error_type = error_type_name(schema, e.name.clone());
                error_type.name = e.name;
                error_type.props = e.props;
            }
        }
        Data::Struct(DataStruct { ref fields, .. }) => {
            let error_type = error_type_name(schema, name);
            match fields {
                Fields::Named(FieldsNamed { ref named, .. }) => {
                    for field in named {
                        let field_name =
                            field.ident.as_ref().expect("named fields must have ident").to_string();
                        if field_name == "kind" {
                            continue;
                        }
                        error_type.props.insert(field_name, "".to_owned()); // TODO: add prop type
                    }
                }
                _ => {
                    panic!("RpcError supports structs with the named fields only");
                }
            }
        }
        Data::Union(_) => {
            panic!("Unions are not supported");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;
    #[test]
    fn should_merge_kind() {
        let mut schema = BTreeMap::default();
        let error_type = syn::parse2(quote! {
            pub struct ActionError {
                pub index: Option<u64>,
                pub kind: ActionErrorKind,
            }
        })
        .unwrap();
        parse_error_type(&mut schema, &error_type);
        let expected: BTreeMap<String, ErrorType> = serde_json::from_str(
            r#"
        {
            "ActionError": {
                "name": "ActionError",
                "subtypes": [],
                "props": {
                "index": ""
                }
            }
          }
        "#,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&expected).unwrap(),
            serde_json::to_string(&schema).unwrap()
        );
        let error_type_kind: DeriveInput = syn::parse2(quote! {
            pub enum ActionErrorKind {
                AccountAlreadyExists { account_id: String },
            }
        })
        .unwrap();
        let expected: BTreeMap<String, ErrorType> = serde_json::from_str(
            r#"
        {
            "ActionError": {
                "name": "ActionError",
                "subtypes": ["AccountAlreadyExists"],
                "props": {
                    "index": ""
                }
            },
            "AccountAlreadyExists": {
                "name": "AccountAlreadyExists",
                "subtypes": [],
                "props": {
                    "account_id": ""
                }
            }
          }
        "#,
        )
        .unwrap();
        parse_error_type(&mut schema, &error_type_kind);
        assert_eq!(
            serde_json::to_string(&expected).unwrap(),
            serde_json::to_string(&schema).unwrap()
        );
    }

    #[test]
    fn complex() {
        let mut schema = BTreeMap::default();
        parse_error_type(
            &mut schema,
            &syn::parse2(quote! {
                pub enum TxExecutionError {
                    ActionError(ActionError),
                    InvalidTxError(InvalidTxError),
                }
            })
            .unwrap(),
        );
        parse_error_type(
            &mut schema,
            &syn::parse2(quote! {
                pub enum InvalidTxError {
                    InvalidAccessKeyError(InvalidAccessKeyError),
                    InvalidSignerId { signer_id: AccountId },
                }
            })
            .unwrap(),
        );
        parse_error_type(
            &mut schema,
            &syn::parse2(quote! {
                pub enum InvalidAccessKeyError {
                    /// The access key identified by the `public_key` doesn't exist for the account
                    AccessKeyNotFound { account_id: AccountId, public_key: PublicKey },
                }
            })
            .unwrap(),
        );
        parse_error_type(
            &mut schema,
            &syn::parse2(quote! {
                pub struct ActionError {
                    pub index: Option<u64>,
                    pub kind: ActionErrorKind,
                }
            })
            .unwrap(),
        );
        parse_error_type(
            &mut schema,
            &syn::parse2(quote! {
                pub enum ActionErrorKind {
                    AccountAlreadyExists { account_id: String },
                }
            })
            .unwrap(),
        );
        let expected: BTreeMap<String, ErrorType> = serde_json::from_str(
            r#"
            {
                "AccessKeyNotFound": {
                  "name": "AccessKeyNotFound",
                  "subtypes": [],
                  "props": {
                    "account_id": "",
                    "public_key": ""
                  }
                },
                "AccountAlreadyExists": {
                  "name": "AccountAlreadyExists",
                  "subtypes": [],
                  "props": {
                    "account_id": ""
                  }
                },
                "ActionError": {
                  "name": "ActionError",
                  "subtypes": [
                    "AccountAlreadyExists"
                  ],
                  "props": {
                    "index": ""
                  }
                },
                "InvalidAccessKeyError": {
                  "name": "InvalidAccessKeyError",
                  "subtypes": [
                    "AccessKeyNotFound"
                  ],
                  "props": {}
                },
                "InvalidSignerId": {
                  "name": "InvalidSignerId",
                  "subtypes": [],
                  "props": {
                    "signer_id": ""
                  }
                },
                "InvalidTxError": {
                  "name": "InvalidTxError",
                  "subtypes": [
                    "InvalidAccessKeyError",
                    "InvalidSignerId"
                  ],
                  "props": {}
                },
                "TxExecutionError": {
                  "name": "TxExecutionError",
                  "subtypes": [
                    "ActionError",
                    "InvalidTxError"
                  ],
                  "props": {}
                }
              }"#,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&expected).unwrap(),
            serde_json::to_string(&schema).unwrap()
        );
    }
    #[test]
    #[should_panic]
    fn should_not_accept_tuples() {
        let mut schema = BTreeMap::default();
        parse_error_type(
            &mut schema,
            &syn::parse2(quote! {
                pub enum ErrorWithATupleVariant {
                    Var(One, Two)
                }
            })
            .unwrap(),
        );
    }
}
