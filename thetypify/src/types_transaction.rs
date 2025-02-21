#![allow(clippy::redundant_closure_call)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::match_single_binding)]
#![allow(clippy::clone_on_copy)]

#[doc = r" Error types."]
pub mod error {
    #[doc = r" Error from a TryFrom or FromStr implementation."]
    pub struct ConversionError(::std::borrow::Cow<'static, str>);
    impl ::std::error::Error for ConversionError {}
    impl ::std::fmt::Display for ConversionError {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> Result<(), ::std::fmt::Error> {
            ::std::fmt::Display::fmt(&self.0, f)
        }
    }
    impl ::std::fmt::Debug for ConversionError {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> Result<(), ::std::fmt::Error> {
            ::std::fmt::Debug::fmt(&self.0, f)
        }
    }
    impl From<&'static str> for ConversionError {
        fn from(value: &'static str) -> Self {
            Self(value.into())
        }
    }
    impl From<String> for ConversionError {
        fn from(value: String) -> Self {
            Self(value.into())
        }
    }
}
#[doc = "Access key provides limited access to an account. Each access key belongs to some account and is identified by a unique (within the account) public key. One account may have large number of access keys. Access keys allow to act on behalf of the account by restricting transactions that can be issued. `account_id,public_key` is a key in the state"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Access key provides limited access to an account. Each access key belongs to some account and is identified by a unique (within the account) public key. One account may have large number of access keys. Access keys allow to act on behalf of the account by restricting transactions that can be issued. `account_id,public_key` is a key in the state\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"nonce\","]
#[doc = "    \"permission\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"nonce\": {"]
#[doc = "      \"description\": \"Nonce for this access key, used for tx nonce generation. When access key is created, nonce is set to `(block_height - 1) * 1e6` to avoid tx hash collision on access key re-creation. See <https://github.com/near/nearcore/issues/3779> for more details.\","]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"permission\": {"]
#[doc = "      \"description\": \"Defines permissions for this access key.\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/AccessKeyPermission\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct AccessKey {
    #[doc = "Nonce for this access key, used for tx nonce generation. When access key is created, nonce is set to `(block_height - 1) * 1e6` to avoid tx hash collision on access key re-creation. See <https://github.com/near/nearcore/issues/3779> for more details."]
    pub nonce: u64,
    #[doc = "Defines permissions for this access key."]
    pub permission: AccessKeyPermission,
}
impl ::std::convert::From<&AccessKey> for AccessKey {
    fn from(value: &AccessKey) -> Self {
        value.clone()
    }
}
impl AccessKey {
    pub fn builder() -> builder::AccessKey {
        Default::default()
    }
}
#[doc = "Defines permissions for AccessKey"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Defines permissions for AccessKey\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"FunctionCall\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"FunctionCall\": {"]
#[doc = "          \"$ref\": \"#/definitions/FunctionCallPermission\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Grants full access to the account. NOTE: It's used to replace account-level public keys.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"FullAccess\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum AccessKeyPermission {
    FunctionCall(FunctionCallPermission),
    #[doc = "Grants full access to the account. NOTE: It's used to replace account-level public keys."]
    FullAccess,
}
impl ::std::convert::From<&Self> for AccessKeyPermission {
    fn from(value: &AccessKeyPermission) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<FunctionCallPermission> for AccessKeyPermission {
    fn from(value: FunctionCallPermission) -> Self {
        Self::FunctionCall(value)
    }
}
#[doc = "AccessKeyPermissionView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"FullAccess\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"FunctionCall\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"FunctionCall\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"allowance\","]
#[doc = "            \"method_names\","]
#[doc = "            \"receiver_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"allowance\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"method_names\": {"]
#[doc = "              \"type\": \"array\","]
#[doc = "              \"items\": {"]
#[doc = "                \"type\": \"string\""]
#[doc = "              }"]
#[doc = "            },"]
#[doc = "            \"receiver_id\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum AccessKeyPermissionView {
    FullAccess,
    FunctionCall {
        allowance: ::std::string::String,
        method_names: ::std::vec::Vec<::std::string::String>,
        receiver_id: ::std::string::String,
    },
}
impl ::std::convert::From<&Self> for AccessKeyPermissionView {
    fn from(value: &AccessKeyPermissionView) -> Self {
        value.clone()
    }
}
#[doc = "AccessKeyView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"nonce\","]
#[doc = "    \"permission\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"nonce\": {"]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"permission\": {"]
#[doc = "      \"$ref\": \"#/definitions/AccessKeyPermissionView\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct AccessKeyView {
    pub nonce: u64,
    pub permission: AccessKeyPermissionView,
}
impl ::std::convert::From<&AccessKeyView> for AccessKeyView {
    fn from(value: &AccessKeyView) -> Self {
        value.clone()
    }
}
impl AccessKeyView {
    pub fn builder() -> builder::AccessKeyView {
        Default::default()
    }
}
#[doc = "NEAR Account Identifier.\n\nThis is a unique, syntactically valid, human-readable account identifier on the NEAR network.\n\n[See the crate-level docs for information about validation.](index.html#account-id-rules)\n\nAlso see [Error kind precedence](AccountId#error-kind-precedence).\n\n## Examples\n\n``` use near_account_id::AccountId;\n\nlet alice: AccountId = \"alice.near\".parse().unwrap();\n\nassert!(\"ƒelicia.near\".parse::<AccountId>().is_err()); // (ƒ is not f) ```"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"NEAR Account Identifier.\\n\\nThis is a unique, syntactically valid, human-readable account identifier on the NEAR network.\\n\\n[See the crate-level docs for information about validation.](index.html#account-id-rules)\\n\\nAlso see [Error kind precedence](AccountId#error-kind-precedence).\\n\\n## Examples\\n\\n``` use near_account_id::AccountId;\\n\\nlet alice: AccountId = \\\"alice.near\\\".parse().unwrap();\\n\\nassert!(\\\"ƒelicia.near\\\".parse::<AccountId>().is_err()); // (ƒ is not f) ```\","]
#[doc = "  \"type\": \"string\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[serde(transparent)]
pub struct AccountId(pub ::std::string::String);
impl ::std::ops::Deref for AccountId {
    type Target = ::std::string::String;
    fn deref(&self) -> &::std::string::String {
        &self.0
    }
}
impl ::std::convert::From<AccountId> for ::std::string::String {
    fn from(value: AccountId) -> Self {
        value.0
    }
}
impl ::std::convert::From<&AccountId> for AccountId {
    fn from(value: &AccountId) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<::std::string::String> for AccountId {
    fn from(value: ::std::string::String) -> Self {
        Self(value)
    }
}
impl ::std::str::FromStr for AccountId {
    type Err = ::std::convert::Infallible;
    fn from_str(value: &str) -> ::std::result::Result<Self, Self::Err> {
        Ok(Self(value.to_string()))
    }
}
impl ::std::fmt::Display for AccountId {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        self.0.fmt(f)
    }
}
#[doc = "Action"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"Create an (sub)account using a transaction `receiver_id` as an ID for a new account ID must pass validation rules described here <http://nomicon.io/Primitives/Account.html>.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"CreateAccount\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"CreateAccount\": {"]
#[doc = "          \"$ref\": \"#/definitions/CreateAccountAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Sets a Wasm code to a receiver_id\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeployContract\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeployContract\": {"]
#[doc = "          \"$ref\": \"#/definitions/DeployContractAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"FunctionCall\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"FunctionCall\": {"]
#[doc = "          \"$ref\": \"#/definitions/FunctionCallAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Transfer\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Transfer\": {"]
#[doc = "          \"$ref\": \"#/definitions/TransferAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Stake\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Stake\": {"]
#[doc = "          \"$ref\": \"#/definitions/StakeAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AddKey\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AddKey\": {"]
#[doc = "          \"$ref\": \"#/definitions/AddKeyAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeleteKey\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeleteKey\": {"]
#[doc = "          \"$ref\": \"#/definitions/DeleteKeyAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeleteAccount\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeleteAccount\": {"]
#[doc = "          \"$ref\": \"#/definitions/DeleteAccountAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Delegate\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Delegate\": {"]
#[doc = "          \"$ref\": \"#/definitions/SignedDelegateAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeployGlobalContract\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeployGlobalContract\": {"]
#[doc = "          \"$ref\": \"#/definitions/DeployGlobalContractAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"UseGlobalContract\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"UseGlobalContract\": {"]
#[doc = "          \"$ref\": \"#/definitions/UseGlobalContractAction\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum Action {
    #[doc = "Create an (sub)account using a transaction `receiver_id` as an ID for a new account ID must pass validation rules described here <http://nomicon.io/Primitives/Account.html>."]
    CreateAccount(CreateAccountAction),
    #[doc = "Sets a Wasm code to a receiver_id"]
    DeployContract(DeployContractAction),
    FunctionCall(FunctionCallAction),
    Transfer(TransferAction),
    Stake(StakeAction),
    AddKey(AddKeyAction),
    DeleteKey(DeleteKeyAction),
    DeleteAccount(DeleteAccountAction),
    Delegate(SignedDelegateAction),
    DeployGlobalContract(DeployGlobalContractAction),
    UseGlobalContract(UseGlobalContractAction),
}
impl ::std::convert::From<&Self> for Action {
    fn from(value: &Action) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<CreateAccountAction> for Action {
    fn from(value: CreateAccountAction) -> Self {
        Self::CreateAccount(value)
    }
}
impl ::std::convert::From<DeployContractAction> for Action {
    fn from(value: DeployContractAction) -> Self {
        Self::DeployContract(value)
    }
}
impl ::std::convert::From<FunctionCallAction> for Action {
    fn from(value: FunctionCallAction) -> Self {
        Self::FunctionCall(value)
    }
}
impl ::std::convert::From<TransferAction> for Action {
    fn from(value: TransferAction) -> Self {
        Self::Transfer(value)
    }
}
impl ::std::convert::From<StakeAction> for Action {
    fn from(value: StakeAction) -> Self {
        Self::Stake(value)
    }
}
impl ::std::convert::From<AddKeyAction> for Action {
    fn from(value: AddKeyAction) -> Self {
        Self::AddKey(value)
    }
}
impl ::std::convert::From<DeleteKeyAction> for Action {
    fn from(value: DeleteKeyAction) -> Self {
        Self::DeleteKey(value)
    }
}
impl ::std::convert::From<DeleteAccountAction> for Action {
    fn from(value: DeleteAccountAction) -> Self {
        Self::DeleteAccount(value)
    }
}
impl ::std::convert::From<SignedDelegateAction> for Action {
    fn from(value: SignedDelegateAction) -> Self {
        Self::Delegate(value)
    }
}
impl ::std::convert::From<DeployGlobalContractAction> for Action {
    fn from(value: DeployGlobalContractAction) -> Self {
        Self::DeployGlobalContract(value)
    }
}
impl ::std::convert::From<UseGlobalContractAction> for Action {
    fn from(value: UseGlobalContractAction) -> Self {
        Self::UseGlobalContract(value)
    }
}
#[doc = "An error happened during Action execution"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"An error happened during Action execution\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"kind\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"index\": {"]
#[doc = "      \"description\": \"Index of the failed action in the transaction. Action index is not defined if ActionError.kind is `ActionErrorKind::LackBalanceForState`\","]
#[doc = "      \"type\": ["]
#[doc = "        \"integer\","]
#[doc = "        \"null\""]
#[doc = "      ],"]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"kind\": {"]
#[doc = "      \"description\": \"The kind of ActionError happened\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/ActionErrorKind\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct ActionError {
    #[doc = "Index of the failed action in the transaction. Action index is not defined if ActionError.kind is `ActionErrorKind::LackBalanceForState`"]
    #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
    pub index: ::std::option::Option<u64>,
    #[doc = "The kind of ActionError happened"]
    pub kind: ActionErrorKind,
}
impl ::std::convert::From<&ActionError> for ActionError {
    fn from(value: &ActionError) -> Self {
        value.clone()
    }
}
impl ActionError {
    pub fn builder() -> builder::ActionError {
        Default::default()
    }
}
#[doc = "ActionErrorKind"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"Happens when CreateAccount action tries to create an account with account_id which is already exists in the storage\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AccountAlreadyExists\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AccountAlreadyExists\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Happens when TX receiver_id doesn't exist (but action is not Action::CreateAccount)\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AccountDoesNotExist\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AccountDoesNotExist\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"A top-level account ID can only be created by registrar.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"CreateAccountOnlyByRegistrar\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"CreateAccountOnlyByRegistrar\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"predecessor_id\","]
#[doc = "            \"registrar_account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"predecessor_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"registrar_account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"A newly created account must be under a namespace of the creator account\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"CreateAccountNotAllowed\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"CreateAccountNotAllowed\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"predecessor_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"predecessor_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Administrative actions like `DeployContract`, `Stake`, `AddKey`, `DeleteKey`. can be proceed only if sender=receiver or the first TX action is a `CreateAccount` action\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ActorNoPermission\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ActorNoPermission\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"actor_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"actor_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Account tries to remove an access key that doesn't exist\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeleteKeyDoesNotExist\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeleteKeyDoesNotExist\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"public_key\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"public_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The public key is already used for an existing access key\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AddKeyAlreadyExists\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AddKeyAlreadyExists\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"public_key\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"public_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Account is staking and can not be deleted\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeleteAccountStaking\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeleteAccountStaking\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"ActionReceipt can't be completed, because the remaining balance will not be enough to cover storage.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"LackBalanceForState\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"LackBalanceForState\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"amount\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"description\": \"An account which needs balance\","]
#[doc = "              \"allOf\": ["]
#[doc = "                {"]
#[doc = "                  \"$ref\": \"#/definitions/AccountId\""]
#[doc = "                }"]
#[doc = "              ]"]
#[doc = "            },"]
#[doc = "            \"amount\": {"]
#[doc = "              \"description\": \"Balance required to complete an action.\","]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Account is not yet staked, but tries to unstake\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"TriesToUnstake\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"TriesToUnstake\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The account doesn't have enough balance to increase the stake.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"TriesToStake\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"TriesToStake\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"balance\","]
#[doc = "            \"locked\","]
#[doc = "            \"stake\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"balance\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"locked\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"stake\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InsufficientStake\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InsufficientStake\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"minimum_stake\","]
#[doc = "            \"stake\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"minimum_stake\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"stake\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"An error occurred during a `FunctionCall` Action, parameter is debug message.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"FunctionCallError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"FunctionCallError\": {"]
#[doc = "          \"$ref\": \"#/definitions/FunctionCallError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Error occurs when a new `ActionReceipt` created by the `FunctionCall` action fails receipt validation.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"NewReceiptValidationError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"NewReceiptValidationError\": {"]
#[doc = "          \"$ref\": \"#/definitions/ReceiptValidationError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Error occurs when a `CreateAccount` action is called on a NEAR-implicit or ETH-implicit account. See NEAR-implicit account creation NEP: <https://github.com/nearprotocol/NEPs/pull/71>. Also, see ETH-implicit account creation NEP: <https://github.com/near/NEPs/issues/518>.\\n\\nTODO(#8598): This error is named very poorly. A better name would be `OnlyNamedAccountCreationAllowed`.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"OnlyImplicitAccountCreationAllowed\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"OnlyImplicitAccountCreationAllowed\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Delete account whose state is large is temporarily banned.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeleteAccountWithLargeState\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeleteAccountWithLargeState\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Signature does not match the provided actions and given signer public key.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"DelegateActionInvalidSignature\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Receiver of the transaction doesn't match Sender of the delegate action\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DelegateActionSenderDoesNotMatchTxReceiver\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DelegateActionSenderDoesNotMatchTxReceiver\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"receiver_id\","]
#[doc = "            \"sender_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"receiver_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"sender_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Delegate action has expired. `max_block_height` is less than actual block height.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"DelegateActionExpired\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The given public key doesn't exist for Sender account\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DelegateActionAccessKeyError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DelegateActionAccessKeyError\": {"]
#[doc = "          \"$ref\": \"#/definitions/InvalidAccessKeyError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"DelegateAction nonce must be greater sender[public_key].nonce\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DelegateActionInvalidNonce\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DelegateActionInvalidNonce\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"ak_nonce\","]
#[doc = "            \"delegate_nonce\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"ak_nonce\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"delegate_nonce\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"DelegateAction nonce is larger than the upper bound given by the block height\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DelegateActionNonceTooLarge\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DelegateActionNonceTooLarge\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"delegate_nonce\","]
#[doc = "            \"upper_bound\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"delegate_nonce\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"upper_bound\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"GlobalContractDoesNotExist\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"GlobalContractDoesNotExist\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"identifier\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"identifier\": {"]
#[doc = "              \"$ref\": \"#/definitions/GlobalContractIdentifier\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum ActionErrorKind {
    #[doc = "Happens when CreateAccount action tries to create an account with account_id which is already exists in the storage"]
    AccountAlreadyExists { account_id: AccountId },
    #[doc = "Happens when TX receiver_id doesn't exist (but action is not Action::CreateAccount)"]
    AccountDoesNotExist { account_id: AccountId },
    #[doc = "A top-level account ID can only be created by registrar."]
    CreateAccountOnlyByRegistrar {
        account_id: AccountId,
        predecessor_id: AccountId,
        registrar_account_id: AccountId,
    },
    #[doc = "A newly created account must be under a namespace of the creator account"]
    CreateAccountNotAllowed {
        account_id: AccountId,
        predecessor_id: AccountId,
    },
    #[doc = "Administrative actions like `DeployContract`, `Stake`, `AddKey`, `DeleteKey`. can be proceed only if sender=receiver or the first TX action is a `CreateAccount` action"]
    ActorNoPermission {
        account_id: AccountId,
        actor_id: AccountId,
    },
    #[doc = "Account tries to remove an access key that doesn't exist"]
    DeleteKeyDoesNotExist {
        account_id: AccountId,
        public_key: PublicKey,
    },
    #[doc = "The public key is already used for an existing access key"]
    AddKeyAlreadyExists {
        account_id: AccountId,
        public_key: PublicKey,
    },
    #[doc = "Account is staking and can not be deleted"]
    DeleteAccountStaking { account_id: AccountId },
    #[doc = "ActionReceipt can't be completed, because the remaining balance will not be enough to cover storage."]
    LackBalanceForState {
        #[doc = "An account which needs balance"]
        account_id: AccountId,
        #[doc = "Balance required to complete an action."]
        amount: ::std::string::String,
    },
    #[doc = "Account is not yet staked, but tries to unstake"]
    TriesToUnstake { account_id: AccountId },
    #[doc = "The account doesn't have enough balance to increase the stake."]
    TriesToStake {
        account_id: AccountId,
        balance: ::std::string::String,
        locked: ::std::string::String,
        stake: ::std::string::String,
    },
    InsufficientStake {
        account_id: AccountId,
        minimum_stake: ::std::string::String,
        stake: ::std::string::String,
    },
    #[doc = "An error occurred during a `FunctionCall` Action, parameter is debug message."]
    FunctionCallError(FunctionCallError),
    #[doc = "Error occurs when a new `ActionReceipt` created by the `FunctionCall` action fails receipt validation."]
    NewReceiptValidationError(ReceiptValidationError),
    #[doc = "Error occurs when a `CreateAccount` action is called on a NEAR-implicit or ETH-implicit account. See NEAR-implicit account creation NEP: <https://github.com/nearprotocol/NEPs/pull/71>. Also, see ETH-implicit account creation NEP: <https://github.com/near/NEPs/issues/518>.\n\nTODO(#8598): This error is named very poorly. A better name would be `OnlyNamedAccountCreationAllowed`."]
    OnlyImplicitAccountCreationAllowed { account_id: AccountId },
    #[doc = "Delete account whose state is large is temporarily banned."]
    DeleteAccountWithLargeState { account_id: AccountId },
    #[doc = "Signature does not match the provided actions and given signer public key."]
    DelegateActionInvalidSignature,
    #[doc = "Receiver of the transaction doesn't match Sender of the delegate action"]
    DelegateActionSenderDoesNotMatchTxReceiver {
        receiver_id: AccountId,
        sender_id: AccountId,
    },
    #[doc = "Delegate action has expired. `max_block_height` is less than actual block height."]
    DelegateActionExpired,
    #[doc = "The given public key doesn't exist for Sender account"]
    DelegateActionAccessKeyError(InvalidAccessKeyError),
    #[doc = "DelegateAction nonce must be greater sender[public_key].nonce"]
    DelegateActionInvalidNonce { ak_nonce: u64, delegate_nonce: u64 },
    #[doc = "DelegateAction nonce is larger than the upper bound given by the block height"]
    DelegateActionNonceTooLarge {
        delegate_nonce: u64,
        upper_bound: u64,
    },
    GlobalContractDoesNotExist {
        identifier: GlobalContractIdentifier,
    },
}
impl ::std::convert::From<&Self> for ActionErrorKind {
    fn from(value: &ActionErrorKind) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<FunctionCallError> for ActionErrorKind {
    fn from(value: FunctionCallError) -> Self {
        Self::FunctionCallError(value)
    }
}
impl ::std::convert::From<ReceiptValidationError> for ActionErrorKind {
    fn from(value: ReceiptValidationError) -> Self {
        Self::NewReceiptValidationError(value)
    }
}
impl ::std::convert::From<InvalidAccessKeyError> for ActionErrorKind {
    fn from(value: InvalidAccessKeyError) -> Self {
        Self::DelegateActionAccessKeyError(value)
    }
}
#[doc = "ActionView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"CreateAccount\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeployContract\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeployContract\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"code\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"code\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"FunctionCall\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"FunctionCall\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"args\","]
#[doc = "            \"deposit\","]
#[doc = "            \"gas\","]
#[doc = "            \"method_name\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"args\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"deposit\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"gas\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"method_name\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Transfer\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Transfer\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"deposit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"deposit\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Stake\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Stake\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"public_key\","]
#[doc = "            \"stake\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"public_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "            },"]
#[doc = "            \"stake\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AddKey\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AddKey\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"access_key\","]
#[doc = "            \"public_key\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"access_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccessKeyView\""]
#[doc = "            },"]
#[doc = "            \"public_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeleteKey\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeleteKey\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"public_key\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"public_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeleteAccount\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeleteAccount\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"beneficiary_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"beneficiary_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Delegate\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Delegate\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"delegate_action\","]
#[doc = "            \"signature\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"delegate_action\": {"]
#[doc = "              \"$ref\": \"#/definitions/DelegateAction\""]
#[doc = "            },"]
#[doc = "            \"signature\": {"]
#[doc = "              \"$ref\": \"#/definitions/Signature\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeployGlobalContract\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeployGlobalContract\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"code\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"code\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"DeployGlobalContractByAccountId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"DeployGlobalContractByAccountId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"code\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"code\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"UseGlobalContract\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"UseGlobalContract\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"code_hash\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"code_hash\": {"]
#[doc = "              \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"UseGlobalContractByAccountId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"UseGlobalContractByAccountId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum ActionView {
    CreateAccount,
    DeployContract {
        code: ::std::string::String,
    },
    FunctionCall {
        args: ::std::string::String,
        deposit: ::std::string::String,
        gas: u64,
        method_name: ::std::string::String,
    },
    Transfer {
        deposit: ::std::string::String,
    },
    Stake {
        public_key: PublicKey,
        stake: ::std::string::String,
    },
    AddKey {
        access_key: AccessKeyView,
        public_key: PublicKey,
    },
    DeleteKey {
        public_key: PublicKey,
    },
    DeleteAccount {
        beneficiary_id: AccountId,
    },
    Delegate {
        delegate_action: DelegateAction,
        signature: Signature,
    },
    DeployGlobalContract {
        code: ::std::string::String,
    },
    DeployGlobalContractByAccountId {
        code: ::std::string::String,
    },
    UseGlobalContract {
        code_hash: CryptoHash,
    },
    UseGlobalContractByAccountId {
        account_id: AccountId,
    },
}
impl ::std::convert::From<&Self> for ActionView {
    fn from(value: &ActionView) -> Self {
        value.clone()
    }
}
#[doc = "Describes the error for validating a list of actions."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Describes the error for validating a list of actions.\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"The delete action must be a final action in transaction\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"DeleteActionMustBeFinal\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The total prepaid gas (for all given actions) exceeded the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"TotalPrepaidGasExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"TotalPrepaidGasExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"total_prepaid_gas\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"total_prepaid_gas\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The number of actions exceeded the given limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"TotalNumberOfActionsExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"TotalNumberOfActionsExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"total_number_of_actions\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"total_number_of_actions\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The total number of bytes of the method names exceeded the limit in a Add Key action.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AddKeyMethodNamesNumberOfBytesExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AddKeyMethodNamesNumberOfBytesExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"total_number_of_bytes\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"total_number_of_bytes\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The length of some method name exceeded the limit in a Add Key action.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AddKeyMethodNameLengthExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AddKeyMethodNameLengthExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"length\","]
#[doc = "            \"limit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"length\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Integer overflow during a compute.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"IntegerOverflow\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Invalid account ID.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidAccountId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidAccountId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The size of the contract code exceeded the limit in a DeployContract action.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ContractSizeExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ContractSizeExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"size\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"size\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The length of the method name exceeded the limit in a Function Call action.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"FunctionCallMethodNameLengthExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"FunctionCallMethodNameLengthExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"length\","]
#[doc = "            \"limit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"length\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The length of the arguments exceeded the limit in a Function Call action.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"FunctionCallArgumentsLengthExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"FunctionCallArgumentsLengthExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"length\","]
#[doc = "            \"limit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"length\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"An attempt to stake with a public key that is not convertible to ristretto.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"UnsuitableStakingKey\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"UnsuitableStakingKey\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"public_key\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"public_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The attached amount of gas in a FunctionCall action has to be a positive number.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"FunctionCallZeroAttachedGas\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"There should be the only one DelegateAction\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"DelegateActionMustBeOnlyOne\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The transaction includes a feature that the current protocol version does not support.\\n\\nNote: we stringify the protocol feature name instead of using `ProtocolFeature` here because we don't want to leak the internals of that type into observable borsh serialization.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"UnsupportedProtocolFeature\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"UnsupportedProtocolFeature\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"protocol_feature\","]
#[doc = "            \"version\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"protocol_feature\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"version\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint32\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum ActionsValidationError {
    #[doc = "The delete action must be a final action in transaction"]
    DeleteActionMustBeFinal,
    #[doc = "The total prepaid gas (for all given actions) exceeded the limit."]
    TotalPrepaidGasExceeded { limit: u64, total_prepaid_gas: u64 },
    #[doc = "The number of actions exceeded the given limit."]
    TotalNumberOfActionsExceeded {
        limit: u64,
        total_number_of_actions: u64,
    },
    #[doc = "The total number of bytes of the method names exceeded the limit in a Add Key action."]
    AddKeyMethodNamesNumberOfBytesExceeded {
        limit: u64,
        total_number_of_bytes: u64,
    },
    #[doc = "The length of some method name exceeded the limit in a Add Key action."]
    AddKeyMethodNameLengthExceeded { length: u64, limit: u64 },
    #[doc = "Integer overflow during a compute."]
    IntegerOverflow,
    #[doc = "Invalid account ID."]
    InvalidAccountId { account_id: ::std::string::String },
    #[doc = "The size of the contract code exceeded the limit in a DeployContract action."]
    ContractSizeExceeded { limit: u64, size: u64 },
    #[doc = "The length of the method name exceeded the limit in a Function Call action."]
    FunctionCallMethodNameLengthExceeded { length: u64, limit: u64 },
    #[doc = "The length of the arguments exceeded the limit in a Function Call action."]
    FunctionCallArgumentsLengthExceeded { length: u64, limit: u64 },
    #[doc = "An attempt to stake with a public key that is not convertible to ristretto."]
    UnsuitableStakingKey { public_key: PublicKey },
    #[doc = "The attached amount of gas in a FunctionCall action has to be a positive number."]
    FunctionCallZeroAttachedGas,
    #[doc = "There should be the only one DelegateAction"]
    DelegateActionMustBeOnlyOne,
    #[doc = "The transaction includes a feature that the current protocol version does not support.\n\nNote: we stringify the protocol feature name instead of using `ProtocolFeature` here because we don't want to leak the internals of that type into observable borsh serialization."]
    UnsupportedProtocolFeature {
        protocol_feature: ::std::string::String,
        version: u32,
    },
}
impl ::std::convert::From<&Self> for ActionsValidationError {
    fn from(value: &ActionsValidationError) -> Self {
        value.clone()
    }
}
#[doc = "AddKeyAction"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"access_key\","]
#[doc = "    \"public_key\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"access_key\": {"]
#[doc = "      \"description\": \"An access key with the permission\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/AccessKey\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"public_key\": {"]
#[doc = "      \"description\": \"A public key which will be associated with an access_key\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct AddKeyAction {
    #[doc = "An access key with the permission"]
    pub access_key: AccessKey,
    #[doc = "A public key which will be associated with an access_key"]
    pub public_key: PublicKey,
}
impl ::std::convert::From<&AddKeyAction> for AddKeyAction {
    fn from(value: &AddKeyAction) -> Self {
        value.clone()
    }
}
impl AddKeyAction {
    pub fn builder() -> builder::AddKeyAction {
        Default::default()
    }
}
#[doc = "CompilationError"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"CodeDoesNotExist\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"CodeDoesNotExist\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"PrepareError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"PrepareError\": {"]
#[doc = "          \"$ref\": \"#/definitions/PrepareError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"This is for defense in depth. We expect our runtime-independent preparation code to fully catch all invalid wasms, but, if it ever misses something we’ll emit this error\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"WasmerCompileError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"WasmerCompileError\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"msg\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"msg\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum CompilationError {
    CodeDoesNotExist {
        account_id: AccountId,
    },
    PrepareError(PrepareError),
    #[doc = "This is for defense in depth. We expect our runtime-independent preparation code to fully catch all invalid wasms, but, if it ever misses something we’ll emit this error"]
    WasmerCompileError {
        msg: ::std::string::String,
    },
}
impl ::std::convert::From<&Self> for CompilationError {
    fn from(value: &CompilationError) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<PrepareError> for CompilationError {
    fn from(value: PrepareError) -> Self {
        Self::PrepareError(value)
    }
}
#[doc = "CostGasUsed"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"cost\","]
#[doc = "    \"cost_category\","]
#[doc = "    \"gas_used\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"cost\": {"]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"cost_category\": {"]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"gas_used\": {"]
#[doc = "      \"type\": \"string\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct CostGasUsed {
    pub cost: ::std::string::String,
    pub cost_category: ::std::string::String,
    pub gas_used: ::std::string::String,
}
impl ::std::convert::From<&CostGasUsed> for CostGasUsed {
    fn from(value: &CostGasUsed) -> Self {
        value.clone()
    }
}
impl CostGasUsed {
    pub fn builder() -> builder::CostGasUsed {
        Default::default()
    }
}
#[doc = "Create account action"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Create account action\","]
#[doc = "  \"type\": \"object\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
#[serde(transparent)]
pub struct CreateAccountAction(pub ::serde_json::Map<::std::string::String, ::serde_json::Value>);
impl ::std::ops::Deref for CreateAccountAction {
    type Target = ::serde_json::Map<::std::string::String, ::serde_json::Value>;
    fn deref(&self) -> &::serde_json::Map<::std::string::String, ::serde_json::Value> {
        &self.0
    }
}
impl ::std::convert::From<CreateAccountAction>
    for ::serde_json::Map<::std::string::String, ::serde_json::Value>
{
    fn from(value: CreateAccountAction) -> Self {
        value.0
    }
}
impl ::std::convert::From<&CreateAccountAction> for CreateAccountAction {
    fn from(value: &CreateAccountAction) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<::serde_json::Map<::std::string::String, ::serde_json::Value>>
    for CreateAccountAction
{
    fn from(value: ::serde_json::Map<::std::string::String, ::serde_json::Value>) -> Self {
        Self(value)
    }
}
#[doc = "CryptoHash"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"string\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[serde(transparent)]
pub struct CryptoHash(pub ::std::string::String);
impl ::std::ops::Deref for CryptoHash {
    type Target = ::std::string::String;
    fn deref(&self) -> &::std::string::String {
        &self.0
    }
}
impl ::std::convert::From<CryptoHash> for ::std::string::String {
    fn from(value: CryptoHash) -> Self {
        value.0
    }
}
impl ::std::convert::From<&CryptoHash> for CryptoHash {
    fn from(value: &CryptoHash) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<::std::string::String> for CryptoHash {
    fn from(value: ::std::string::String) -> Self {
        Self(value)
    }
}
impl ::std::str::FromStr for CryptoHash {
    type Err = ::std::convert::Infallible;
    fn from_str(value: &str) -> ::std::result::Result<Self, Self::Err> {
        Ok(Self(value.to_string()))
    }
}
impl ::std::fmt::Display for CryptoHash {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        self.0.fmt(f)
    }
}
#[doc = "DataReceiverView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"data_id\","]
#[doc = "    \"receiver_id\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"data_id\": {"]
#[doc = "      \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "    },"]
#[doc = "    \"receiver_id\": {"]
#[doc = "      \"$ref\": \"#/definitions/AccountId\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct DataReceiverView {
    pub data_id: CryptoHash,
    pub receiver_id: AccountId,
}
impl ::std::convert::From<&DataReceiverView> for DataReceiverView {
    fn from(value: &DataReceiverView) -> Self {
        value.clone()
    }
}
impl DataReceiverView {
    pub fn builder() -> builder::DataReceiverView {
        Default::default()
    }
}
#[doc = "This action allows to execute the inner actions behalf of the defined sender."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"This action allows to execute the inner actions behalf of the defined sender.\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"actions\","]
#[doc = "    \"max_block_height\","]
#[doc = "    \"nonce\","]
#[doc = "    \"public_key\","]
#[doc = "    \"receiver_id\","]
#[doc = "    \"sender_id\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"actions\": {"]
#[doc = "      \"description\": \"List of actions to be executed.\\n\\nWith the meta transactions MVP defined in NEP-366, nested DelegateActions are not allowed. A separate type is used to enforce it.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/NonDelegateAction\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"max_block_height\": {"]
#[doc = "      \"description\": \"The maximal height of the block in the blockchain below which the given DelegateAction is valid.\","]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"nonce\": {"]
#[doc = "      \"description\": \"Nonce to ensure that the same delegate action is not sent twice by a relayer and should match for given account's `public_key`. After this action is processed it will increment.\","]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"public_key\": {"]
#[doc = "      \"description\": \"Public key used to sign this delegated action.\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"receiver_id\": {"]
#[doc = "      \"description\": \"Receiver of the delegated actions.\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/AccountId\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"sender_id\": {"]
#[doc = "      \"description\": \"Signer of the delegated actions\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/AccountId\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct DelegateAction {
    #[doc = "List of actions to be executed.\n\nWith the meta transactions MVP defined in NEP-366, nested DelegateActions are not allowed. A separate type is used to enforce it."]
    pub actions: ::std::vec::Vec<NonDelegateAction>,
    #[doc = "The maximal height of the block in the blockchain below which the given DelegateAction is valid."]
    pub max_block_height: u64,
    #[doc = "Nonce to ensure that the same delegate action is not sent twice by a relayer and should match for given account's `public_key`. After this action is processed it will increment."]
    pub nonce: u64,
    #[doc = "Public key used to sign this delegated action."]
    pub public_key: PublicKey,
    #[doc = "Receiver of the delegated actions."]
    pub receiver_id: AccountId,
    #[doc = "Signer of the delegated actions"]
    pub sender_id: AccountId,
}
impl ::std::convert::From<&DelegateAction> for DelegateAction {
    fn from(value: &DelegateAction) -> Self {
        value.clone()
    }
}
impl DelegateAction {
    pub fn builder() -> builder::DelegateAction {
        Default::default()
    }
}
#[doc = "DeleteAccountAction"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"beneficiary_id\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"beneficiary_id\": {"]
#[doc = "      \"$ref\": \"#/definitions/AccountId\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct DeleteAccountAction {
    pub beneficiary_id: AccountId,
}
impl ::std::convert::From<&DeleteAccountAction> for DeleteAccountAction {
    fn from(value: &DeleteAccountAction) -> Self {
        value.clone()
    }
}
impl DeleteAccountAction {
    pub fn builder() -> builder::DeleteAccountAction {
        Default::default()
    }
}
#[doc = "DeleteKeyAction"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"public_key\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"public_key\": {"]
#[doc = "      \"description\": \"A public key associated with the access_key to be deleted.\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct DeleteKeyAction {
    #[doc = "A public key associated with the access_key to be deleted."]
    pub public_key: PublicKey,
}
impl ::std::convert::From<&DeleteKeyAction> for DeleteKeyAction {
    fn from(value: &DeleteKeyAction) -> Self {
        value.clone()
    }
}
impl DeleteKeyAction {
    pub fn builder() -> builder::DeleteKeyAction {
        Default::default()
    }
}
#[doc = "Deploy contract action"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Deploy contract action\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"code\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"code\": {"]
#[doc = "      \"description\": \"WebAssembly binary\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct DeployContractAction {
    #[doc = "WebAssembly binary"]
    pub code: ::std::string::String,
}
impl ::std::convert::From<&DeployContractAction> for DeployContractAction {
    fn from(value: &DeployContractAction) -> Self {
        value.clone()
    }
}
impl DeployContractAction {
    pub fn builder() -> builder::DeployContractAction {
        Default::default()
    }
}
#[doc = "Deploy global contract action"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Deploy global contract action\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"code\","]
#[doc = "    \"deploy_mode\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"code\": {"]
#[doc = "      \"description\": \"WebAssembly binary\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"deploy_mode\": {"]
#[doc = "      \"$ref\": \"#/definitions/GlobalContractDeployMode\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct DeployGlobalContractAction {
    #[doc = "WebAssembly binary"]
    pub code: ::std::string::String,
    pub deploy_mode: GlobalContractDeployMode,
}
impl ::std::convert::From<&DeployGlobalContractAction> for DeployGlobalContractAction {
    fn from(value: &DeployGlobalContractAction) -> Self {
        value.clone()
    }
}
impl DeployGlobalContractAction {
    pub fn builder() -> builder::DeployGlobalContractAction {
        Default::default()
    }
}
#[doc = "Direction"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"Left\","]
#[doc = "    \"Right\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub enum Direction {
    Left,
    Right,
}
impl ::std::convert::From<&Self> for Direction {
    fn from(value: &Direction) -> Self {
        value.clone()
    }
}
impl ::std::fmt::Display for Direction {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match *self {
            Self::Left => write!(f, "Left"),
            Self::Right => write!(f, "Right"),
        }
    }
}
impl ::std::str::FromStr for Direction {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        match value {
            "Left" => Ok(Self::Left),
            "Right" => Ok(Self::Right),
            _ => Err("invalid value".into()),
        }
    }
}
impl ::std::convert::TryFrom<&str> for Direction {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<&::std::string::String> for Direction {
    type Error = self::error::ConversionError;
    fn try_from(
        value: &::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<::std::string::String> for Direction {
    type Error = self::error::ConversionError;
    fn try_from(
        value: ::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "ExecutionMetadataView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"version\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"gas_profile\": {"]
#[doc = "      \"type\": ["]
#[doc = "        \"array\","]
#[doc = "        \"null\""]
#[doc = "      ],"]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/CostGasUsed\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"version\": {"]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint32\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct ExecutionMetadataView {
    #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
    pub gas_profile: ::std::option::Option<::std::vec::Vec<CostGasUsed>>,
    pub version: u32,
}
impl ::std::convert::From<&ExecutionMetadataView> for ExecutionMetadataView {
    fn from(value: &ExecutionMetadataView) -> Self {
        value.clone()
    }
}
impl ExecutionMetadataView {
    pub fn builder() -> builder::ExecutionMetadataView {
        Default::default()
    }
}
#[doc = "ExecutionOutcomeView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"executor_id\","]
#[doc = "    \"gas_burnt\","]
#[doc = "    \"logs\","]
#[doc = "    \"receipt_ids\","]
#[doc = "    \"status\","]
#[doc = "    \"tokens_burnt\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"executor_id\": {"]
#[doc = "      \"description\": \"The id of the account on which the execution happens. For transaction this is signer_id, for receipt this is receiver_id.\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/AccountId\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"gas_burnt\": {"]
#[doc = "      \"description\": \"The amount of the gas burnt by the given transaction or receipt.\","]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"logs\": {"]
#[doc = "      \"description\": \"Logs from this transaction or receipt.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"type\": \"string\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"metadata\": {"]
#[doc = "      \"description\": \"Execution metadata, versioned\","]
#[doc = "      \"default\": {"]
#[doc = "        \"gas_profile\": null,"]
#[doc = "        \"version\": 1"]
#[doc = "      },"]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/ExecutionMetadataView\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"receipt_ids\": {"]
#[doc = "      \"description\": \"Receipt IDs generated by this transaction or receipt.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"status\": {"]
#[doc = "      \"description\": \"Execution status. Contains the result in case of successful execution.\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/ExecutionStatusView\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"tokens_burnt\": {"]
#[doc = "      \"description\": \"The amount of tokens burnt corresponding to the burnt gas amount. This value doesn't always equal to the `gas_burnt` multiplied by the gas price, because the prepaid gas price might be lower than the actual gas price and it creates a deficit.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct ExecutionOutcomeView {
    #[doc = "The id of the account on which the execution happens. For transaction this is signer_id, for receipt this is receiver_id."]
    pub executor_id: AccountId,
    #[doc = "The amount of the gas burnt by the given transaction or receipt."]
    pub gas_burnt: u64,
    #[doc = "Logs from this transaction or receipt."]
    pub logs: ::std::vec::Vec<::std::string::String>,
    #[doc = "Execution metadata, versioned"]
    #[serde(default = "defaults::execution_outcome_view_metadata")]
    pub metadata: ExecutionMetadataView,
    #[doc = "Receipt IDs generated by this transaction or receipt."]
    pub receipt_ids: ::std::vec::Vec<CryptoHash>,
    #[doc = "Execution status. Contains the result in case of successful execution."]
    pub status: ExecutionStatusView,
    #[doc = "The amount of tokens burnt corresponding to the burnt gas amount. This value doesn't always equal to the `gas_burnt` multiplied by the gas price, because the prepaid gas price might be lower than the actual gas price and it creates a deficit."]
    pub tokens_burnt: ::std::string::String,
}
impl ::std::convert::From<&ExecutionOutcomeView> for ExecutionOutcomeView {
    fn from(value: &ExecutionOutcomeView) -> Self {
        value.clone()
    }
}
impl ExecutionOutcomeView {
    pub fn builder() -> builder::ExecutionOutcomeView {
        Default::default()
    }
}
#[doc = "ExecutionOutcomeWithIdView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"block_hash\","]
#[doc = "    \"id\","]
#[doc = "    \"outcome\","]
#[doc = "    \"proof\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"block_hash\": {"]
#[doc = "      \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "    },"]
#[doc = "    \"id\": {"]
#[doc = "      \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "    },"]
#[doc = "    \"outcome\": {"]
#[doc = "      \"$ref\": \"#/definitions/ExecutionOutcomeView\""]
#[doc = "    },"]
#[doc = "    \"proof\": {"]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/MerklePathItem\""]
#[doc = "      }"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct ExecutionOutcomeWithIdView {
    pub block_hash: CryptoHash,
    pub id: CryptoHash,
    pub outcome: ExecutionOutcomeView,
    pub proof: ::std::vec::Vec<MerklePathItem>,
}
impl ::std::convert::From<&ExecutionOutcomeWithIdView> for ExecutionOutcomeWithIdView {
    fn from(value: &ExecutionOutcomeWithIdView) -> Self {
        value.clone()
    }
}
impl ExecutionOutcomeWithIdView {
    pub fn builder() -> builder::ExecutionOutcomeWithIdView {
        Default::default()
    }
}
#[doc = "ExecutionStatusView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"The execution is pending or unknown.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"Unknown\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The execution has failed.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Failure\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Failure\": {"]
#[doc = "          \"$ref\": \"#/definitions/TxExecutionError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The final action succeeded and returned some value or an empty vec encoded in base64.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"SuccessValue\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"SuccessValue\": {"]
#[doc = "          \"type\": \"string\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The final action of the receipt returned a promise or the signed transaction was converted to a receipt. Contains the receipt_id of the generated receipt.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"SuccessReceiptId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"SuccessReceiptId\": {"]
#[doc = "          \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum ExecutionStatusView {
    #[doc = "The execution is pending or unknown."]
    Unknown,
    #[doc = "The execution has failed."]
    Failure(TxExecutionError),
    #[doc = "The final action succeeded and returned some value or an empty vec encoded in base64."]
    SuccessValue(::std::string::String),
    #[doc = "The final action of the receipt returned a promise or the signed transaction was converted to a receipt. Contains the receipt_id of the generated receipt."]
    SuccessReceiptId(CryptoHash),
}
impl ::std::convert::From<&Self> for ExecutionStatusView {
    fn from(value: &ExecutionStatusView) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<TxExecutionError> for ExecutionStatusView {
    fn from(value: TxExecutionError) -> Self {
        Self::Failure(value)
    }
}
impl ::std::convert::From<CryptoHash> for ExecutionStatusView {
    fn from(value: CryptoHash) -> Self {
        Self::SuccessReceiptId(value)
    }
}
#[doc = "Execution outcome of the transaction and all the subsequent receipts. Could be not finalized yet"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Execution outcome of the transaction and all the subsequent receipts. Could be not finalized yet\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"receipts_outcome\","]
#[doc = "    \"status\","]
#[doc = "    \"transaction\","]
#[doc = "    \"transaction_outcome\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"receipts_outcome\": {"]
#[doc = "      \"description\": \"The execution outcome of receipts.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/ExecutionOutcomeWithIdView\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"status\": {"]
#[doc = "      \"description\": \"Execution status defined by chain.rs:get_final_transaction_result FinalExecutionStatus::NotStarted - the tx is not converted to the receipt yet FinalExecutionStatus::Started - we have at least 1 receipt, but the first leaf receipt_id (using dfs) hasn't finished the execution FinalExecutionStatus::Failure - the result of the first leaf receipt_id FinalExecutionStatus::SuccessValue - the result of the first leaf receipt_id\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/FinalExecutionStatus\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"transaction\": {"]
#[doc = "      \"description\": \"Signed Transaction\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/SignedTransactionView\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"transaction_outcome\": {"]
#[doc = "      \"description\": \"The execution outcome of the signed transaction.\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/ExecutionOutcomeWithIdView\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct FinalExecutionOutcomeView {
    #[doc = "The execution outcome of receipts."]
    pub receipts_outcome: ::std::vec::Vec<ExecutionOutcomeWithIdView>,
    #[doc = "Execution status defined by chain.rs:get_final_transaction_result FinalExecutionStatus::NotStarted - the tx is not converted to the receipt yet FinalExecutionStatus::Started - we have at least 1 receipt, but the first leaf receipt_id (using dfs) hasn't finished the execution FinalExecutionStatus::Failure - the result of the first leaf receipt_id FinalExecutionStatus::SuccessValue - the result of the first leaf receipt_id"]
    pub status: FinalExecutionStatus,
    #[doc = "Signed Transaction"]
    pub transaction: SignedTransactionView,
    #[doc = "The execution outcome of the signed transaction."]
    pub transaction_outcome: ExecutionOutcomeWithIdView,
}
impl ::std::convert::From<&FinalExecutionOutcomeView> for FinalExecutionOutcomeView {
    fn from(value: &FinalExecutionOutcomeView) -> Self {
        value.clone()
    }
}
impl FinalExecutionOutcomeView {
    pub fn builder() -> builder::FinalExecutionOutcomeView {
        Default::default()
    }
}
#[doc = "Final execution outcome of the transaction and all of subsequent the receipts. Also includes the generated receipt."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Final execution outcome of the transaction and all of subsequent the receipts. Also includes the generated receipt.\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"receipts\","]
#[doc = "    \"receipts_outcome\","]
#[doc = "    \"status\","]
#[doc = "    \"transaction\","]
#[doc = "    \"transaction_outcome\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"receipts\": {"]
#[doc = "      \"description\": \"Receipts generated from the transaction\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/ReceiptView\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"receipts_outcome\": {"]
#[doc = "      \"description\": \"The execution outcome of receipts.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/ExecutionOutcomeWithIdView\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"status\": {"]
#[doc = "      \"description\": \"Execution status defined by chain.rs:get_final_transaction_result FinalExecutionStatus::NotStarted - the tx is not converted to the receipt yet FinalExecutionStatus::Started - we have at least 1 receipt, but the first leaf receipt_id (using dfs) hasn't finished the execution FinalExecutionStatus::Failure - the result of the first leaf receipt_id FinalExecutionStatus::SuccessValue - the result of the first leaf receipt_id\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/FinalExecutionStatus\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"transaction\": {"]
#[doc = "      \"description\": \"Signed Transaction\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/SignedTransactionView\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"transaction_outcome\": {"]
#[doc = "      \"description\": \"The execution outcome of the signed transaction.\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/ExecutionOutcomeWithIdView\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct FinalExecutionOutcomeWithReceiptView {
    #[doc = "Receipts generated from the transaction"]
    pub receipts: ::std::vec::Vec<ReceiptView>,
    #[doc = "The execution outcome of receipts."]
    pub receipts_outcome: ::std::vec::Vec<ExecutionOutcomeWithIdView>,
    #[doc = "Execution status defined by chain.rs:get_final_transaction_result FinalExecutionStatus::NotStarted - the tx is not converted to the receipt yet FinalExecutionStatus::Started - we have at least 1 receipt, but the first leaf receipt_id (using dfs) hasn't finished the execution FinalExecutionStatus::Failure - the result of the first leaf receipt_id FinalExecutionStatus::SuccessValue - the result of the first leaf receipt_id"]
    pub status: FinalExecutionStatus,
    #[doc = "Signed Transaction"]
    pub transaction: SignedTransactionView,
    #[doc = "The execution outcome of the signed transaction."]
    pub transaction_outcome: ExecutionOutcomeWithIdView,
}
impl ::std::convert::From<&FinalExecutionOutcomeWithReceiptView>
    for FinalExecutionOutcomeWithReceiptView
{
    fn from(value: &FinalExecutionOutcomeWithReceiptView) -> Self {
        value.clone()
    }
}
impl FinalExecutionOutcomeWithReceiptView {
    pub fn builder() -> builder::FinalExecutionOutcomeWithReceiptView {
        Default::default()
    }
}
#[doc = "FinalExecutionStatus"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"The execution has not yet started.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"NotStarted\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The execution has started and still going.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"Started\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The execution has failed with the given error.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Failure\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Failure\": {"]
#[doc = "          \"$ref\": \"#/definitions/TxExecutionError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The execution has succeeded and returned some value or an empty vec encoded in base64.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"SuccessValue\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"SuccessValue\": {"]
#[doc = "          \"type\": \"string\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum FinalExecutionStatus {
    #[doc = "The execution has not yet started."]
    NotStarted,
    #[doc = "The execution has started and still going."]
    Started,
    #[doc = "The execution has failed with the given error."]
    Failure(TxExecutionError),
    #[doc = "The execution has succeeded and returned some value or an empty vec encoded in base64."]
    SuccessValue(::std::string::String),
}
impl ::std::convert::From<&Self> for FinalExecutionStatus {
    fn from(value: &FinalExecutionStatus) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<TxExecutionError> for FinalExecutionStatus {
    fn from(value: TxExecutionError) -> Self {
        Self::Failure(value)
    }
}
#[doc = "FunctionCallAction"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"args\","]
#[doc = "    \"deposit\","]
#[doc = "    \"gas\","]
#[doc = "    \"method_name\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"args\": {"]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"deposit\": {"]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"gas\": {"]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"method_name\": {"]
#[doc = "      \"type\": \"string\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct FunctionCallAction {
    pub args: ::std::string::String,
    pub deposit: ::std::string::String,
    pub gas: u64,
    pub method_name: ::std::string::String,
}
impl ::std::convert::From<&FunctionCallAction> for FunctionCallAction {
    fn from(value: &FunctionCallAction) -> Self {
        value.clone()
    }
}
impl FunctionCallAction {
    pub fn builder() -> builder::FunctionCallAction {
        Default::default()
    }
}
#[doc = "Serializable version of `near-vm-runner::FunctionCallError`.\n\nMust never reorder/remove elements, can only add new variants at the end (but do that very carefully). It describes stable serialization format, and only used by serialization logic."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Serializable version of `near-vm-runner::FunctionCallError`.\\n\\nMust never reorder/remove elements, can only add new variants at the end (but do that very carefully). It describes stable serialization format, and only used by serialization logic.\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"WasmUnknownError\","]
#[doc = "        \"_EVMError\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Wasm compilation error\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"CompilationError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"CompilationError\": {"]
#[doc = "          \"$ref\": \"#/definitions/CompilationError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Wasm binary env link error\\n\\nNote: this is only to deserialize old data, use execution error for new data\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"LinkError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"LinkError\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"msg\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"msg\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Import/export resolve error\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"MethodResolveError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"MethodResolveError\": {"]
#[doc = "          \"$ref\": \"#/definitions/MethodResolveError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"A trap happened during execution of a binary\\n\\nNote: this is only to deserialize old data, use execution error for new data\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"WasmTrap\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"WasmTrap\": {"]
#[doc = "          \"$ref\": \"#/definitions/WasmTrap\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Note: this is only to deserialize old data, use execution error for new data\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"HostError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"HostError\": {"]
#[doc = "          \"$ref\": \"#/definitions/HostError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ExecutionError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ExecutionError\": {"]
#[doc = "          \"type\": \"string\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum FunctionCallError {
    WasmUnknownError,
    #[serde(rename = "_EVMError")]
    EvmError,
    #[doc = "Wasm compilation error"]
    CompilationError(CompilationError),
    #[doc = "Wasm binary env link error\n\nNote: this is only to deserialize old data, use execution error for new data"]
    LinkError {
        msg: ::std::string::String,
    },
    #[doc = "Import/export resolve error"]
    MethodResolveError(MethodResolveError),
    #[doc = "A trap happened during execution of a binary\n\nNote: this is only to deserialize old data, use execution error for new data"]
    WasmTrap(WasmTrap),
    #[doc = "Note: this is only to deserialize old data, use execution error for new data"]
    HostError(HostError),
    ExecutionError(::std::string::String),
}
impl ::std::convert::From<&Self> for FunctionCallError {
    fn from(value: &FunctionCallError) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<CompilationError> for FunctionCallError {
    fn from(value: CompilationError) -> Self {
        Self::CompilationError(value)
    }
}
impl ::std::convert::From<MethodResolveError> for FunctionCallError {
    fn from(value: MethodResolveError) -> Self {
        Self::MethodResolveError(value)
    }
}
impl ::std::convert::From<WasmTrap> for FunctionCallError {
    fn from(value: WasmTrap) -> Self {
        Self::WasmTrap(value)
    }
}
impl ::std::convert::From<HostError> for FunctionCallError {
    fn from(value: HostError) -> Self {
        Self::HostError(value)
    }
}
#[doc = "Grants limited permission to make transactions with FunctionCallActions The permission can limit the allowed balance to be spent on the prepaid gas. It also restrict the account ID of the receiver for this function call. It also can restrict the method name for the allowed function calls."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Grants limited permission to make transactions with FunctionCallActions The permission can limit the allowed balance to be spent on the prepaid gas. It also restrict the account ID of the receiver for this function call. It also can restrict the method name for the allowed function calls.\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"allowance\","]
#[doc = "    \"method_names\","]
#[doc = "    \"receiver_id\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"allowance\": {"]
#[doc = "      \"description\": \"Allowance is a balance limit to use by this access key to pay for function call gas and transaction fees. When this access key is used, both account balance and the allowance is decreased by the same value. `None` means unlimited allowance. NOTE: To change or increase the allowance, the old access key needs to be deleted and a new access key should be created.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"method_names\": {"]
#[doc = "      \"description\": \"A list of method names that can be used. The access key only allows transactions with the function call of one of the given method names. Empty list means any method name can be used.\","]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"type\": \"string\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"receiver_id\": {"]
#[doc = "      \"description\": \"The access key only allows transactions with the given receiver's account id.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct FunctionCallPermission {
    #[doc = "Allowance is a balance limit to use by this access key to pay for function call gas and transaction fees. When this access key is used, both account balance and the allowance is decreased by the same value. `None` means unlimited allowance. NOTE: To change or increase the allowance, the old access key needs to be deleted and a new access key should be created."]
    pub allowance: ::std::string::String,
    #[doc = "A list of method names that can be used. The access key only allows transactions with the function call of one of the given method names. Empty list means any method name can be used."]
    pub method_names: ::std::vec::Vec<::std::string::String>,
    #[doc = "The access key only allows transactions with the given receiver's account id."]
    pub receiver_id: ::std::string::String,
}
impl ::std::convert::From<&FunctionCallPermission> for FunctionCallPermission {
    fn from(value: &FunctionCallPermission) -> Self {
        value.clone()
    }
}
impl FunctionCallPermission {
    pub fn builder() -> builder::FunctionCallPermission {
        Default::default()
    }
}
#[doc = "GlobalContractData"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"code\","]
#[doc = "    \"id\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"code\": {"]
#[doc = "      \"type\": \"string\""]
#[doc = "    },"]
#[doc = "    \"id\": {"]
#[doc = "      \"$ref\": \"#/definitions/GlobalContractIdentifier\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct GlobalContractData {
    pub code: ::std::string::String,
    pub id: GlobalContractIdentifier,
}
impl ::std::convert::From<&GlobalContractData> for GlobalContractData {
    fn from(value: &GlobalContractData) -> Self {
        value.clone()
    }
}
impl GlobalContractData {
    pub fn builder() -> builder::GlobalContractData {
        Default::default()
    }
}
#[doc = "GlobalContractDeployMode"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"Contract is deployed under its code hash. Users will be able reference it by that hash. This effectively makes the contract immutable.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"CodeHash\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Contract is deployed under the owner account id. Users will be able reference it by that account id. This allows the owner to update the contract for all its users.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"AccountId\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub enum GlobalContractDeployMode {
    #[doc = "Contract is deployed under its code hash. Users will be able reference it by that hash. This effectively makes the contract immutable."]
    CodeHash,
    #[doc = "Contract is deployed under the owner account id. Users will be able reference it by that account id. This allows the owner to update the contract for all its users."]
    AccountId,
}
impl ::std::convert::From<&Self> for GlobalContractDeployMode {
    fn from(value: &GlobalContractDeployMode) -> Self {
        value.clone()
    }
}
impl ::std::fmt::Display for GlobalContractDeployMode {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match *self {
            Self::CodeHash => write!(f, "CodeHash"),
            Self::AccountId => write!(f, "AccountId"),
        }
    }
}
impl ::std::str::FromStr for GlobalContractDeployMode {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        match value {
            "CodeHash" => Ok(Self::CodeHash),
            "AccountId" => Ok(Self::AccountId),
            _ => Err("invalid value".into()),
        }
    }
}
impl ::std::convert::TryFrom<&str> for GlobalContractDeployMode {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<&::std::string::String> for GlobalContractDeployMode {
    type Error = self::error::ConversionError;
    fn try_from(
        value: &::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<::std::string::String> for GlobalContractDeployMode {
    type Error = self::error::ConversionError;
    fn try_from(
        value: ::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "GlobalContractIdentifier"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"CodeHash\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"CodeHash\": {"]
#[doc = "          \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AccountId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AccountId\": {"]
#[doc = "          \"$ref\": \"#/definitions/AccountId\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum GlobalContractIdentifier {
    CodeHash(CryptoHash),
    AccountId(AccountId),
}
impl ::std::convert::From<&Self> for GlobalContractIdentifier {
    fn from(value: &GlobalContractIdentifier) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<CryptoHash> for GlobalContractIdentifier {
    fn from(value: CryptoHash) -> Self {
        Self::CodeHash(value)
    }
}
impl ::std::convert::From<AccountId> for GlobalContractIdentifier {
    fn from(value: AccountId) -> Self {
        Self::AccountId(value)
    }
}
#[doc = "HostError"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"String encoding is bad UTF-16 sequence\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"BadUTF16\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"String encoding is bad UTF-8 sequence\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"BadUTF8\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Exceeded the prepaid gas\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"GasExceeded\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Exceeded the maximum amount of gas allowed to burn per contract\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"GasLimitExceeded\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Exceeded the account balance\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"BalanceExceeded\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Tried to call an empty method name\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"EmptyMethodName\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Smart contract panicked\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"GuestPanic\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"GuestPanic\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"panic_msg\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"panic_msg\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"IntegerOverflow happened during a contract execution\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"IntegerOverflow\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"`promise_idx` does not correspond to existing promises\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidPromiseIndex\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidPromiseIndex\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"promise_idx\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"promise_idx\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Actions can only be appended to non-joint promise.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"CannotAppendActionToJointPromise\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Returning joint promise is currently prohibited\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"CannotReturnJointPromise\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Accessed invalid promise result index\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidPromiseResultIndex\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidPromiseResultIndex\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"result_idx\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"result_idx\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Accessed invalid register id\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidRegisterId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidRegisterId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"register_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"register_id\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Iterator `iterator_index` was invalidated after its creation by performing a mutable operation on trie\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"IteratorWasInvalidated\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"IteratorWasInvalidated\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"iterator_index\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"iterator_index\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Accessed memory outside the bounds\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"MemoryAccessViolation\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"VM Logic returned an invalid receipt index\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidReceiptIndex\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidReceiptIndex\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"receipt_index\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"receipt_index\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Iterator index `iterator_index` does not exist\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidIteratorIndex\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidIteratorIndex\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"iterator_index\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"iterator_index\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"VM Logic returned an invalid account id\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"InvalidAccountId\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"VM Logic returned an invalid method name\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"InvalidMethodName\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"VM Logic provided an invalid public key\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"InvalidPublicKey\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"`method_name` is not allowed in view calls\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ProhibitedInView\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ProhibitedInView\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"method_name\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"method_name\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The total number of logs will exceed the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"NumberOfLogsExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"NumberOfLogsExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The storage key length exceeded the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"KeyLengthExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"KeyLengthExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"length\","]
#[doc = "            \"limit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"length\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The storage value length exceeded the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ValueLengthExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ValueLengthExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"length\","]
#[doc = "            \"limit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"length\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The total log length exceeded the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"TotalLogLengthExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"TotalLogLengthExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"length\","]
#[doc = "            \"limit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"length\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The maximum number of promises within a FunctionCall exceeded the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"NumberPromisesExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"NumberPromisesExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"number_of_promises\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"number_of_promises\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The maximum number of input data dependencies exceeded the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"NumberInputDataDependenciesExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"NumberInputDataDependenciesExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"number_of_input_data_dependencies\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"number_of_input_data_dependencies\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The returned value length exceeded the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ReturnedValueLengthExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ReturnedValueLengthExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"length\","]
#[doc = "            \"limit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"length\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The contract size for DeployContract action exceeded the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ContractSizeExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ContractSizeExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"size\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"size\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The host function was deprecated.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Deprecated\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Deprecated\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"method_name\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"method_name\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"General errors for ECDSA recover.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ECRecoverError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ECRecoverError\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"msg\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"msg\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Invalid input to alt_bn128 family of functions (e.g., point which isn't on the curve).\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AltBn128InvalidInput\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AltBn128InvalidInput\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"msg\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"msg\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Invalid input to ed25519 signature verification function (e.g. signature cannot be derived from bytes).\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Ed25519VerifyInvalidInput\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Ed25519VerifyInvalidInput\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"msg\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"msg\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum HostError {
    #[doc = "String encoding is bad UTF-16 sequence"]
    #[serde(rename = "BadUTF16")]
    BadUtf16,
    #[doc = "String encoding is bad UTF-8 sequence"]
    #[serde(rename = "BadUTF8")]
    BadUtf8,
    #[doc = "Exceeded the prepaid gas"]
    GasExceeded,
    #[doc = "Exceeded the maximum amount of gas allowed to burn per contract"]
    GasLimitExceeded,
    #[doc = "Exceeded the account balance"]
    BalanceExceeded,
    #[doc = "Tried to call an empty method name"]
    EmptyMethodName,
    #[doc = "Smart contract panicked"]
    GuestPanic { panic_msg: ::std::string::String },
    #[doc = "IntegerOverflow happened during a contract execution"]
    IntegerOverflow,
    #[doc = "`promise_idx` does not correspond to existing promises"]
    InvalidPromiseIndex { promise_idx: u64 },
    #[doc = "Actions can only be appended to non-joint promise."]
    CannotAppendActionToJointPromise,
    #[doc = "Returning joint promise is currently prohibited"]
    CannotReturnJointPromise,
    #[doc = "Accessed invalid promise result index"]
    InvalidPromiseResultIndex { result_idx: u64 },
    #[doc = "Accessed invalid register id"]
    InvalidRegisterId { register_id: u64 },
    #[doc = "Iterator `iterator_index` was invalidated after its creation by performing a mutable operation on trie"]
    IteratorWasInvalidated { iterator_index: u64 },
    #[doc = "Accessed memory outside the bounds"]
    MemoryAccessViolation,
    #[doc = "VM Logic returned an invalid receipt index"]
    InvalidReceiptIndex { receipt_index: u64 },
    #[doc = "Iterator index `iterator_index` does not exist"]
    InvalidIteratorIndex { iterator_index: u64 },
    #[doc = "VM Logic returned an invalid account id"]
    InvalidAccountId,
    #[doc = "VM Logic returned an invalid method name"]
    InvalidMethodName,
    #[doc = "VM Logic provided an invalid public key"]
    InvalidPublicKey,
    #[doc = "`method_name` is not allowed in view calls"]
    ProhibitedInView { method_name: ::std::string::String },
    #[doc = "The total number of logs will exceed the limit."]
    NumberOfLogsExceeded { limit: u64 },
    #[doc = "The storage key length exceeded the limit."]
    KeyLengthExceeded { length: u64, limit: u64 },
    #[doc = "The storage value length exceeded the limit."]
    ValueLengthExceeded { length: u64, limit: u64 },
    #[doc = "The total log length exceeded the limit."]
    TotalLogLengthExceeded { length: u64, limit: u64 },
    #[doc = "The maximum number of promises within a FunctionCall exceeded the limit."]
    NumberPromisesExceeded { limit: u64, number_of_promises: u64 },
    #[doc = "The maximum number of input data dependencies exceeded the limit."]
    NumberInputDataDependenciesExceeded {
        limit: u64,
        number_of_input_data_dependencies: u64,
    },
    #[doc = "The returned value length exceeded the limit."]
    ReturnedValueLengthExceeded { length: u64, limit: u64 },
    #[doc = "The contract size for DeployContract action exceeded the limit."]
    ContractSizeExceeded { limit: u64, size: u64 },
    #[doc = "The host function was deprecated."]
    Deprecated { method_name: ::std::string::String },
    #[doc = "General errors for ECDSA recover."]
    #[serde(rename = "ECRecoverError")]
    EcRecoverError { msg: ::std::string::String },
    #[doc = "Invalid input to alt_bn128 family of functions (e.g., point which isn't on the curve)."]
    AltBn128InvalidInput { msg: ::std::string::String },
    #[doc = "Invalid input to ed25519 signature verification function (e.g. signature cannot be derived from bytes)."]
    Ed25519VerifyInvalidInput { msg: ::std::string::String },
}
impl ::std::convert::From<&Self> for HostError {
    fn from(value: &HostError) -> Self {
        value.clone()
    }
}
#[doc = "InvalidAccessKeyError"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"The access key identified by the `public_key` doesn't exist for the account\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"AccessKeyNotFound\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"AccessKeyNotFound\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"public_key\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"public_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction `receiver_id` doesn't match the access key receiver_id\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ReceiverMismatch\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ReceiverMismatch\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"ak_receiver\","]
#[doc = "            \"tx_receiver\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"ak_receiver\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"tx_receiver\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction method name isn't allowed by the access key\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"MethodNameMismatch\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"MethodNameMismatch\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"method_name\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"method_name\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction requires a full permission access key.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"RequiresFullAccess\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Access Key does not have enough allowance to cover transaction cost\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"NotEnoughAllowance\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"NotEnoughAllowance\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\","]
#[doc = "            \"allowance\","]
#[doc = "            \"cost\","]
#[doc = "            \"public_key\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"allowance\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"cost\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"public_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Having a deposit with a function call action is not allowed with a function call access key.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"DepositWithFunctionCall\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum InvalidAccessKeyError {
    #[doc = "The access key identified by the `public_key` doesn't exist for the account"]
    AccessKeyNotFound {
        account_id: AccountId,
        public_key: PublicKey,
    },
    #[doc = "Transaction `receiver_id` doesn't match the access key receiver_id"]
    ReceiverMismatch {
        ak_receiver: ::std::string::String,
        tx_receiver: AccountId,
    },
    #[doc = "Transaction method name isn't allowed by the access key"]
    MethodNameMismatch { method_name: ::std::string::String },
    #[doc = "Transaction requires a full permission access key."]
    RequiresFullAccess,
    #[doc = "Access Key does not have enough allowance to cover transaction cost"]
    NotEnoughAllowance {
        account_id: AccountId,
        allowance: ::std::string::String,
        cost: ::std::string::String,
        public_key: PublicKey,
    },
    #[doc = "Having a deposit with a function call action is not allowed with a function call access key."]
    DepositWithFunctionCall,
}
impl ::std::convert::From<&Self> for InvalidAccessKeyError {
    fn from(value: &InvalidAccessKeyError) -> Self {
        value.clone()
    }
}
#[doc = "An error happened during TX execution"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"An error happened during TX execution\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"Happens if a wrong AccessKey used or AccessKey has not enough permissions\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidAccessKeyError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidAccessKeyError\": {"]
#[doc = "          \"$ref\": \"#/definitions/InvalidAccessKeyError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"TX signer_id is not a valid [`AccountId`]\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidSignerId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidSignerId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"signer_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"signer_id\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"TX signer_id is not found in a storage\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"SignerDoesNotExist\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"SignerDoesNotExist\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"signer_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"signer_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction nonce must be `account[access_key].nonce + 1`.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidNonce\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidNonce\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"ak_nonce\","]
#[doc = "            \"tx_nonce\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"ak_nonce\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"tx_nonce\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction nonce is larger than the upper bound given by the block height\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"NonceTooLarge\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"NonceTooLarge\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"tx_nonce\","]
#[doc = "            \"upper_bound\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"tx_nonce\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"upper_bound\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"TX receiver_id is not a valid AccountId\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidReceiverId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidReceiverId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"receiver_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"receiver_id\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"TX signature is not valid\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"InvalidSignature\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Account does not have enough balance to cover TX cost\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"NotEnoughBalance\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"NotEnoughBalance\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"balance\","]
#[doc = "            \"cost\","]
#[doc = "            \"signer_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"balance\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"cost\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"signer_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Signer account doesn't have enough balance after transaction.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"LackBalanceForState\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"LackBalanceForState\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"amount\","]
#[doc = "            \"signer_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"amount\": {"]
#[doc = "              \"description\": \"Required balance to cover the state.\","]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"signer_id\": {"]
#[doc = "              \"description\": \"An account which doesn't have enough balance to cover storage.\","]
#[doc = "              \"allOf\": ["]
#[doc = "                {"]
#[doc = "                  \"$ref\": \"#/definitions/AccountId\""]
#[doc = "                }"]
#[doc = "              ]"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"An integer overflow occurred during transaction cost estimation.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"CostOverflow\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction parent block hash doesn't belong to the current chain\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"InvalidChain\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction has expired\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"Expired\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"An error occurred while validating actions of a Transaction.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ActionsValidation\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ActionsValidation\": {"]
#[doc = "          \"$ref\": \"#/definitions/ActionsValidationError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The size of serialized transaction exceeded the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"TransactionSizeExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"TransactionSizeExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"size\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"size\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction version is invalid.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"InvalidTransactionVersion\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"StorageError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"StorageError\": {"]
#[doc = "          \"$ref\": \"#/definitions/StorageError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The receiver shard of the transaction is too congested to accept new transactions at the moment.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ShardCongested\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ShardCongested\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"congestion_level\","]
#[doc = "            \"shard_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"congestion_level\": {"]
#[doc = "              \"description\": \"A value between 0 (no congestion) and 1 (max congestion).\","]
#[doc = "              \"type\": \"number\","]
#[doc = "              \"format\": \"double\""]
#[doc = "            },"]
#[doc = "            \"shard_id\": {"]
#[doc = "              \"description\": \"The congested shard.\","]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint32\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The receiver shard of the transaction missed several chunks and rejects new transaction until it can make progress again.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ShardStuck\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ShardStuck\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"missed_chunks\","]
#[doc = "            \"shard_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"missed_chunks\": {"]
#[doc = "              \"description\": \"The number of blocks since the last included chunk of the shard.\","]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"shard_id\": {"]
#[doc = "              \"description\": \"The shard that fails making progress.\","]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint32\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum InvalidTxError {
    #[doc = "Happens if a wrong AccessKey used or AccessKey has not enough permissions"]
    InvalidAccessKeyError(InvalidAccessKeyError),
    #[doc = "TX signer_id is not a valid [`AccountId`]"]
    InvalidSignerId {
        signer_id: ::std::string::String,
    },
    #[doc = "TX signer_id is not found in a storage"]
    SignerDoesNotExist {
        signer_id: AccountId,
    },
    #[doc = "Transaction nonce must be `account[access_key].nonce + 1`."]
    InvalidNonce {
        ak_nonce: u64,
        tx_nonce: u64,
    },
    #[doc = "Transaction nonce is larger than the upper bound given by the block height"]
    NonceTooLarge {
        tx_nonce: u64,
        upper_bound: u64,
    },
    #[doc = "TX receiver_id is not a valid AccountId"]
    InvalidReceiverId {
        receiver_id: ::std::string::String,
    },
    #[doc = "TX signature is not valid"]
    InvalidSignature,
    #[doc = "Account does not have enough balance to cover TX cost"]
    NotEnoughBalance {
        balance: ::std::string::String,
        cost: ::std::string::String,
        signer_id: AccountId,
    },
    #[doc = "Signer account doesn't have enough balance after transaction."]
    LackBalanceForState {
        #[doc = "Required balance to cover the state."]
        amount: ::std::string::String,
        #[doc = "An account which doesn't have enough balance to cover storage."]
        signer_id: AccountId,
    },
    #[doc = "An integer overflow occurred during transaction cost estimation."]
    CostOverflow,
    #[doc = "Transaction parent block hash doesn't belong to the current chain"]
    InvalidChain,
    #[doc = "Transaction has expired"]
    Expired,
    #[doc = "An error occurred while validating actions of a Transaction."]
    ActionsValidation(ActionsValidationError),
    #[doc = "The size of serialized transaction exceeded the limit."]
    TransactionSizeExceeded {
        limit: u64,
        size: u64,
    },
    #[doc = "Transaction version is invalid."]
    InvalidTransactionVersion,
    StorageError(StorageError),
    #[doc = "The receiver shard of the transaction is too congested to accept new transactions at the moment."]
    ShardCongested {
        congestion_level: f64,
        #[doc = "The congested shard."]
        shard_id: u32,
    },
    #[doc = "The receiver shard of the transaction missed several chunks and rejects new transaction until it can make progress again."]
    ShardStuck {
        #[doc = "The number of blocks since the last included chunk of the shard."]
        missed_chunks: u64,
        #[doc = "The shard that fails making progress."]
        shard_id: u32,
    },
}
impl ::std::convert::From<&Self> for InvalidTxError {
    fn from(value: &InvalidTxError) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<InvalidAccessKeyError> for InvalidTxError {
    fn from(value: InvalidAccessKeyError) -> Self {
        Self::InvalidAccessKeyError(value)
    }
}
impl ::std::convert::From<ActionsValidationError> for InvalidTxError {
    fn from(value: ActionsValidationError) -> Self {
        Self::ActionsValidation(value)
    }
}
impl ::std::convert::From<StorageError> for InvalidTxError {
    fn from(value: StorageError) -> Self {
        Self::StorageError(value)
    }
}
#[doc = "MerklePathItem"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"direction\","]
#[doc = "    \"hash\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"direction\": {"]
#[doc = "      \"$ref\": \"#/definitions/Direction\""]
#[doc = "    },"]
#[doc = "    \"hash\": {"]
#[doc = "      \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct MerklePathItem {
    pub direction: Direction,
    pub hash: CryptoHash,
}
impl ::std::convert::From<&MerklePathItem> for MerklePathItem {
    fn from(value: &MerklePathItem) -> Self {
        value.clone()
    }
}
impl MerklePathItem {
    pub fn builder() -> builder::MerklePathItem {
        Default::default()
    }
}
#[doc = "MethodResolveError"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"string\","]
#[doc = "  \"enum\": ["]
#[doc = "    \"MethodEmptyName\","]
#[doc = "    \"MethodNotFound\","]
#[doc = "    \"MethodInvalidSignature\""]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub enum MethodResolveError {
    MethodEmptyName,
    MethodNotFound,
    MethodInvalidSignature,
}
impl ::std::convert::From<&Self> for MethodResolveError {
    fn from(value: &MethodResolveError) -> Self {
        value.clone()
    }
}
impl ::std::fmt::Display for MethodResolveError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match *self {
            Self::MethodEmptyName => write!(f, "MethodEmptyName"),
            Self::MethodNotFound => write!(f, "MethodNotFound"),
            Self::MethodInvalidSignature => write!(f, "MethodInvalidSignature"),
        }
    }
}
impl ::std::str::FromStr for MethodResolveError {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        match value {
            "MethodEmptyName" => Ok(Self::MethodEmptyName),
            "MethodNotFound" => Ok(Self::MethodNotFound),
            "MethodInvalidSignature" => Ok(Self::MethodInvalidSignature),
            _ => Err("invalid value".into()),
        }
    }
}
impl ::std::convert::TryFrom<&str> for MethodResolveError {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<&::std::string::String> for MethodResolveError {
    type Error = self::error::ConversionError;
    fn try_from(
        value: &::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<::std::string::String> for MethodResolveError {
    type Error = self::error::ConversionError;
    fn try_from(
        value: ::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "Contexts in which `StorageError::MissingTrieValue` error might occur."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Contexts in which `StorageError::MissingTrieValue` error might occur.\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"Missing trie value when reading from TrieIterator.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"TrieIterator\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Missing trie value when reading from TriePrefetchingStorage.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"TriePrefetchingStorage\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Missing trie value when reading from TrieMemoryPartialStorage.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"TrieMemoryPartialStorage\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Missing trie value when reading from TrieStorage.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"TrieStorage\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub enum MissingTrieValueContext {
    #[doc = "Missing trie value when reading from TrieIterator."]
    TrieIterator,
    #[doc = "Missing trie value when reading from TriePrefetchingStorage."]
    TriePrefetchingStorage,
    #[doc = "Missing trie value when reading from TrieMemoryPartialStorage."]
    TrieMemoryPartialStorage,
    #[doc = "Missing trie value when reading from TrieStorage."]
    TrieStorage,
}
impl ::std::convert::From<&Self> for MissingTrieValueContext {
    fn from(value: &MissingTrieValueContext) -> Self {
        value.clone()
    }
}
impl ::std::fmt::Display for MissingTrieValueContext {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match *self {
            Self::TrieIterator => write!(f, "TrieIterator"),
            Self::TriePrefetchingStorage => write!(f, "TriePrefetchingStorage"),
            Self::TrieMemoryPartialStorage => write!(f, "TrieMemoryPartialStorage"),
            Self::TrieStorage => write!(f, "TrieStorage"),
        }
    }
}
impl ::std::str::FromStr for MissingTrieValueContext {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        match value {
            "TrieIterator" => Ok(Self::TrieIterator),
            "TriePrefetchingStorage" => Ok(Self::TriePrefetchingStorage),
            "TrieMemoryPartialStorage" => Ok(Self::TrieMemoryPartialStorage),
            "TrieStorage" => Ok(Self::TrieStorage),
            _ => Err("invalid value".into()),
        }
    }
}
impl ::std::convert::TryFrom<&str> for MissingTrieValueContext {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<&::std::string::String> for MissingTrieValueContext {
    type Error = self::error::ConversionError;
    fn try_from(
        value: &::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<::std::string::String> for MissingTrieValueContext {
    type Error = self::error::ConversionError;
    fn try_from(
        value: ::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "This is Action which mustn't contain DelegateAction.\n\nThis struct is needed to avoid the recursion when Action/DelegateAction is deserialized.\n\nImportant: Don't make the inner Action public, this must only be constructed through the correct interface that ensures the inner Action is actually not a delegate action. That would break an assumption of this type, which we use in several places. For example, borsh de-/serialization relies on it. If the invariant is broken, we may end up with a `Transaction` or `Receipt` that we can serialize but deserializing it back causes a parsing error."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"This is Action which mustn't contain DelegateAction.\\n\\nThis struct is needed to avoid the recursion when Action/DelegateAction is deserialized.\\n\\nImportant: Don't make the inner Action public, this must only be constructed through the correct interface that ensures the inner Action is actually not a delegate action. That would break an assumption of this type, which we use in several places. For example, borsh de-/serialization relies on it. If the invariant is broken, we may end up with a `Transaction` or `Receipt` that we can serialize but deserializing it back causes a parsing error.\","]
#[doc = "  \"allOf\": ["]
#[doc = "    {"]
#[doc = "      \"$ref\": \"#/definitions/Action\""]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
#[serde(transparent)]
pub struct NonDelegateAction(pub Action);
impl ::std::ops::Deref for NonDelegateAction {
    type Target = Action;
    fn deref(&self) -> &Action {
        &self.0
    }
}
impl ::std::convert::From<NonDelegateAction> for Action {
    fn from(value: NonDelegateAction) -> Self {
        value.0
    }
}
impl ::std::convert::From<&NonDelegateAction> for NonDelegateAction {
    fn from(value: &NonDelegateAction) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<Action> for NonDelegateAction {
    fn from(value: Action) -> Self {
        Self(value)
    }
}
#[doc = "Error that can occur while preparing or executing Wasm smart-contract."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Error that can occur while preparing or executing Wasm smart-contract.\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"Error happened while serializing the module.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"Serialization\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Error happened while deserializing the module.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"Deserialization\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Internal memory declaration has been found in the module.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"InternalMemoryDeclared\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Gas instrumentation failed.\\n\\nThis most likely indicates the module isn't valid.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"GasInstrumentation\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Stack instrumentation failed.\\n\\nThis  most likely indicates the module isn't valid.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"StackHeightInstrumentation\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Error happened during instantiation.\\n\\nThis might indicate that `start` function trapped, or module isn't instantiable and/or un-linkable.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"Instantiate\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Error creating memory.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"Memory\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Contract contains too many functions.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"TooManyFunctions\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Contract contains too many locals.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"TooManyLocals\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub enum PrepareError {
    #[doc = "Error happened while serializing the module."]
    Serialization,
    #[doc = "Error happened while deserializing the module."]
    Deserialization,
    #[doc = "Internal memory declaration has been found in the module."]
    InternalMemoryDeclared,
    #[doc = "Gas instrumentation failed.\n\nThis most likely indicates the module isn't valid."]
    GasInstrumentation,
    #[doc = "Stack instrumentation failed.\n\nThis  most likely indicates the module isn't valid."]
    StackHeightInstrumentation,
    #[doc = "Error happened during instantiation.\n\nThis might indicate that `start` function trapped, or module isn't instantiable and/or un-linkable."]
    Instantiate,
    #[doc = "Error creating memory."]
    Memory,
    #[doc = "Contract contains too many functions."]
    TooManyFunctions,
    #[doc = "Contract contains too many locals."]
    TooManyLocals,
}
impl ::std::convert::From<&Self> for PrepareError {
    fn from(value: &PrepareError) -> Self {
        value.clone()
    }
}
impl ::std::fmt::Display for PrepareError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match *self {
            Self::Serialization => write!(f, "Serialization"),
            Self::Deserialization => write!(f, "Deserialization"),
            Self::InternalMemoryDeclared => write!(f, "InternalMemoryDeclared"),
            Self::GasInstrumentation => write!(f, "GasInstrumentation"),
            Self::StackHeightInstrumentation => write!(f, "StackHeightInstrumentation"),
            Self::Instantiate => write!(f, "Instantiate"),
            Self::Memory => write!(f, "Memory"),
            Self::TooManyFunctions => write!(f, "TooManyFunctions"),
            Self::TooManyLocals => write!(f, "TooManyLocals"),
        }
    }
}
impl ::std::str::FromStr for PrepareError {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        match value {
            "Serialization" => Ok(Self::Serialization),
            "Deserialization" => Ok(Self::Deserialization),
            "InternalMemoryDeclared" => Ok(Self::InternalMemoryDeclared),
            "GasInstrumentation" => Ok(Self::GasInstrumentation),
            "StackHeightInstrumentation" => Ok(Self::StackHeightInstrumentation),
            "Instantiate" => Ok(Self::Instantiate),
            "Memory" => Ok(Self::Memory),
            "TooManyFunctions" => Ok(Self::TooManyFunctions),
            "TooManyLocals" => Ok(Self::TooManyLocals),
            _ => Err("invalid value".into()),
        }
    }
}
impl ::std::convert::TryFrom<&str> for PrepareError {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<&::std::string::String> for PrepareError {
    type Error = self::error::ConversionError;
    fn try_from(
        value: &::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<::std::string::String> for PrepareError {
    type Error = self::error::ConversionError;
    fn try_from(
        value: ::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "PublicKey"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"string\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[serde(transparent)]
pub struct PublicKey(pub ::std::string::String);
impl ::std::ops::Deref for PublicKey {
    type Target = ::std::string::String;
    fn deref(&self) -> &::std::string::String {
        &self.0
    }
}
impl ::std::convert::From<PublicKey> for ::std::string::String {
    fn from(value: PublicKey) -> Self {
        value.0
    }
}
impl ::std::convert::From<&PublicKey> for PublicKey {
    fn from(value: &PublicKey) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<::std::string::String> for PublicKey {
    fn from(value: ::std::string::String) -> Self {
        Self(value)
    }
}
impl ::std::str::FromStr for PublicKey {
    type Err = ::std::convert::Infallible;
    fn from_str(value: &str) -> ::std::result::Result<Self, Self::Err> {
        Ok(Self(value.to_string()))
    }
}
impl ::std::fmt::Display for PublicKey {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        self.0.fmt(f)
    }
}
#[doc = "ReceiptEnumView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Action\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Action\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"actions\","]
#[doc = "            \"gas_price\","]
#[doc = "            \"input_data_ids\","]
#[doc = "            \"output_data_receivers\","]
#[doc = "            \"signer_id\","]
#[doc = "            \"signer_public_key\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"actions\": {"]
#[doc = "              \"type\": \"array\","]
#[doc = "              \"items\": {"]
#[doc = "                \"$ref\": \"#/definitions/ActionView\""]
#[doc = "              }"]
#[doc = "            },"]
#[doc = "            \"gas_price\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            },"]
#[doc = "            \"input_data_ids\": {"]
#[doc = "              \"type\": \"array\","]
#[doc = "              \"items\": {"]
#[doc = "                \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "              }"]
#[doc = "            },"]
#[doc = "            \"is_promise_yield\": {"]
#[doc = "              \"default\": false,"]
#[doc = "              \"type\": \"boolean\""]
#[doc = "            },"]
#[doc = "            \"output_data_receivers\": {"]
#[doc = "              \"type\": \"array\","]
#[doc = "              \"items\": {"]
#[doc = "                \"$ref\": \"#/definitions/DataReceiverView\""]
#[doc = "              }"]
#[doc = "            },"]
#[doc = "            \"signer_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/AccountId\""]
#[doc = "            },"]
#[doc = "            \"signer_public_key\": {"]
#[doc = "              \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"Data\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"Data\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"data_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"data\": {"]
#[doc = "              \"default\": null,"]
#[doc = "              \"type\": ["]
#[doc = "                \"string\","]
#[doc = "                \"null\""]
#[doc = "              ]"]
#[doc = "            },"]
#[doc = "            \"data_id\": {"]
#[doc = "              \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "            },"]
#[doc = "            \"is_promise_resume\": {"]
#[doc = "              \"default\": false,"]
#[doc = "              \"type\": \"boolean\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"GlobalContractDistribution\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"GlobalContractDistribution\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"data\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"data\": {"]
#[doc = "              \"$ref\": \"#/definitions/GlobalContractData\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum ReceiptEnumView {
    Action {
        actions: ::std::vec::Vec<ActionView>,
        gas_price: ::std::string::String,
        input_data_ids: ::std::vec::Vec<CryptoHash>,
        #[serde(default)]
        is_promise_yield: bool,
        output_data_receivers: ::std::vec::Vec<DataReceiverView>,
        signer_id: AccountId,
        signer_public_key: PublicKey,
    },
    Data {
        #[serde(default, skip_serializing_if = "::std::option::Option::is_none")]
        data: ::std::option::Option<::std::string::String>,
        data_id: CryptoHash,
        #[serde(default)]
        is_promise_resume: bool,
    },
    GlobalContractDistribution {
        data: GlobalContractData,
    },
}
impl ::std::convert::From<&Self> for ReceiptEnumView {
    fn from(value: &ReceiptEnumView) -> Self {
        value.clone()
    }
}
#[doc = "Describes the error for validating a receipt."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Describes the error for validating a receipt.\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"The `predecessor_id` of a Receipt is not valid.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidPredecessorId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidPredecessorId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The `receiver_id` of a Receipt is not valid.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidReceiverId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidReceiverId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The `signer_id` of an ActionReceipt is not valid.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidSignerId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidSignerId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The `receiver_id` of a DataReceiver within an ActionReceipt is not valid.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidDataReceiverId\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidDataReceiverId\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"account_id\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"account_id\": {"]
#[doc = "              \"type\": \"string\""]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The length of the returned data exceeded the limit in a DataReceipt.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ReturnedValueLengthExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ReturnedValueLengthExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"length\","]
#[doc = "            \"limit\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"length\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"The number of input data dependencies exceeds the limit in an ActionReceipt.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"NumberInputDataDependenciesExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"NumberInputDataDependenciesExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"number_of_input_data_dependencies\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"number_of_input_data_dependencies\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"An error occurred while validating actions of an ActionReceipt.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ActionsValidation\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ActionsValidation\": {"]
#[doc = "          \"$ref\": \"#/definitions/ActionsValidationError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Receipt is bigger than the limit.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ReceiptSizeExceeded\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ReceiptSizeExceeded\": {"]
#[doc = "          \"type\": \"object\","]
#[doc = "          \"required\": ["]
#[doc = "            \"limit\","]
#[doc = "            \"size\""]
#[doc = "          ],"]
#[doc = "          \"properties\": {"]
#[doc = "            \"limit\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            },"]
#[doc = "            \"size\": {"]
#[doc = "              \"type\": \"integer\","]
#[doc = "              \"format\": \"uint64\","]
#[doc = "              \"minimum\": 0.0"]
#[doc = "            }"]
#[doc = "          }"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum ReceiptValidationError {
    #[doc = "The `predecessor_id` of a Receipt is not valid."]
    InvalidPredecessorId { account_id: ::std::string::String },
    #[doc = "The `receiver_id` of a Receipt is not valid."]
    InvalidReceiverId { account_id: ::std::string::String },
    #[doc = "The `signer_id` of an ActionReceipt is not valid."]
    InvalidSignerId { account_id: ::std::string::String },
    #[doc = "The `receiver_id` of a DataReceiver within an ActionReceipt is not valid."]
    InvalidDataReceiverId { account_id: ::std::string::String },
    #[doc = "The length of the returned data exceeded the limit in a DataReceipt."]
    ReturnedValueLengthExceeded { length: u64, limit: u64 },
    #[doc = "The number of input data dependencies exceeds the limit in an ActionReceipt."]
    NumberInputDataDependenciesExceeded {
        limit: u64,
        number_of_input_data_dependencies: u64,
    },
    #[doc = "An error occurred while validating actions of an ActionReceipt."]
    ActionsValidation(ActionsValidationError),
    #[doc = "Receipt is bigger than the limit."]
    ReceiptSizeExceeded { limit: u64, size: u64 },
}
impl ::std::convert::From<&Self> for ReceiptValidationError {
    fn from(value: &ReceiptValidationError) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<ActionsValidationError> for ReceiptValidationError {
    fn from(value: ActionsValidationError) -> Self {
        Self::ActionsValidation(value)
    }
}
#[doc = "ReceiptView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"predecessor_id\","]
#[doc = "    \"receipt\","]
#[doc = "    \"receipt_id\","]
#[doc = "    \"receiver_id\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"predecessor_id\": {"]
#[doc = "      \"$ref\": \"#/definitions/AccountId\""]
#[doc = "    },"]
#[doc = "    \"priority\": {"]
#[doc = "      \"default\": 0,"]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"receipt\": {"]
#[doc = "      \"$ref\": \"#/definitions/ReceiptEnumView\""]
#[doc = "    },"]
#[doc = "    \"receipt_id\": {"]
#[doc = "      \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "    },"]
#[doc = "    \"receiver_id\": {"]
#[doc = "      \"$ref\": \"#/definitions/AccountId\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct ReceiptView {
    pub predecessor_id: AccountId,
    #[serde(default)]
    pub priority: u64,
    pub receipt: ReceiptEnumView,
    pub receipt_id: CryptoHash,
    pub receiver_id: AccountId,
}
impl ::std::convert::From<&ReceiptView> for ReceiptView {
    fn from(value: &ReceiptView) -> Self {
        value.clone()
    }
}
impl ReceiptView {
    pub fn builder() -> builder::ReceiptView {
        Default::default()
    }
}
#[doc = "RpcTransactionResponse"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"title\": \"RpcTransactionResponse\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"anyOf\": ["]
#[doc = "    {"]
#[doc = "      \"$ref\": \"#/definitions/FinalExecutionOutcomeWithReceiptView\""]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"$ref\": \"#/definitions/FinalExecutionOutcomeView\""]
#[doc = "    }"]
#[doc = "  ],"]
#[doc = "  \"required\": ["]
#[doc = "    \"final_execution_status\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"final_execution_status\": {"]
#[doc = "      \"$ref\": \"#/definitions/TxExecutionStatus\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
#[serde(untagged)]
pub enum RpcTransactionResponse {
    Variant0 {
        final_execution_status: TxExecutionStatus,
        #[doc = "Receipts generated from the transaction"]
        receipts: ::std::vec::Vec<ReceiptView>,
        #[doc = "The execution outcome of receipts."]
        receipts_outcome: ::std::vec::Vec<ExecutionOutcomeWithIdView>,
        #[doc = "Execution status defined by chain.rs:get_final_transaction_result FinalExecutionStatus::NotStarted - the tx is not converted to the receipt yet FinalExecutionStatus::Started - we have at least 1 receipt, but the first leaf receipt_id (using dfs) hasn't finished the execution FinalExecutionStatus::Failure - the result of the first leaf receipt_id FinalExecutionStatus::SuccessValue - the result of the first leaf receipt_id"]
        status: FinalExecutionStatus,
        #[doc = "Signed Transaction"]
        transaction: SignedTransactionView,
        #[doc = "The execution outcome of the signed transaction."]
        transaction_outcome: ExecutionOutcomeWithIdView,
    },
    Variant1 {
        final_execution_status: TxExecutionStatus,
        #[doc = "The execution outcome of receipts."]
        receipts_outcome: ::std::vec::Vec<ExecutionOutcomeWithIdView>,
        #[doc = "Execution status defined by chain.rs:get_final_transaction_result FinalExecutionStatus::NotStarted - the tx is not converted to the receipt yet FinalExecutionStatus::Started - we have at least 1 receipt, but the first leaf receipt_id (using dfs) hasn't finished the execution FinalExecutionStatus::Failure - the result of the first leaf receipt_id FinalExecutionStatus::SuccessValue - the result of the first leaf receipt_id"]
        status: FinalExecutionStatus,
        #[doc = "Signed Transaction"]
        transaction: SignedTransactionView,
        #[doc = "The execution outcome of the signed transaction."]
        transaction_outcome: ExecutionOutcomeWithIdView,
    },
}
impl ::std::convert::From<&Self> for RpcTransactionResponse {
    fn from(value: &RpcTransactionResponse) -> Self {
        value.clone()
    }
}
#[doc = "Signature"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"string\""]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
#[serde(transparent)]
pub struct Signature(pub ::std::string::String);
impl ::std::ops::Deref for Signature {
    type Target = ::std::string::String;
    fn deref(&self) -> &::std::string::String {
        &self.0
    }
}
impl ::std::convert::From<Signature> for ::std::string::String {
    fn from(value: Signature) -> Self {
        value.0
    }
}
impl ::std::convert::From<&Signature> for Signature {
    fn from(value: &Signature) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<::std::string::String> for Signature {
    fn from(value: ::std::string::String) -> Self {
        Self(value)
    }
}
impl ::std::str::FromStr for Signature {
    type Err = ::std::convert::Infallible;
    fn from_str(value: &str) -> ::std::result::Result<Self, Self::Err> {
        Ok(Self(value.to_string()))
    }
}
impl ::std::fmt::Display for Signature {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        self.0.fmt(f)
    }
}
#[doc = "SignedDelegateAction"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"delegate_action\","]
#[doc = "    \"signature\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"delegate_action\": {"]
#[doc = "      \"$ref\": \"#/definitions/DelegateAction\""]
#[doc = "    },"]
#[doc = "    \"signature\": {"]
#[doc = "      \"$ref\": \"#/definitions/Signature\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct SignedDelegateAction {
    pub delegate_action: DelegateAction,
    pub signature: Signature,
}
impl ::std::convert::From<&SignedDelegateAction> for SignedDelegateAction {
    fn from(value: &SignedDelegateAction) -> Self {
        value.clone()
    }
}
impl SignedDelegateAction {
    pub fn builder() -> builder::SignedDelegateAction {
        Default::default()
    }
}
#[doc = "SignedTransactionView"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"actions\","]
#[doc = "    \"hash\","]
#[doc = "    \"nonce\","]
#[doc = "    \"public_key\","]
#[doc = "    \"receiver_id\","]
#[doc = "    \"signature\","]
#[doc = "    \"signer_id\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"actions\": {"]
#[doc = "      \"type\": \"array\","]
#[doc = "      \"items\": {"]
#[doc = "        \"$ref\": \"#/definitions/ActionView\""]
#[doc = "      }"]
#[doc = "    },"]
#[doc = "    \"hash\": {"]
#[doc = "      \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "    },"]
#[doc = "    \"nonce\": {"]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"priority_fee\": {"]
#[doc = "      \"default\": 0,"]
#[doc = "      \"type\": \"integer\","]
#[doc = "      \"format\": \"uint64\","]
#[doc = "      \"minimum\": 0.0"]
#[doc = "    },"]
#[doc = "    \"public_key\": {"]
#[doc = "      \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "    },"]
#[doc = "    \"receiver_id\": {"]
#[doc = "      \"$ref\": \"#/definitions/AccountId\""]
#[doc = "    },"]
#[doc = "    \"signature\": {"]
#[doc = "      \"$ref\": \"#/definitions/Signature\""]
#[doc = "    },"]
#[doc = "    \"signer_id\": {"]
#[doc = "      \"$ref\": \"#/definitions/AccountId\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct SignedTransactionView {
    pub actions: ::std::vec::Vec<ActionView>,
    pub hash: CryptoHash,
    pub nonce: u64,
    #[serde(default)]
    pub priority_fee: u64,
    pub public_key: PublicKey,
    pub receiver_id: AccountId,
    pub signature: Signature,
    pub signer_id: AccountId,
}
impl ::std::convert::From<&SignedTransactionView> for SignedTransactionView {
    fn from(value: &SignedTransactionView) -> Self {
        value.clone()
    }
}
impl SignedTransactionView {
    pub fn builder() -> builder::SignedTransactionView {
        Default::default()
    }
}
#[doc = "An action which stakes signer_id tokens and setup's validator public key"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"An action which stakes signer_id tokens and setup's validator public key\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"public_key\","]
#[doc = "    \"stake\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"public_key\": {"]
#[doc = "      \"description\": \"Validator key which will be used to sign transactions on behalf of signer_id\","]
#[doc = "      \"allOf\": ["]
#[doc = "        {"]
#[doc = "          \"$ref\": \"#/definitions/PublicKey\""]
#[doc = "        }"]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    \"stake\": {"]
#[doc = "      \"description\": \"Amount of tokens to stake.\","]
#[doc = "      \"type\": \"string\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct StakeAction {
    #[doc = "Validator key which will be used to sign transactions on behalf of signer_id"]
    pub public_key: PublicKey,
    #[doc = "Amount of tokens to stake."]
    pub stake: ::std::string::String,
}
impl ::std::convert::From<&StakeAction> for StakeAction {
    fn from(value: &StakeAction) -> Self {
        value.clone()
    }
}
impl StakeAction {
    pub fn builder() -> builder::StakeAction {
        Default::default()
    }
}
#[doc = "Errors which may occur during working with trie storages, storing trie values (trie nodes and state values) by their hashes."]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Errors which may occur during working with trie storages, storing trie values (trie nodes and state values) by their hashes.\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"Key-value db internal failure\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"StorageInternalError\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Requested trie value by its hash which is missing in storage.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"MissingTrieValue\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"MissingTrieValue\": {"]
#[doc = "          \"type\": \"array\","]
#[doc = "          \"items\": ["]
#[doc = "            {"]
#[doc = "              \"$ref\": \"#/definitions/MissingTrieValueContext\""]
#[doc = "            },"]
#[doc = "            {"]
#[doc = "              \"$ref\": \"#/definitions/CryptoHash\""]
#[doc = "            }"]
#[doc = "          ],"]
#[doc = "          \"maxItems\": 2,"]
#[doc = "          \"minItems\": 2"]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Found trie node which shouldn't be part of state. Raised during validation of state sync parts where incorrect node was passed. TODO (#8997): consider including hash of trie node.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"UnexpectedTrieValue\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Either invalid state or key-value db is corrupted. For PartialStorage it cannot be corrupted. Error message is unreliable and for debugging purposes only. It's also probably ok to panic in every place that produces this error. We can check if db is corrupted by verifying everything in the state trie.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"StorageInconsistentState\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"StorageInconsistentState\": {"]
#[doc = "          \"type\": \"string\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Flat storage error, meaning that it doesn't support some block anymore. We guarantee that such block cannot become final, thus block processing must resume normally.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"FlatStorageBlockNotSupported\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"FlatStorageBlockNotSupported\": {"]
#[doc = "          \"type\": \"string\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"In-memory trie could not be loaded for some reason.\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"MemTrieLoadingError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"MemTrieLoadingError\": {"]
#[doc = "          \"type\": \"string\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Indicates that a resharding operation on flat storage is already in progress, when it wasn't expected to be so.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"FlatStorageReshardingAlreadyInProgress\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum StorageError {
    #[doc = "Key-value db internal failure"]
    StorageInternalError,
    #[doc = "Requested trie value by its hash which is missing in storage."]
    MissingTrieValue(MissingTrieValueContext, CryptoHash),
    #[doc = "Found trie node which shouldn't be part of state. Raised during validation of state sync parts where incorrect node was passed. TODO (#8997): consider including hash of trie node."]
    UnexpectedTrieValue,
    #[doc = "Either invalid state or key-value db is corrupted. For PartialStorage it cannot be corrupted. Error message is unreliable and for debugging purposes only. It's also probably ok to panic in every place that produces this error. We can check if db is corrupted by verifying everything in the state trie."]
    StorageInconsistentState(::std::string::String),
    #[doc = "Flat storage error, meaning that it doesn't support some block anymore. We guarantee that such block cannot become final, thus block processing must resume normally."]
    FlatStorageBlockNotSupported(::std::string::String),
    #[doc = "In-memory trie could not be loaded for some reason."]
    MemTrieLoadingError(::std::string::String),
    #[doc = "Indicates that a resharding operation on flat storage is already in progress, when it wasn't expected to be so."]
    FlatStorageReshardingAlreadyInProgress,
}
impl ::std::convert::From<&Self> for StorageError {
    fn from(value: &StorageError) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<(MissingTrieValueContext, CryptoHash)> for StorageError {
    fn from(value: (MissingTrieValueContext, CryptoHash)) -> Self {
        Self::MissingTrieValue(value.0, value.1)
    }
}
#[doc = "TransferAction"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"deposit\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"deposit\": {"]
#[doc = "      \"type\": \"string\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct TransferAction {
    pub deposit: ::std::string::String,
}
impl ::std::convert::From<&TransferAction> for TransferAction {
    fn from(value: &TransferAction) -> Self {
        value.clone()
    }
}
impl TransferAction {
    pub fn builder() -> builder::TransferAction {
        Default::default()
    }
}
#[doc = "Error returned in the ExecutionOutcome in case of failure"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Error returned in the ExecutionOutcome in case of failure\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"An error happened during Action execution\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"ActionError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"ActionError\": {"]
#[doc = "          \"$ref\": \"#/definitions/ActionError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"An error happened during Transaction execution\","]
#[doc = "      \"type\": \"object\","]
#[doc = "      \"required\": ["]
#[doc = "        \"InvalidTxError\""]
#[doc = "      ],"]
#[doc = "      \"properties\": {"]
#[doc = "        \"InvalidTxError\": {"]
#[doc = "          \"$ref\": \"#/definitions/InvalidTxError\""]
#[doc = "        }"]
#[doc = "      },"]
#[doc = "      \"additionalProperties\": false"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub enum TxExecutionError {
    #[doc = "An error happened during Action execution"]
    ActionError(ActionError),
    #[doc = "An error happened during Transaction execution"]
    InvalidTxError(InvalidTxError),
}
impl ::std::convert::From<&Self> for TxExecutionError {
    fn from(value: &TxExecutionError) -> Self {
        value.clone()
    }
}
impl ::std::convert::From<ActionError> for TxExecutionError {
    fn from(value: ActionError) -> Self {
        Self::ActionError(value)
    }
}
impl ::std::convert::From<InvalidTxError> for TxExecutionError {
    fn from(value: InvalidTxError) -> Self {
        Self::InvalidTxError(value)
    }
}
#[doc = "TxExecutionStatus"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction is waiting to be included into the block\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"NONE\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction is included into the block. The block may be not finalized yet\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"INCLUDED\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction is included into the block + All non-refund transaction receipts finished their execution. The corresponding blocks for tx and each receipt may be not finalized yet\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"EXECUTED_OPTIMISTIC\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction is included into finalized block\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"INCLUDED_FINAL\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction is included into finalized block + All non-refund transaction receipts finished their execution. The corresponding blocks for each receipt may be not finalized yet\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"EXECUTED\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Transaction is included into finalized block + Execution of all transaction receipts is finalized, including refund receipts\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"FINAL\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub enum TxExecutionStatus {
    #[doc = "Transaction is waiting to be included into the block"]
    #[serde(rename = "NONE")]
    None,
    #[doc = "Transaction is included into the block. The block may be not finalized yet"]
    #[serde(rename = "INCLUDED")]
    Included,
    #[doc = "Transaction is included into the block + All non-refund transaction receipts finished their execution. The corresponding blocks for tx and each receipt may be not finalized yet"]
    #[serde(rename = "EXECUTED_OPTIMISTIC")]
    ExecutedOptimistic,
    #[doc = "Transaction is included into finalized block"]
    #[serde(rename = "INCLUDED_FINAL")]
    IncludedFinal,
    #[doc = "Transaction is included into finalized block + All non-refund transaction receipts finished their execution. The corresponding blocks for each receipt may be not finalized yet"]
    #[serde(rename = "EXECUTED")]
    Executed,
    #[doc = "Transaction is included into finalized block + Execution of all transaction receipts is finalized, including refund receipts"]
    #[serde(rename = "FINAL")]
    Final,
}
impl ::std::convert::From<&Self> for TxExecutionStatus {
    fn from(value: &TxExecutionStatus) -> Self {
        value.clone()
    }
}
impl ::std::fmt::Display for TxExecutionStatus {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match *self {
            Self::None => write!(f, "NONE"),
            Self::Included => write!(f, "INCLUDED"),
            Self::ExecutedOptimistic => write!(f, "EXECUTED_OPTIMISTIC"),
            Self::IncludedFinal => write!(f, "INCLUDED_FINAL"),
            Self::Executed => write!(f, "EXECUTED"),
            Self::Final => write!(f, "FINAL"),
        }
    }
}
impl ::std::str::FromStr for TxExecutionStatus {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        match value {
            "NONE" => Ok(Self::None),
            "INCLUDED" => Ok(Self::Included),
            "EXECUTED_OPTIMISTIC" => Ok(Self::ExecutedOptimistic),
            "INCLUDED_FINAL" => Ok(Self::IncludedFinal),
            "EXECUTED" => Ok(Self::Executed),
            "FINAL" => Ok(Self::Final),
            _ => Err("invalid value".into()),
        }
    }
}
impl ::std::convert::TryFrom<&str> for TxExecutionStatus {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<&::std::string::String> for TxExecutionStatus {
    type Error = self::error::ConversionError;
    fn try_from(
        value: &::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<::std::string::String> for TxExecutionStatus {
    type Error = self::error::ConversionError;
    fn try_from(
        value: ::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = "Use global contract action"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"Use global contract action\","]
#[doc = "  \"type\": \"object\","]
#[doc = "  \"required\": ["]
#[doc = "    \"contract_identifier\""]
#[doc = "  ],"]
#[doc = "  \"properties\": {"]
#[doc = "    \"contract_identifier\": {"]
#[doc = "      \"$ref\": \"#/definitions/GlobalContractIdentifier\""]
#[doc = "    }"]
#[doc = "  }"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(:: serde :: Deserialize, :: serde :: Serialize, Clone, Debug)]
pub struct UseGlobalContractAction {
    pub contract_identifier: GlobalContractIdentifier,
}
impl ::std::convert::From<&UseGlobalContractAction> for UseGlobalContractAction {
    fn from(value: &UseGlobalContractAction) -> Self {
        value.clone()
    }
}
impl UseGlobalContractAction {
    pub fn builder() -> builder::UseGlobalContractAction {
        Default::default()
    }
}
#[doc = "A kind of a trap happened during execution of a binary"]
#[doc = r""]
#[doc = r" <details><summary>JSON schema</summary>"]
#[doc = r""]
#[doc = r" ```json"]
#[doc = "{"]
#[doc = "  \"description\": \"A kind of a trap happened during execution of a binary\","]
#[doc = "  \"oneOf\": ["]
#[doc = "    {"]
#[doc = "      \"description\": \"An `unreachable` opcode was executed.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"Unreachable\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Call indirect incorrect signature trap.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"IncorrectCallIndirectSignature\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Memory out of bounds trap.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"MemoryOutOfBounds\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Call indirect out of bounds trap.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"CallIndirectOOB\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"An arithmetic exception, e.g. divided by zero.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"IllegalArithmetic\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Misaligned atomic access trap.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"MisalignedAtomicAccess\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Indirect call to null.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"IndirectCallToNull\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Stack overflow.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"StackOverflow\""]
#[doc = "      ]"]
#[doc = "    },"]
#[doc = "    {"]
#[doc = "      \"description\": \"Generic trap.\","]
#[doc = "      \"type\": \"string\","]
#[doc = "      \"enum\": ["]
#[doc = "        \"GenericTrap\""]
#[doc = "      ]"]
#[doc = "    }"]
#[doc = "  ]"]
#[doc = "}"]
#[doc = r" ```"]
#[doc = r" </details>"]
#[derive(
    :: serde :: Deserialize,
    :: serde :: Serialize,
    Clone,
    Copy,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
)]
pub enum WasmTrap {
    #[doc = "An `unreachable` opcode was executed."]
    Unreachable,
    #[doc = "Call indirect incorrect signature trap."]
    IncorrectCallIndirectSignature,
    #[doc = "Memory out of bounds trap."]
    MemoryOutOfBounds,
    #[doc = "Call indirect out of bounds trap."]
    #[serde(rename = "CallIndirectOOB")]
    CallIndirectOob,
    #[doc = "An arithmetic exception, e.g. divided by zero."]
    IllegalArithmetic,
    #[doc = "Misaligned atomic access trap."]
    MisalignedAtomicAccess,
    #[doc = "Indirect call to null."]
    IndirectCallToNull,
    #[doc = "Stack overflow."]
    StackOverflow,
    #[doc = "Generic trap."]
    GenericTrap,
}
impl ::std::convert::From<&Self> for WasmTrap {
    fn from(value: &WasmTrap) -> Self {
        value.clone()
    }
}
impl ::std::fmt::Display for WasmTrap {
    fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
        match *self {
            Self::Unreachable => write!(f, "Unreachable"),
            Self::IncorrectCallIndirectSignature => write!(f, "IncorrectCallIndirectSignature"),
            Self::MemoryOutOfBounds => write!(f, "MemoryOutOfBounds"),
            Self::CallIndirectOob => write!(f, "CallIndirectOOB"),
            Self::IllegalArithmetic => write!(f, "IllegalArithmetic"),
            Self::MisalignedAtomicAccess => write!(f, "MisalignedAtomicAccess"),
            Self::IndirectCallToNull => write!(f, "IndirectCallToNull"),
            Self::StackOverflow => write!(f, "StackOverflow"),
            Self::GenericTrap => write!(f, "GenericTrap"),
        }
    }
}
impl ::std::str::FromStr for WasmTrap {
    type Err = self::error::ConversionError;
    fn from_str(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        match value {
            "Unreachable" => Ok(Self::Unreachable),
            "IncorrectCallIndirectSignature" => Ok(Self::IncorrectCallIndirectSignature),
            "MemoryOutOfBounds" => Ok(Self::MemoryOutOfBounds),
            "CallIndirectOOB" => Ok(Self::CallIndirectOob),
            "IllegalArithmetic" => Ok(Self::IllegalArithmetic),
            "MisalignedAtomicAccess" => Ok(Self::MisalignedAtomicAccess),
            "IndirectCallToNull" => Ok(Self::IndirectCallToNull),
            "StackOverflow" => Ok(Self::StackOverflow),
            "GenericTrap" => Ok(Self::GenericTrap),
            _ => Err("invalid value".into()),
        }
    }
}
impl ::std::convert::TryFrom<&str> for WasmTrap {
    type Error = self::error::ConversionError;
    fn try_from(value: &str) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<&::std::string::String> for WasmTrap {
    type Error = self::error::ConversionError;
    fn try_from(
        value: &::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
impl ::std::convert::TryFrom<::std::string::String> for WasmTrap {
    type Error = self::error::ConversionError;
    fn try_from(
        value: ::std::string::String,
    ) -> ::std::result::Result<Self, self::error::ConversionError> {
        value.parse()
    }
}
#[doc = r" Types for composing complex structures."]
pub mod builder {
    #[derive(Clone, Debug)]
    pub struct AccessKey {
        nonce: ::std::result::Result<u64, ::std::string::String>,
        permission: ::std::result::Result<super::AccessKeyPermission, ::std::string::String>,
    }
    impl ::std::default::Default for AccessKey {
        fn default() -> Self {
            Self {
                nonce: Err("no value supplied for nonce".to_string()),
                permission: Err("no value supplied for permission".to_string()),
            }
        }
    }
    impl AccessKey {
        pub fn nonce<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u64>,
            T::Error: ::std::fmt::Display,
        {
            self.nonce = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for nonce: {}", e));
            self
        }
        pub fn permission<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccessKeyPermission>,
            T::Error: ::std::fmt::Display,
        {
            self.permission = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for permission: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<AccessKey> for super::AccessKey {
        type Error = super::error::ConversionError;
        fn try_from(
            value: AccessKey,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                nonce: value.nonce?,
                permission: value.permission?,
            })
        }
    }
    impl ::std::convert::From<super::AccessKey> for AccessKey {
        fn from(value: super::AccessKey) -> Self {
            Self {
                nonce: Ok(value.nonce),
                permission: Ok(value.permission),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct AccessKeyView {
        nonce: ::std::result::Result<u64, ::std::string::String>,
        permission: ::std::result::Result<super::AccessKeyPermissionView, ::std::string::String>,
    }
    impl ::std::default::Default for AccessKeyView {
        fn default() -> Self {
            Self {
                nonce: Err("no value supplied for nonce".to_string()),
                permission: Err("no value supplied for permission".to_string()),
            }
        }
    }
    impl AccessKeyView {
        pub fn nonce<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u64>,
            T::Error: ::std::fmt::Display,
        {
            self.nonce = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for nonce: {}", e));
            self
        }
        pub fn permission<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccessKeyPermissionView>,
            T::Error: ::std::fmt::Display,
        {
            self.permission = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for permission: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<AccessKeyView> for super::AccessKeyView {
        type Error = super::error::ConversionError;
        fn try_from(
            value: AccessKeyView,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                nonce: value.nonce?,
                permission: value.permission?,
            })
        }
    }
    impl ::std::convert::From<super::AccessKeyView> for AccessKeyView {
        fn from(value: super::AccessKeyView) -> Self {
            Self {
                nonce: Ok(value.nonce),
                permission: Ok(value.permission),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct ActionError {
        index: ::std::result::Result<::std::option::Option<u64>, ::std::string::String>,
        kind: ::std::result::Result<super::ActionErrorKind, ::std::string::String>,
    }
    impl ::std::default::Default for ActionError {
        fn default() -> Self {
            Self {
                index: Ok(Default::default()),
                kind: Err("no value supplied for kind".to_string()),
            }
        }
    }
    impl ActionError {
        pub fn index<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::option::Option<u64>>,
            T::Error: ::std::fmt::Display,
        {
            self.index = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for index: {}", e));
            self
        }
        pub fn kind<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::ActionErrorKind>,
            T::Error: ::std::fmt::Display,
        {
            self.kind = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for kind: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<ActionError> for super::ActionError {
        type Error = super::error::ConversionError;
        fn try_from(
            value: ActionError,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                index: value.index?,
                kind: value.kind?,
            })
        }
    }
    impl ::std::convert::From<super::ActionError> for ActionError {
        fn from(value: super::ActionError) -> Self {
            Self {
                index: Ok(value.index),
                kind: Ok(value.kind),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct AddKeyAction {
        access_key: ::std::result::Result<super::AccessKey, ::std::string::String>,
        public_key: ::std::result::Result<super::PublicKey, ::std::string::String>,
    }
    impl ::std::default::Default for AddKeyAction {
        fn default() -> Self {
            Self {
                access_key: Err("no value supplied for access_key".to_string()),
                public_key: Err("no value supplied for public_key".to_string()),
            }
        }
    }
    impl AddKeyAction {
        pub fn access_key<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccessKey>,
            T::Error: ::std::fmt::Display,
        {
            self.access_key = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for access_key: {}", e));
            self
        }
        pub fn public_key<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::PublicKey>,
            T::Error: ::std::fmt::Display,
        {
            self.public_key = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for public_key: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<AddKeyAction> for super::AddKeyAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: AddKeyAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                access_key: value.access_key?,
                public_key: value.public_key?,
            })
        }
    }
    impl ::std::convert::From<super::AddKeyAction> for AddKeyAction {
        fn from(value: super::AddKeyAction) -> Self {
            Self {
                access_key: Ok(value.access_key),
                public_key: Ok(value.public_key),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct CostGasUsed {
        cost: ::std::result::Result<::std::string::String, ::std::string::String>,
        cost_category: ::std::result::Result<::std::string::String, ::std::string::String>,
        gas_used: ::std::result::Result<::std::string::String, ::std::string::String>,
    }
    impl ::std::default::Default for CostGasUsed {
        fn default() -> Self {
            Self {
                cost: Err("no value supplied for cost".to_string()),
                cost_category: Err("no value supplied for cost_category".to_string()),
                gas_used: Err("no value supplied for gas_used".to_string()),
            }
        }
    }
    impl CostGasUsed {
        pub fn cost<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.cost = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for cost: {}", e));
            self
        }
        pub fn cost_category<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.cost_category = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for cost_category: {}", e));
            self
        }
        pub fn gas_used<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.gas_used = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for gas_used: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<CostGasUsed> for super::CostGasUsed {
        type Error = super::error::ConversionError;
        fn try_from(
            value: CostGasUsed,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                cost: value.cost?,
                cost_category: value.cost_category?,
                gas_used: value.gas_used?,
            })
        }
    }
    impl ::std::convert::From<super::CostGasUsed> for CostGasUsed {
        fn from(value: super::CostGasUsed) -> Self {
            Self {
                cost: Ok(value.cost),
                cost_category: Ok(value.cost_category),
                gas_used: Ok(value.gas_used),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct DataReceiverView {
        data_id: ::std::result::Result<super::CryptoHash, ::std::string::String>,
        receiver_id: ::std::result::Result<super::AccountId, ::std::string::String>,
    }
    impl ::std::default::Default for DataReceiverView {
        fn default() -> Self {
            Self {
                data_id: Err("no value supplied for data_id".to_string()),
                receiver_id: Err("no value supplied for receiver_id".to_string()),
            }
        }
    }
    impl DataReceiverView {
        pub fn data_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::CryptoHash>,
            T::Error: ::std::fmt::Display,
        {
            self.data_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for data_id: {}", e));
            self
        }
        pub fn receiver_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccountId>,
            T::Error: ::std::fmt::Display,
        {
            self.receiver_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for receiver_id: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<DataReceiverView> for super::DataReceiverView {
        type Error = super::error::ConversionError;
        fn try_from(
            value: DataReceiverView,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                data_id: value.data_id?,
                receiver_id: value.receiver_id?,
            })
        }
    }
    impl ::std::convert::From<super::DataReceiverView> for DataReceiverView {
        fn from(value: super::DataReceiverView) -> Self {
            Self {
                data_id: Ok(value.data_id),
                receiver_id: Ok(value.receiver_id),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct DelegateAction {
        actions:
            ::std::result::Result<::std::vec::Vec<super::NonDelegateAction>, ::std::string::String>,
        max_block_height: ::std::result::Result<u64, ::std::string::String>,
        nonce: ::std::result::Result<u64, ::std::string::String>,
        public_key: ::std::result::Result<super::PublicKey, ::std::string::String>,
        receiver_id: ::std::result::Result<super::AccountId, ::std::string::String>,
        sender_id: ::std::result::Result<super::AccountId, ::std::string::String>,
    }
    impl ::std::default::Default for DelegateAction {
        fn default() -> Self {
            Self {
                actions: Err("no value supplied for actions".to_string()),
                max_block_height: Err("no value supplied for max_block_height".to_string()),
                nonce: Err("no value supplied for nonce".to_string()),
                public_key: Err("no value supplied for public_key".to_string()),
                receiver_id: Err("no value supplied for receiver_id".to_string()),
                sender_id: Err("no value supplied for sender_id".to_string()),
            }
        }
    }
    impl DelegateAction {
        pub fn actions<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::vec::Vec<super::NonDelegateAction>>,
            T::Error: ::std::fmt::Display,
        {
            self.actions = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for actions: {}", e));
            self
        }
        pub fn max_block_height<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u64>,
            T::Error: ::std::fmt::Display,
        {
            self.max_block_height = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for max_block_height: {}",
                    e
                )
            });
            self
        }
        pub fn nonce<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u64>,
            T::Error: ::std::fmt::Display,
        {
            self.nonce = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for nonce: {}", e));
            self
        }
        pub fn public_key<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::PublicKey>,
            T::Error: ::std::fmt::Display,
        {
            self.public_key = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for public_key: {}", e));
            self
        }
        pub fn receiver_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccountId>,
            T::Error: ::std::fmt::Display,
        {
            self.receiver_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for receiver_id: {}", e));
            self
        }
        pub fn sender_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccountId>,
            T::Error: ::std::fmt::Display,
        {
            self.sender_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for sender_id: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<DelegateAction> for super::DelegateAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: DelegateAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                actions: value.actions?,
                max_block_height: value.max_block_height?,
                nonce: value.nonce?,
                public_key: value.public_key?,
                receiver_id: value.receiver_id?,
                sender_id: value.sender_id?,
            })
        }
    }
    impl ::std::convert::From<super::DelegateAction> for DelegateAction {
        fn from(value: super::DelegateAction) -> Self {
            Self {
                actions: Ok(value.actions),
                max_block_height: Ok(value.max_block_height),
                nonce: Ok(value.nonce),
                public_key: Ok(value.public_key),
                receiver_id: Ok(value.receiver_id),
                sender_id: Ok(value.sender_id),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct DeleteAccountAction {
        beneficiary_id: ::std::result::Result<super::AccountId, ::std::string::String>,
    }
    impl ::std::default::Default for DeleteAccountAction {
        fn default() -> Self {
            Self {
                beneficiary_id: Err("no value supplied for beneficiary_id".to_string()),
            }
        }
    }
    impl DeleteAccountAction {
        pub fn beneficiary_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccountId>,
            T::Error: ::std::fmt::Display,
        {
            self.beneficiary_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for beneficiary_id: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<DeleteAccountAction> for super::DeleteAccountAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: DeleteAccountAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                beneficiary_id: value.beneficiary_id?,
            })
        }
    }
    impl ::std::convert::From<super::DeleteAccountAction> for DeleteAccountAction {
        fn from(value: super::DeleteAccountAction) -> Self {
            Self {
                beneficiary_id: Ok(value.beneficiary_id),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct DeleteKeyAction {
        public_key: ::std::result::Result<super::PublicKey, ::std::string::String>,
    }
    impl ::std::default::Default for DeleteKeyAction {
        fn default() -> Self {
            Self {
                public_key: Err("no value supplied for public_key".to_string()),
            }
        }
    }
    impl DeleteKeyAction {
        pub fn public_key<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::PublicKey>,
            T::Error: ::std::fmt::Display,
        {
            self.public_key = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for public_key: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<DeleteKeyAction> for super::DeleteKeyAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: DeleteKeyAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                public_key: value.public_key?,
            })
        }
    }
    impl ::std::convert::From<super::DeleteKeyAction> for DeleteKeyAction {
        fn from(value: super::DeleteKeyAction) -> Self {
            Self {
                public_key: Ok(value.public_key),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct DeployContractAction {
        code: ::std::result::Result<::std::string::String, ::std::string::String>,
    }
    impl ::std::default::Default for DeployContractAction {
        fn default() -> Self {
            Self {
                code: Err("no value supplied for code".to_string()),
            }
        }
    }
    impl DeployContractAction {
        pub fn code<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.code = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for code: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<DeployContractAction> for super::DeployContractAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: DeployContractAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self { code: value.code? })
        }
    }
    impl ::std::convert::From<super::DeployContractAction> for DeployContractAction {
        fn from(value: super::DeployContractAction) -> Self {
            Self {
                code: Ok(value.code),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct DeployGlobalContractAction {
        code: ::std::result::Result<::std::string::String, ::std::string::String>,
        deploy_mode: ::std::result::Result<super::GlobalContractDeployMode, ::std::string::String>,
    }
    impl ::std::default::Default for DeployGlobalContractAction {
        fn default() -> Self {
            Self {
                code: Err("no value supplied for code".to_string()),
                deploy_mode: Err("no value supplied for deploy_mode".to_string()),
            }
        }
    }
    impl DeployGlobalContractAction {
        pub fn code<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.code = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for code: {}", e));
            self
        }
        pub fn deploy_mode<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::GlobalContractDeployMode>,
            T::Error: ::std::fmt::Display,
        {
            self.deploy_mode = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for deploy_mode: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<DeployGlobalContractAction> for super::DeployGlobalContractAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: DeployGlobalContractAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                code: value.code?,
                deploy_mode: value.deploy_mode?,
            })
        }
    }
    impl ::std::convert::From<super::DeployGlobalContractAction> for DeployGlobalContractAction {
        fn from(value: super::DeployGlobalContractAction) -> Self {
            Self {
                code: Ok(value.code),
                deploy_mode: Ok(value.deploy_mode),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct ExecutionMetadataView {
        gas_profile: ::std::result::Result<
            ::std::option::Option<::std::vec::Vec<super::CostGasUsed>>,
            ::std::string::String,
        >,
        version: ::std::result::Result<u32, ::std::string::String>,
    }
    impl ::std::default::Default for ExecutionMetadataView {
        fn default() -> Self {
            Self {
                gas_profile: Ok(Default::default()),
                version: Err("no value supplied for version".to_string()),
            }
        }
    }
    impl ExecutionMetadataView {
        pub fn gas_profile<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::option::Option<::std::vec::Vec<super::CostGasUsed>>>,
            T::Error: ::std::fmt::Display,
        {
            self.gas_profile = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for gas_profile: {}", e));
            self
        }
        pub fn version<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u32>,
            T::Error: ::std::fmt::Display,
        {
            self.version = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for version: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<ExecutionMetadataView> for super::ExecutionMetadataView {
        type Error = super::error::ConversionError;
        fn try_from(
            value: ExecutionMetadataView,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                gas_profile: value.gas_profile?,
                version: value.version?,
            })
        }
    }
    impl ::std::convert::From<super::ExecutionMetadataView> for ExecutionMetadataView {
        fn from(value: super::ExecutionMetadataView) -> Self {
            Self {
                gas_profile: Ok(value.gas_profile),
                version: Ok(value.version),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct ExecutionOutcomeView {
        executor_id: ::std::result::Result<super::AccountId, ::std::string::String>,
        gas_burnt: ::std::result::Result<u64, ::std::string::String>,
        logs: ::std::result::Result<::std::vec::Vec<::std::string::String>, ::std::string::String>,
        metadata: ::std::result::Result<super::ExecutionMetadataView, ::std::string::String>,
        receipt_ids:
            ::std::result::Result<::std::vec::Vec<super::CryptoHash>, ::std::string::String>,
        status: ::std::result::Result<super::ExecutionStatusView, ::std::string::String>,
        tokens_burnt: ::std::result::Result<::std::string::String, ::std::string::String>,
    }
    impl ::std::default::Default for ExecutionOutcomeView {
        fn default() -> Self {
            Self {
                executor_id: Err("no value supplied for executor_id".to_string()),
                gas_burnt: Err("no value supplied for gas_burnt".to_string()),
                logs: Err("no value supplied for logs".to_string()),
                metadata: Ok(super::defaults::execution_outcome_view_metadata()),
                receipt_ids: Err("no value supplied for receipt_ids".to_string()),
                status: Err("no value supplied for status".to_string()),
                tokens_burnt: Err("no value supplied for tokens_burnt".to_string()),
            }
        }
    }
    impl ExecutionOutcomeView {
        pub fn executor_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccountId>,
            T::Error: ::std::fmt::Display,
        {
            self.executor_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for executor_id: {}", e));
            self
        }
        pub fn gas_burnt<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u64>,
            T::Error: ::std::fmt::Display,
        {
            self.gas_burnt = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for gas_burnt: {}", e));
            self
        }
        pub fn logs<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::vec::Vec<::std::string::String>>,
            T::Error: ::std::fmt::Display,
        {
            self.logs = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for logs: {}", e));
            self
        }
        pub fn metadata<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::ExecutionMetadataView>,
            T::Error: ::std::fmt::Display,
        {
            self.metadata = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for metadata: {}", e));
            self
        }
        pub fn receipt_ids<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::vec::Vec<super::CryptoHash>>,
            T::Error: ::std::fmt::Display,
        {
            self.receipt_ids = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for receipt_ids: {}", e));
            self
        }
        pub fn status<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::ExecutionStatusView>,
            T::Error: ::std::fmt::Display,
        {
            self.status = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for status: {}", e));
            self
        }
        pub fn tokens_burnt<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.tokens_burnt = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for tokens_burnt: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<ExecutionOutcomeView> for super::ExecutionOutcomeView {
        type Error = super::error::ConversionError;
        fn try_from(
            value: ExecutionOutcomeView,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                executor_id: value.executor_id?,
                gas_burnt: value.gas_burnt?,
                logs: value.logs?,
                metadata: value.metadata?,
                receipt_ids: value.receipt_ids?,
                status: value.status?,
                tokens_burnt: value.tokens_burnt?,
            })
        }
    }
    impl ::std::convert::From<super::ExecutionOutcomeView> for ExecutionOutcomeView {
        fn from(value: super::ExecutionOutcomeView) -> Self {
            Self {
                executor_id: Ok(value.executor_id),
                gas_burnt: Ok(value.gas_burnt),
                logs: Ok(value.logs),
                metadata: Ok(value.metadata),
                receipt_ids: Ok(value.receipt_ids),
                status: Ok(value.status),
                tokens_burnt: Ok(value.tokens_burnt),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct ExecutionOutcomeWithIdView {
        block_hash: ::std::result::Result<super::CryptoHash, ::std::string::String>,
        id: ::std::result::Result<super::CryptoHash, ::std::string::String>,
        outcome: ::std::result::Result<super::ExecutionOutcomeView, ::std::string::String>,
        proof: ::std::result::Result<::std::vec::Vec<super::MerklePathItem>, ::std::string::String>,
    }
    impl ::std::default::Default for ExecutionOutcomeWithIdView {
        fn default() -> Self {
            Self {
                block_hash: Err("no value supplied for block_hash".to_string()),
                id: Err("no value supplied for id".to_string()),
                outcome: Err("no value supplied for outcome".to_string()),
                proof: Err("no value supplied for proof".to_string()),
            }
        }
    }
    impl ExecutionOutcomeWithIdView {
        pub fn block_hash<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::CryptoHash>,
            T::Error: ::std::fmt::Display,
        {
            self.block_hash = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for block_hash: {}", e));
            self
        }
        pub fn id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::CryptoHash>,
            T::Error: ::std::fmt::Display,
        {
            self.id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for id: {}", e));
            self
        }
        pub fn outcome<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::ExecutionOutcomeView>,
            T::Error: ::std::fmt::Display,
        {
            self.outcome = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for outcome: {}", e));
            self
        }
        pub fn proof<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::vec::Vec<super::MerklePathItem>>,
            T::Error: ::std::fmt::Display,
        {
            self.proof = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for proof: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<ExecutionOutcomeWithIdView> for super::ExecutionOutcomeWithIdView {
        type Error = super::error::ConversionError;
        fn try_from(
            value: ExecutionOutcomeWithIdView,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                block_hash: value.block_hash?,
                id: value.id?,
                outcome: value.outcome?,
                proof: value.proof?,
            })
        }
    }
    impl ::std::convert::From<super::ExecutionOutcomeWithIdView> for ExecutionOutcomeWithIdView {
        fn from(value: super::ExecutionOutcomeWithIdView) -> Self {
            Self {
                block_hash: Ok(value.block_hash),
                id: Ok(value.id),
                outcome: Ok(value.outcome),
                proof: Ok(value.proof),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct FinalExecutionOutcomeView {
        receipts_outcome: ::std::result::Result<
            ::std::vec::Vec<super::ExecutionOutcomeWithIdView>,
            ::std::string::String,
        >,
        status: ::std::result::Result<super::FinalExecutionStatus, ::std::string::String>,
        transaction: ::std::result::Result<super::SignedTransactionView, ::std::string::String>,
        transaction_outcome:
            ::std::result::Result<super::ExecutionOutcomeWithIdView, ::std::string::String>,
    }
    impl ::std::default::Default for FinalExecutionOutcomeView {
        fn default() -> Self {
            Self {
                receipts_outcome: Err("no value supplied for receipts_outcome".to_string()),
                status: Err("no value supplied for status".to_string()),
                transaction: Err("no value supplied for transaction".to_string()),
                transaction_outcome: Err("no value supplied for transaction_outcome".to_string()),
            }
        }
    }
    impl FinalExecutionOutcomeView {
        pub fn receipts_outcome<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::vec::Vec<super::ExecutionOutcomeWithIdView>>,
            T::Error: ::std::fmt::Display,
        {
            self.receipts_outcome = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for receipts_outcome: {}",
                    e
                )
            });
            self
        }
        pub fn status<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::FinalExecutionStatus>,
            T::Error: ::std::fmt::Display,
        {
            self.status = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for status: {}", e));
            self
        }
        pub fn transaction<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::SignedTransactionView>,
            T::Error: ::std::fmt::Display,
        {
            self.transaction = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for transaction: {}", e));
            self
        }
        pub fn transaction_outcome<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::ExecutionOutcomeWithIdView>,
            T::Error: ::std::fmt::Display,
        {
            self.transaction_outcome = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for transaction_outcome: {}",
                    e
                )
            });
            self
        }
    }
    impl ::std::convert::TryFrom<FinalExecutionOutcomeView> for super::FinalExecutionOutcomeView {
        type Error = super::error::ConversionError;
        fn try_from(
            value: FinalExecutionOutcomeView,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                receipts_outcome: value.receipts_outcome?,
                status: value.status?,
                transaction: value.transaction?,
                transaction_outcome: value.transaction_outcome?,
            })
        }
    }
    impl ::std::convert::From<super::FinalExecutionOutcomeView> for FinalExecutionOutcomeView {
        fn from(value: super::FinalExecutionOutcomeView) -> Self {
            Self {
                receipts_outcome: Ok(value.receipts_outcome),
                status: Ok(value.status),
                transaction: Ok(value.transaction),
                transaction_outcome: Ok(value.transaction_outcome),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct FinalExecutionOutcomeWithReceiptView {
        receipts: ::std::result::Result<::std::vec::Vec<super::ReceiptView>, ::std::string::String>,
        receipts_outcome: ::std::result::Result<
            ::std::vec::Vec<super::ExecutionOutcomeWithIdView>,
            ::std::string::String,
        >,
        status: ::std::result::Result<super::FinalExecutionStatus, ::std::string::String>,
        transaction: ::std::result::Result<super::SignedTransactionView, ::std::string::String>,
        transaction_outcome:
            ::std::result::Result<super::ExecutionOutcomeWithIdView, ::std::string::String>,
    }
    impl ::std::default::Default for FinalExecutionOutcomeWithReceiptView {
        fn default() -> Self {
            Self {
                receipts: Err("no value supplied for receipts".to_string()),
                receipts_outcome: Err("no value supplied for receipts_outcome".to_string()),
                status: Err("no value supplied for status".to_string()),
                transaction: Err("no value supplied for transaction".to_string()),
                transaction_outcome: Err("no value supplied for transaction_outcome".to_string()),
            }
        }
    }
    impl FinalExecutionOutcomeWithReceiptView {
        pub fn receipts<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::vec::Vec<super::ReceiptView>>,
            T::Error: ::std::fmt::Display,
        {
            self.receipts = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for receipts: {}", e));
            self
        }
        pub fn receipts_outcome<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::vec::Vec<super::ExecutionOutcomeWithIdView>>,
            T::Error: ::std::fmt::Display,
        {
            self.receipts_outcome = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for receipts_outcome: {}",
                    e
                )
            });
            self
        }
        pub fn status<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::FinalExecutionStatus>,
            T::Error: ::std::fmt::Display,
        {
            self.status = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for status: {}", e));
            self
        }
        pub fn transaction<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::SignedTransactionView>,
            T::Error: ::std::fmt::Display,
        {
            self.transaction = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for transaction: {}", e));
            self
        }
        pub fn transaction_outcome<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::ExecutionOutcomeWithIdView>,
            T::Error: ::std::fmt::Display,
        {
            self.transaction_outcome = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for transaction_outcome: {}",
                    e
                )
            });
            self
        }
    }
    impl ::std::convert::TryFrom<FinalExecutionOutcomeWithReceiptView>
        for super::FinalExecutionOutcomeWithReceiptView
    {
        type Error = super::error::ConversionError;
        fn try_from(
            value: FinalExecutionOutcomeWithReceiptView,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                receipts: value.receipts?,
                receipts_outcome: value.receipts_outcome?,
                status: value.status?,
                transaction: value.transaction?,
                transaction_outcome: value.transaction_outcome?,
            })
        }
    }
    impl ::std::convert::From<super::FinalExecutionOutcomeWithReceiptView>
        for FinalExecutionOutcomeWithReceiptView
    {
        fn from(value: super::FinalExecutionOutcomeWithReceiptView) -> Self {
            Self {
                receipts: Ok(value.receipts),
                receipts_outcome: Ok(value.receipts_outcome),
                status: Ok(value.status),
                transaction: Ok(value.transaction),
                transaction_outcome: Ok(value.transaction_outcome),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct FunctionCallAction {
        args: ::std::result::Result<::std::string::String, ::std::string::String>,
        deposit: ::std::result::Result<::std::string::String, ::std::string::String>,
        gas: ::std::result::Result<u64, ::std::string::String>,
        method_name: ::std::result::Result<::std::string::String, ::std::string::String>,
    }
    impl ::std::default::Default for FunctionCallAction {
        fn default() -> Self {
            Self {
                args: Err("no value supplied for args".to_string()),
                deposit: Err("no value supplied for deposit".to_string()),
                gas: Err("no value supplied for gas".to_string()),
                method_name: Err("no value supplied for method_name".to_string()),
            }
        }
    }
    impl FunctionCallAction {
        pub fn args<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.args = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for args: {}", e));
            self
        }
        pub fn deposit<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.deposit = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for deposit: {}", e));
            self
        }
        pub fn gas<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u64>,
            T::Error: ::std::fmt::Display,
        {
            self.gas = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for gas: {}", e));
            self
        }
        pub fn method_name<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.method_name = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for method_name: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<FunctionCallAction> for super::FunctionCallAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: FunctionCallAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                args: value.args?,
                deposit: value.deposit?,
                gas: value.gas?,
                method_name: value.method_name?,
            })
        }
    }
    impl ::std::convert::From<super::FunctionCallAction> for FunctionCallAction {
        fn from(value: super::FunctionCallAction) -> Self {
            Self {
                args: Ok(value.args),
                deposit: Ok(value.deposit),
                gas: Ok(value.gas),
                method_name: Ok(value.method_name),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct FunctionCallPermission {
        allowance: ::std::result::Result<::std::string::String, ::std::string::String>,
        method_names:
            ::std::result::Result<::std::vec::Vec<::std::string::String>, ::std::string::String>,
        receiver_id: ::std::result::Result<::std::string::String, ::std::string::String>,
    }
    impl ::std::default::Default for FunctionCallPermission {
        fn default() -> Self {
            Self {
                allowance: Err("no value supplied for allowance".to_string()),
                method_names: Err("no value supplied for method_names".to_string()),
                receiver_id: Err("no value supplied for receiver_id".to_string()),
            }
        }
    }
    impl FunctionCallPermission {
        pub fn allowance<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.allowance = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for allowance: {}", e));
            self
        }
        pub fn method_names<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::vec::Vec<::std::string::String>>,
            T::Error: ::std::fmt::Display,
        {
            self.method_names = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for method_names: {}", e));
            self
        }
        pub fn receiver_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.receiver_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for receiver_id: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<FunctionCallPermission> for super::FunctionCallPermission {
        type Error = super::error::ConversionError;
        fn try_from(
            value: FunctionCallPermission,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                allowance: value.allowance?,
                method_names: value.method_names?,
                receiver_id: value.receiver_id?,
            })
        }
    }
    impl ::std::convert::From<super::FunctionCallPermission> for FunctionCallPermission {
        fn from(value: super::FunctionCallPermission) -> Self {
            Self {
                allowance: Ok(value.allowance),
                method_names: Ok(value.method_names),
                receiver_id: Ok(value.receiver_id),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct GlobalContractData {
        code: ::std::result::Result<::std::string::String, ::std::string::String>,
        id: ::std::result::Result<super::GlobalContractIdentifier, ::std::string::String>,
    }
    impl ::std::default::Default for GlobalContractData {
        fn default() -> Self {
            Self {
                code: Err("no value supplied for code".to_string()),
                id: Err("no value supplied for id".to_string()),
            }
        }
    }
    impl GlobalContractData {
        pub fn code<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.code = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for code: {}", e));
            self
        }
        pub fn id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::GlobalContractIdentifier>,
            T::Error: ::std::fmt::Display,
        {
            self.id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for id: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<GlobalContractData> for super::GlobalContractData {
        type Error = super::error::ConversionError;
        fn try_from(
            value: GlobalContractData,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                code: value.code?,
                id: value.id?,
            })
        }
    }
    impl ::std::convert::From<super::GlobalContractData> for GlobalContractData {
        fn from(value: super::GlobalContractData) -> Self {
            Self {
                code: Ok(value.code),
                id: Ok(value.id),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct MerklePathItem {
        direction: ::std::result::Result<super::Direction, ::std::string::String>,
        hash: ::std::result::Result<super::CryptoHash, ::std::string::String>,
    }
    impl ::std::default::Default for MerklePathItem {
        fn default() -> Self {
            Self {
                direction: Err("no value supplied for direction".to_string()),
                hash: Err("no value supplied for hash".to_string()),
            }
        }
    }
    impl MerklePathItem {
        pub fn direction<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::Direction>,
            T::Error: ::std::fmt::Display,
        {
            self.direction = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for direction: {}", e));
            self
        }
        pub fn hash<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::CryptoHash>,
            T::Error: ::std::fmt::Display,
        {
            self.hash = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for hash: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<MerklePathItem> for super::MerklePathItem {
        type Error = super::error::ConversionError;
        fn try_from(
            value: MerklePathItem,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                direction: value.direction?,
                hash: value.hash?,
            })
        }
    }
    impl ::std::convert::From<super::MerklePathItem> for MerklePathItem {
        fn from(value: super::MerklePathItem) -> Self {
            Self {
                direction: Ok(value.direction),
                hash: Ok(value.hash),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct ReceiptView {
        predecessor_id: ::std::result::Result<super::AccountId, ::std::string::String>,
        priority: ::std::result::Result<u64, ::std::string::String>,
        receipt: ::std::result::Result<super::ReceiptEnumView, ::std::string::String>,
        receipt_id: ::std::result::Result<super::CryptoHash, ::std::string::String>,
        receiver_id: ::std::result::Result<super::AccountId, ::std::string::String>,
    }
    impl ::std::default::Default for ReceiptView {
        fn default() -> Self {
            Self {
                predecessor_id: Err("no value supplied for predecessor_id".to_string()),
                priority: Ok(Default::default()),
                receipt: Err("no value supplied for receipt".to_string()),
                receipt_id: Err("no value supplied for receipt_id".to_string()),
                receiver_id: Err("no value supplied for receiver_id".to_string()),
            }
        }
    }
    impl ReceiptView {
        pub fn predecessor_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccountId>,
            T::Error: ::std::fmt::Display,
        {
            self.predecessor_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for predecessor_id: {}", e));
            self
        }
        pub fn priority<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u64>,
            T::Error: ::std::fmt::Display,
        {
            self.priority = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for priority: {}", e));
            self
        }
        pub fn receipt<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::ReceiptEnumView>,
            T::Error: ::std::fmt::Display,
        {
            self.receipt = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for receipt: {}", e));
            self
        }
        pub fn receipt_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::CryptoHash>,
            T::Error: ::std::fmt::Display,
        {
            self.receipt_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for receipt_id: {}", e));
            self
        }
        pub fn receiver_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccountId>,
            T::Error: ::std::fmt::Display,
        {
            self.receiver_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for receiver_id: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<ReceiptView> for super::ReceiptView {
        type Error = super::error::ConversionError;
        fn try_from(
            value: ReceiptView,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                predecessor_id: value.predecessor_id?,
                priority: value.priority?,
                receipt: value.receipt?,
                receipt_id: value.receipt_id?,
                receiver_id: value.receiver_id?,
            })
        }
    }
    impl ::std::convert::From<super::ReceiptView> for ReceiptView {
        fn from(value: super::ReceiptView) -> Self {
            Self {
                predecessor_id: Ok(value.predecessor_id),
                priority: Ok(value.priority),
                receipt: Ok(value.receipt),
                receipt_id: Ok(value.receipt_id),
                receiver_id: Ok(value.receiver_id),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct SignedDelegateAction {
        delegate_action: ::std::result::Result<super::DelegateAction, ::std::string::String>,
        signature: ::std::result::Result<super::Signature, ::std::string::String>,
    }
    impl ::std::default::Default for SignedDelegateAction {
        fn default() -> Self {
            Self {
                delegate_action: Err("no value supplied for delegate_action".to_string()),
                signature: Err("no value supplied for signature".to_string()),
            }
        }
    }
    impl SignedDelegateAction {
        pub fn delegate_action<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::DelegateAction>,
            T::Error: ::std::fmt::Display,
        {
            self.delegate_action = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for delegate_action: {}", e));
            self
        }
        pub fn signature<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::Signature>,
            T::Error: ::std::fmt::Display,
        {
            self.signature = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for signature: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<SignedDelegateAction> for super::SignedDelegateAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: SignedDelegateAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                delegate_action: value.delegate_action?,
                signature: value.signature?,
            })
        }
    }
    impl ::std::convert::From<super::SignedDelegateAction> for SignedDelegateAction {
        fn from(value: super::SignedDelegateAction) -> Self {
            Self {
                delegate_action: Ok(value.delegate_action),
                signature: Ok(value.signature),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct SignedTransactionView {
        actions: ::std::result::Result<::std::vec::Vec<super::ActionView>, ::std::string::String>,
        hash: ::std::result::Result<super::CryptoHash, ::std::string::String>,
        nonce: ::std::result::Result<u64, ::std::string::String>,
        priority_fee: ::std::result::Result<u64, ::std::string::String>,
        public_key: ::std::result::Result<super::PublicKey, ::std::string::String>,
        receiver_id: ::std::result::Result<super::AccountId, ::std::string::String>,
        signature: ::std::result::Result<super::Signature, ::std::string::String>,
        signer_id: ::std::result::Result<super::AccountId, ::std::string::String>,
    }
    impl ::std::default::Default for SignedTransactionView {
        fn default() -> Self {
            Self {
                actions: Err("no value supplied for actions".to_string()),
                hash: Err("no value supplied for hash".to_string()),
                nonce: Err("no value supplied for nonce".to_string()),
                priority_fee: Ok(Default::default()),
                public_key: Err("no value supplied for public_key".to_string()),
                receiver_id: Err("no value supplied for receiver_id".to_string()),
                signature: Err("no value supplied for signature".to_string()),
                signer_id: Err("no value supplied for signer_id".to_string()),
            }
        }
    }
    impl SignedTransactionView {
        pub fn actions<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::vec::Vec<super::ActionView>>,
            T::Error: ::std::fmt::Display,
        {
            self.actions = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for actions: {}", e));
            self
        }
        pub fn hash<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::CryptoHash>,
            T::Error: ::std::fmt::Display,
        {
            self.hash = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for hash: {}", e));
            self
        }
        pub fn nonce<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u64>,
            T::Error: ::std::fmt::Display,
        {
            self.nonce = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for nonce: {}", e));
            self
        }
        pub fn priority_fee<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<u64>,
            T::Error: ::std::fmt::Display,
        {
            self.priority_fee = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for priority_fee: {}", e));
            self
        }
        pub fn public_key<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::PublicKey>,
            T::Error: ::std::fmt::Display,
        {
            self.public_key = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for public_key: {}", e));
            self
        }
        pub fn receiver_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccountId>,
            T::Error: ::std::fmt::Display,
        {
            self.receiver_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for receiver_id: {}", e));
            self
        }
        pub fn signature<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::Signature>,
            T::Error: ::std::fmt::Display,
        {
            self.signature = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for signature: {}", e));
            self
        }
        pub fn signer_id<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::AccountId>,
            T::Error: ::std::fmt::Display,
        {
            self.signer_id = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for signer_id: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<SignedTransactionView> for super::SignedTransactionView {
        type Error = super::error::ConversionError;
        fn try_from(
            value: SignedTransactionView,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                actions: value.actions?,
                hash: value.hash?,
                nonce: value.nonce?,
                priority_fee: value.priority_fee?,
                public_key: value.public_key?,
                receiver_id: value.receiver_id?,
                signature: value.signature?,
                signer_id: value.signer_id?,
            })
        }
    }
    impl ::std::convert::From<super::SignedTransactionView> for SignedTransactionView {
        fn from(value: super::SignedTransactionView) -> Self {
            Self {
                actions: Ok(value.actions),
                hash: Ok(value.hash),
                nonce: Ok(value.nonce),
                priority_fee: Ok(value.priority_fee),
                public_key: Ok(value.public_key),
                receiver_id: Ok(value.receiver_id),
                signature: Ok(value.signature),
                signer_id: Ok(value.signer_id),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct StakeAction {
        public_key: ::std::result::Result<super::PublicKey, ::std::string::String>,
        stake: ::std::result::Result<::std::string::String, ::std::string::String>,
    }
    impl ::std::default::Default for StakeAction {
        fn default() -> Self {
            Self {
                public_key: Err("no value supplied for public_key".to_string()),
                stake: Err("no value supplied for stake".to_string()),
            }
        }
    }
    impl StakeAction {
        pub fn public_key<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::PublicKey>,
            T::Error: ::std::fmt::Display,
        {
            self.public_key = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for public_key: {}", e));
            self
        }
        pub fn stake<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.stake = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for stake: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<StakeAction> for super::StakeAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: StakeAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                public_key: value.public_key?,
                stake: value.stake?,
            })
        }
    }
    impl ::std::convert::From<super::StakeAction> for StakeAction {
        fn from(value: super::StakeAction) -> Self {
            Self {
                public_key: Ok(value.public_key),
                stake: Ok(value.stake),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct TransferAction {
        deposit: ::std::result::Result<::std::string::String, ::std::string::String>,
    }
    impl ::std::default::Default for TransferAction {
        fn default() -> Self {
            Self {
                deposit: Err("no value supplied for deposit".to_string()),
            }
        }
    }
    impl TransferAction {
        pub fn deposit<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<::std::string::String>,
            T::Error: ::std::fmt::Display,
        {
            self.deposit = value
                .try_into()
                .map_err(|e| format!("error converting supplied value for deposit: {}", e));
            self
        }
    }
    impl ::std::convert::TryFrom<TransferAction> for super::TransferAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: TransferAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                deposit: value.deposit?,
            })
        }
    }
    impl ::std::convert::From<super::TransferAction> for TransferAction {
        fn from(value: super::TransferAction) -> Self {
            Self {
                deposit: Ok(value.deposit),
            }
        }
    }
    #[derive(Clone, Debug)]
    pub struct UseGlobalContractAction {
        contract_identifier:
            ::std::result::Result<super::GlobalContractIdentifier, ::std::string::String>,
    }
    impl ::std::default::Default for UseGlobalContractAction {
        fn default() -> Self {
            Self {
                contract_identifier: Err("no value supplied for contract_identifier".to_string()),
            }
        }
    }
    impl UseGlobalContractAction {
        pub fn contract_identifier<T>(mut self, value: T) -> Self
        where
            T: ::std::convert::TryInto<super::GlobalContractIdentifier>,
            T::Error: ::std::fmt::Display,
        {
            self.contract_identifier = value.try_into().map_err(|e| {
                format!(
                    "error converting supplied value for contract_identifier: {}",
                    e
                )
            });
            self
        }
    }
    impl ::std::convert::TryFrom<UseGlobalContractAction> for super::UseGlobalContractAction {
        type Error = super::error::ConversionError;
        fn try_from(
            value: UseGlobalContractAction,
        ) -> ::std::result::Result<Self, super::error::ConversionError> {
            Ok(Self {
                contract_identifier: value.contract_identifier?,
            })
        }
    }
    impl ::std::convert::From<super::UseGlobalContractAction> for UseGlobalContractAction {
        fn from(value: super::UseGlobalContractAction) -> Self {
            Self {
                contract_identifier: Ok(value.contract_identifier),
            }
        }
    }
}
#[doc = r" Generation of default values for serde."]
pub mod defaults {
    pub(super) fn execution_outcome_view_metadata() -> super::ExecutionMetadataView {
        super::ExecutionMetadataView {
            gas_profile: ::std::option::Option::None,
            version: 1_u32,
        }
    }
}
