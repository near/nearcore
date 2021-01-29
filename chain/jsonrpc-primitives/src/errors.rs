use serde::{Deserialize, Serialize};
use serde_json::{to_value, Value};

#[derive(Serialize)]
pub struct RpcParseError(pub String);

/// This struct may be returned from JSON RPC server in case of error
/// It is expected that that this struct has impls From<_> all other RPC errors
/// like [RpcBlockError](crate::types::blocks::RpcBlockError)
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    /// A generic constructor.
    ///
    /// Mostly for completeness, doesn't do anything but filling in the corresponding fields.
    pub fn new(code: i64, message: String, data: Option<Value>) -> Self {
        RpcError { code, message, data }
    }
    /// Create an Invalid Param error.
    pub fn invalid_params(data: impl Serialize) -> Self {
        let value = match to_value(data) {
            Ok(value) => value,
            Err(err) => {
                return Self::server_error(Some(format!(
                    "Failed to serialize invalid parameters error: {:?}",
                    err.to_string()
                )))
            }
        };
        RpcError::new(-32_602, "Invalid params".to_owned(), Some(value))
    }
    /// Create a server error.
    pub fn server_error<E: Serialize>(e: Option<E>) -> Self {
        RpcError::new(
            -32_000,
            "Server error".to_owned(),
            e.map(|v| to_value(v).expect("Must be representable in JSON")),
        )
    }
    /// Create an invalid request error.
    pub fn invalid_request() -> Self {
        RpcError::new(-32_600, "Invalid request".to_owned(), None)
    }
    /// Create a parse error.
    pub fn parse_error(e: String) -> Self {
        RpcError::new(-32_700, "Parse error".to_owned(), Some(Value::String(e)))
    }
    /// Create a method not found error.
    pub fn method_not_found(method: String) -> Self {
        RpcError::new(-32_601, "Method not found".to_owned(), Some(Value::String(method)))
    }
}

impl std::string::ToString for RpcError {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

impl From<actix::MailboxError> for RpcError {
    fn from(error: actix::MailboxError) -> Self {
        Self::new(-32_000, "Server error".to_string(), Some(Value::String(error.to_string())))
    }
}

impl From<crate::errors::RpcParseError> for RpcError {
    fn from(parse_error: crate::errors::RpcParseError) -> Self {
        Self::invalid_params(parse_error.0)
    }
}
