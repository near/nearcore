use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{to_value, Value};

use near_primitives::errors::{InvalidTxError, TxExecutionError};

#[derive(Serialize)]
pub struct RpcParseError(pub String);

/// This struct may be returned from JSON RPC server in case of error
/// It is expected that that this struct has impls From<_> all other RPC errors
/// like [RpcBlockError](crate::types::blocks::RpcBlockError)
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RpcError {
    pub code: i64,
    #[serde(flatten)]
    pub error_struct: Option<RpcErrorKind>,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "name", content = "cause", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RpcErrorKind {
    ValidationError(ValidationErrorKind),
    HandlerError(Value),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "name", content = "info", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ValidationErrorKind {
    MethodNotFound { method_name: String },
    InvalidRequest,
    ParseError { error_message: String },
}

/// A general Server Error
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, near_rpc_error_macro::RpcError)]
pub enum ServerError {
    TxExecutionError(TxExecutionError),
    Timeout,
    Closed,
    InternalError,
}

impl RpcError {
    /// A generic constructor.
    ///
    /// Mostly for completeness, doesn't do anything but filling in the corresponding fields.
    pub fn new(code: i64, message: String, data: Option<Value>) -> Self {
        RpcError { code, message, data, error_struct: None }
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
        RpcError {
            code: -32_600,
            message: "Invalid request".to_owned(),
            data: None,
            error_struct: Some(RpcErrorKind::ValidationError(ValidationErrorKind::InvalidRequest)),
        }
    }
    /// Create a parse error.
    pub fn parse_error(e: String) -> Self {
        RpcError {
            code: -32_700,
            message: "Parse error".to_owned(),
            data: Some(Value::String(e.clone())),
            error_struct: Some(RpcErrorKind::ValidationError(ValidationErrorKind::ParseError {
                error_message: e,
            })),
        }
    }
    pub fn serialization_error(e: String) -> Self {
        // handler error
        RpcError {
            code: -32_000,
            message: "Server error".to_owned(),
            data: Some(Value::String(e.clone())),
            error_struct: Some(RpcErrorKind::HandlerError(serde_json::json!({
                "name": "SERIALIZATION_ERROR",
                "info": serde_json::json!({
                    "error_message": e
                })
            }))),
        }
    }
    pub fn new_handler_error(error_data: Option<Value>, error_struct: Value) -> Self {
        RpcError {
            code: -32_000,
            message: "Server error".to_owned(),
            data: error_data,
            error_struct: Some(RpcErrorKind::HandlerError(error_struct)),
        }
    }
    pub fn new_validation_error(
        error_data: Option<Value>,
        validatation_error_kind: ValidationErrorKind,
    ) -> Self {
        RpcError {
            code: -32_000,
            message: "Server error".to_owned(),
            data: error_data,
            error_struct: Some(RpcErrorKind::ValidationError(validatation_error_kind)),
        }
    }
    /// Create a method not found error.
    pub fn method_not_found(method: String) -> Self {
        RpcError {
            code: -32_601,
            message: "Method not found".to_owned(),
            data: Some(Value::String(method.clone())),
            error_struct: Some(RpcErrorKind::ValidationError(
                ValidationErrorKind::MethodNotFound { method_name: method },
            )),
        }
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

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerError::TxExecutionError(e) => write!(f, "ServerError: {}", e),
            ServerError::Timeout => write!(f, "ServerError: Timeout"),
            ServerError::Closed => write!(f, "ServerError: Closed"),
            ServerError::InternalError => write!(f, "ServerError: Internal Error"),
        }
    }
}

impl From<InvalidTxError> for ServerError {
    fn from(e: InvalidTxError) -> ServerError {
        ServerError::TxExecutionError(TxExecutionError::InvalidTxError(e))
    }
}

impl From<actix::MailboxError> for ServerError {
    fn from(e: actix::MailboxError) -> Self {
        match e {
            actix::MailboxError::Closed => ServerError::Closed,
            actix::MailboxError::Timeout => ServerError::Timeout,
        }
    }
}

impl From<ServerError> for RpcError {
    fn from(e: ServerError) -> RpcError {
        RpcError::server_error(Some(e))
    }
}
