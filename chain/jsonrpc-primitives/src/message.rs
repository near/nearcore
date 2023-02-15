// Copyright 2017 tokio-jsonrpc Developers
//
// Licensed under the Apache License, Version 2.0, <LICENSE-APACHE or
// http://apache.org/licenses/LICENSE-2.0> or the MIT license <LICENSE-MIT or
// http://opensource.org/licenses/MIT>, at your option. This file may not be
// copied, modified, or distributed except according to those terms.

//! JSON-RPC 2.0 messages.
//!
//! The main entrypoint here is the [Message](enum.Message.html). The others are just building
//! blocks and you should generally work with `Message` instead.
use crate::errors::RpcError;
use serde::de::{Deserializer, Error, Unexpected, Visitor};
use serde::ser::{SerializeStruct, Serializer};
use serde_json::{Result as JsonResult, Value};
use std::fmt::{Formatter, Result as FmtResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Version;

impl serde::Serialize for Version {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str("2.0")
    }
}

impl<'de> serde::Deserialize<'de> for Version {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct VersionVisitor;
        impl<'de> Visitor<'de> for VersionVisitor {
            type Value = Version;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> FmtResult {
                formatter.write_str("a version string")
            }

            fn visit_str<E: Error>(self, value: &str) -> Result<Version, E> {
                match value {
                    "2.0" => Ok(Version),
                    _ => Err(E::invalid_value(Unexpected::Str(value), &"value 2.0")),
                }
            }
        }
        deserializer.deserialize_str(VersionVisitor)
    }
}

/// An RPC request.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Request {
    jsonrpc: Version,
    pub method: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub params: Value,
    pub id: Value,
}

impl Request {
    /// Answer the request with a (positive) reply.
    ///
    /// The ID is taken from the request.
    pub fn reply(&self, reply: Value) -> Message {
        Message::Response(Response { jsonrpc: Version, result: Ok(reply), id: self.id.clone() })
    }
    /// Answer the request with an error.
    pub fn error(&self, error: RpcError) -> Message {
        Message::Response(Response { jsonrpc: Version, result: Err(error), id: self.id.clone() })
    }
}

/// A response to an RPC.
///
/// It is created by the methods on [Request](struct.Request.html).
#[derive(Debug, Clone, PartialEq)]
pub struct Response {
    jsonrpc: Version,
    pub result: Result<Value, RpcError>,
    pub id: Value,
}

impl serde::Serialize for Response {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sub = serializer.serialize_struct("Response", 3)?;
        sub.serialize_field("jsonrpc", &self.jsonrpc)?;
        match self.result {
            Ok(ref value) => sub.serialize_field("result", value),
            Err(ref err) => sub.serialize_field("error", err),
        }?;
        sub.serialize_field("id", &self.id)?;
        sub.end()
    }
}

/// Deserializer for `Option<Value>` that produces `Some(Value::Null)`.
///
/// The usual one produces None in that case. But we need to know the difference between
/// `{x: null}` and `{}`.
fn some_value<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    serde::Deserialize::deserialize(deserializer).map(Some)
}

/// A helper trick for deserialization.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WireResponse {
    // It is actually used to eat and sanity check the deserialized text
    #[allow(dead_code)]
    jsonrpc: Version,
    // Make sure we accept null as Some(Value::Null), instead of going to None
    #[serde(default, deserialize_with = "some_value")]
    result: Option<Value>,
    error: Option<RpcError>,
    id: Value,
}

// Implementing deserialize is hard. We sidestep the difficulty by deserializing a similar
// structure that directly corresponds to whatever is on the wire and then convert it to our more
// convenient representation.
impl<'de> serde::Deserialize<'de> for Response {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wr: WireResponse = serde::Deserialize::deserialize(deserializer)?;
        let result = match (wr.result, wr.error) {
            (Some(res), None) => Ok(res),
            (None, Some(err)) => Err(err),
            _ => {
                let err = D::Error::custom("Either 'error' or 'result' is expected, but not both");
                return Err(err);
            }
        };
        Ok(Response { jsonrpc: Version, result, id: wr.id })
    }
}

/// A notification (doesn't expect an answer).
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Notification {
    jsonrpc: Version,
    pub method: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub params: Value,
}

/// One message of the JSON RPC protocol.
///
/// One message, directly mapped from the structures of the protocol. See the
/// [specification](http://www.jsonrpc.org/specification) for more details.
///
/// Since the protocol allows one endpoint to be both client and server at the same time, the
/// message can decode and encode both directions of the protocol.
///
/// The `Batch` variant is supposed to be created directly, without a constructor.
///
/// The `UnmatchedSub` variant is used when a request is an array and some of the subrequests
/// aren't recognized as valid json rpc 2.0 messages. This is never returned as a top-level
/// element, it is returned as `Err(Broken::Unmatched)`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Message {
    /// An RPC request.
    Request(Request),
    /// A response to a Request.
    Response(Response),
    /// A notification.
    Notification(Notification),
    /// A batch of more requests or responses.
    ///
    /// The protocol allows bundling multiple requests, notifications or responses to a single
    /// message.
    ///
    /// This variant has no direct constructor and is expected to be constructed manually.
    Batch(Vec<Message>),
    /// An unmatched sub entry in a `Batch`.
    ///
    /// When there's a `Batch` and an element doesn't comform to the JSONRPC 2.0 format, that one
    /// is represented by this. This is never produced as a top-level value when parsing, the
    /// `Err(Broken::Unmatched)` is used instead. It is not possible to serialize.
    #[serde(skip_serializing)]
    UnmatchedSub(Value),
}

impl Message {
    /// A constructor for a request.
    ///
    /// The ID is auto-generated.
    pub fn request(method: String, params: Value) -> Self {
        let id = Value::from(near_primitives::utils::generate_random_string(9));
        Message::Request(Request { jsonrpc: Version, method, params, id })
    }
    /// Create a top-level error (without an ID).
    pub fn error(error: RpcError) -> Self {
        Message::Response(Response { jsonrpc: Version, result: Err(error), id: Value::Null })
    }
    /// A constructor for a notification.
    pub fn notification(method: String, params: Value) -> Self {
        Message::Notification(Notification { jsonrpc: Version, method, params })
    }
    /// A constructor for a response.
    pub fn response(id: Value, result: Result<Value, RpcError>) -> Self {
        Message::Response(Response { jsonrpc: Version, result, id })
    }
    /// Returns id or Null if there is no id.
    pub fn id(&self) -> Value {
        match self {
            Message::Request(req) => req.id.clone(),
            _ => Value::Null,
        }
    }
}

/// A broken message.
///
/// Protocol-level errors.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(untagged)]
pub enum Broken {
    /// It was valid JSON, but doesn't match the form of a JSONRPC 2.0 message.
    Unmatched(Value),
    /// Invalid JSON.
    #[serde(skip_deserializing)]
    SyntaxError(String),
}

impl Broken {
    /// Generate an appropriate error message.
    ///
    /// The error message for these things are specified in the RFC, so this just creates an error
    /// with the right values.
    pub fn reply(&self) -> Message {
        match *self {
            Broken::Unmatched(_) => Message::error(RpcError::parse_error(
                "JSON RPC Request format was expected".to_owned(),
            )),
            Broken::SyntaxError(ref e) => Message::error(RpcError::parse_error(e.clone())),
        }
    }
}

/// A trick to easily deserialize and detect valid JSON, but invalid Message.
#[derive(serde::Deserialize)]
#[serde(untagged)]
pub enum WireMessage {
    Message(Message),
    Broken(Broken),
}

pub fn decoded_to_parsed(res: JsonResult<WireMessage>) -> Parsed {
    match res {
        Ok(WireMessage::Message(Message::UnmatchedSub(value))) => Err(Broken::Unmatched(value)),
        Ok(WireMessage::Message(m)) => Ok(m),
        Ok(WireMessage::Broken(b)) => Err(b),
        Err(e) => Err(Broken::SyntaxError(e.to_string())),
    }
}

pub type Parsed = Result<Message, Broken>;

/// Read a [Message](enum.Message.html) from a slice.
///
/// Invalid JSON or JSONRPC messages are reported as [Broken](enum.Broken.html).
pub fn from_slice(s: &[u8]) -> Parsed {
    decoded_to_parsed(::serde_json::de::from_slice(s))
}

/// Read a [Message](enum.Message.html) from a string.
///
/// Invalid JSON or JSONRPC messages are reported as [Broken](enum.Broken.html).
pub fn from_str(s: &str) -> Parsed {
    from_slice(s.as_bytes())
}

impl Into<String> for Message {
    fn into(self) -> String {
        ::serde_json::ser::to_string(&self).unwrap()
    }
}

impl Into<Vec<u8>> for Message {
    fn into(self) -> Vec<u8> {
        ::serde_json::ser::to_vec(&self).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::de::from_slice;
    use serde_json::json;
    use serde_json::ser::to_vec;
    use serde_json::Value;

    use super::*;

    /// Test serialization and deserialization of the Message
    ///
    /// We first deserialize it from a string. That way we check deserialization works.
    /// But since serialization doesn't have to produce the exact same result (order, spaces, …),
    /// we then serialize and deserialize the thing again and check it matches.
    #[test]
    fn message_serde() {
        // A helper for running one message test
        fn one(input: &str, expected: &Message) {
            let parsed: Message = from_str(input).unwrap();
            assert_eq!(*expected, parsed);
            let serialized = to_vec(&parsed).unwrap();
            let deserialized: Message = from_slice(&serialized).unwrap();
            assert_eq!(parsed, deserialized);
        }

        // A request without parameters
        one(
            r#"{"jsonrpc": "2.0", "method": "call", "id": 1}"#,
            &Message::Request(Request {
                jsonrpc: Version,
                method: "call".to_owned(),
                params: Value::Null,
                id: json!(1),
            }),
        );
        // A request with parameters
        one(
            r#"{"jsonrpc": "2.0", "method": "call", "params": [1, 2, 3], "id": 2}"#,
            &Message::Request(Request {
                jsonrpc: Version,
                method: "call".to_owned(),
                params: json!([1, 2, 3]),
                id: json!(2),
            }),
        );
        // A notification (with parameters)
        one(
            r#"{"jsonrpc": "2.0", "method": "notif", "params": {"x": "y"}}"#,
            &Message::Notification(Notification {
                jsonrpc: Version,
                method: "notif".to_owned(),
                params: json!({"x": "y"}),
            }),
        );
        // A successful response
        one(
            r#"{"jsonrpc": "2.0", "result": 42, "id": 3}"#,
            &Message::Response(Response { jsonrpc: Version, result: Ok(json!(42)), id: json!(3) }),
        );
        // A successful response
        one(
            r#"{"jsonrpc": "2.0", "result": null, "id": 3}"#,
            &Message::Response(Response {
                jsonrpc: Version,
                result: Ok(Value::Null),
                id: json!(3),
            }),
        );
        // An error
        one(
            r#"{"jsonrpc": "2.0", "error": {"code": 42, "message": "Wrong!"}, "id": null}"#,
            &Message::Response(Response {
                jsonrpc: Version,
                result: Err(RpcError::new(42, "Wrong!".to_owned(), None)),
                id: Value::Null,
            }),
        );
        // A batch
        one(
            r#"[
                {"jsonrpc": "2.0", "method": "notif"},
                {"jsonrpc": "2.0", "method": "call", "id": 42}
            ]"#,
            &Message::Batch(vec![
                Message::Notification(Notification {
                    jsonrpc: Version,
                    method: "notif".to_owned(),
                    params: Value::Null,
                }),
                Message::Request(Request {
                    jsonrpc: Version,
                    method: "call".to_owned(),
                    params: Value::Null,
                    id: json!(42),
                }),
            ]),
        );
        // Some handling of broken messages inside a batch
        let parsed = from_str(
            r#"[
                {"jsonrpc": "2.0", "method": "notif"},
                {"jsonrpc": "2.0", "method": "call", "id": 42},
                true
            ]"#,
        )
        .unwrap();
        assert_eq!(
            Message::Batch(vec![
                Message::Notification(Notification {
                    jsonrpc: Version,
                    method: "notif".to_owned(),
                    params: Value::Null,
                }),
                Message::Request(Request {
                    jsonrpc: Version,
                    method: "call".to_owned(),
                    params: Value::Null,
                    id: json!(42),
                }),
                Message::UnmatchedSub(Value::Bool(true)),
            ]),
            parsed
        );
        to_vec(&Message::UnmatchedSub(Value::Null)).unwrap_err();
    }

    /// A helper for the `broken` test.
    ///
    /// Check that the given JSON string parses, but is not recognized as a valid RPC message.

    /// Test things that are almost but not entirely JSONRPC are rejected
    ///
    /// The reject is done by returning it as Unmatched.
    #[test]
    fn broken() {
        // A helper with one test
        fn one(input: &str) {
            let msg = from_str(input);
            match msg {
                Err(Broken::Unmatched(_)) => (),
                _ => panic!("{} recognized as an RPC message: {:?}!", input, msg),
            }
        }

        // Missing the version
        one(r#"{"method": "notif"}"#);
        // Wrong version
        one(r#"{"jsonrpc": 2.0, "method": "notif"}"#);
        // A response with both result and error
        one(r#"{"jsonrpc": "2.0", "result": 42, "error": {"code": 42, "message": "!"}, "id": 1}"#);
        // A response without an id
        one(r#"{"jsonrpc": "2.0", "result": 42}"#);
        // An extra field
        one(r#"{"jsonrpc": "2.0", "method": "weird", "params": 42, "others": 43, "id": 2}"#);
        // Something completely different
        one(r#"{"x": [1, 2, 3]}"#);

        match from_str(r#"{]"#) {
            Err(Broken::SyntaxError(_)) => (),
            other => panic!("Something unexpected: {:?}", other),
        };
    }

    /// Test some non-trivial aspects of the constructors
    ///
    /// This doesn't have a full coverage, because there's not much to actually test there.
    /// Most of it is related to the ids.
    #[test]
    fn constructors() {
        let msg1 = Message::request("call".to_owned(), json!([1, 2, 3]));
        let msg2 = Message::request("call".to_owned(), json!([1, 2, 3]));
        // They differ, even when created with the same parameters
        assert_ne!(msg1, msg2);
        // And, specifically, they differ in the ID's
        let (req1, req2) = if let (Message::Request(req1), Message::Request(req2)) = (msg1, msg2) {
            assert_ne!(req1.id, req2.id);
            assert!(req1.id.is_string());
            assert!(req2.id.is_string());
            (req1, req2)
        } else {
            panic!("Non-request received");
        };
        let id1 = req1.id.clone();
        // When we answer a message, we get the same ID
        if let Message::Response(ref resp) = req1.reply(json!([1, 2, 3])) {
            assert_eq!(*resp, Response { jsonrpc: Version, result: Ok(json!([1, 2, 3])), id: id1 });
        } else {
            panic!("Not a response");
        }
        let id2 = req2.id.clone();
        // The same with an error
        if let Message::Response(ref resp) =
            req2.error(RpcError::new(42, "Wrong!".to_owned(), None))
        {
            assert_eq!(
                *resp,
                Response {
                    jsonrpc: Version,
                    result: Err(RpcError::new(42, "Wrong!".to_owned(), None)),
                    id: id2,
                }
            );
        } else {
            panic!("Not a response");
        }
        // When we have unmatched, we generate a top-level error with Null id.
        if let Message::Response(ref resp) =
            Message::error(RpcError::new(43, "Also wrong!".to_owned(), None))
        {
            assert_eq!(
                *resp,
                Response {
                    jsonrpc: Version,
                    result: Err(RpcError::new(43, "Also wrong!".to_owned(), None)),
                    id: Value::Null,
                }
            );
        } else {
            panic!("Not a response");
        }
    }
}
