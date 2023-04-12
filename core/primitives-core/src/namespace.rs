use std::{ops::Deref, string::FromUtf8Error};

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

// TODO: Character restraints (e.g. only alphanumeric)

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
    PartialEq,
    PartialOrd,
    Eq,
    Ord,
    Clone,
    Debug,
    Default,
    Hash,
)]
pub struct Namespace(String);

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<&'_ [u8]> for Namespace {
    type Error = FromUtf8Error;

    fn try_from(value: &'_ [u8]) -> Result<Self, Self::Error> {
        Ok(Self(String::from_utf8(value.to_vec())?))
    }
}

impl From<String> for Namespace {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for Namespace {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<Namespace> for String {
    fn from(value: Namespace) -> Self {
        value.0
    }
}

impl AsRef<str> for Namespace {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl AsRef<[u8]> for Namespace {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Deref for Namespace {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
