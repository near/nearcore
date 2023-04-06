use std::ops::Deref;

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

impl Deref for Namespace {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
