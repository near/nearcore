use std::{fmt, ops::Deref};

#[derive(
    Eq,
    Ord,
    Hash,
    Clone,
    PartialEq,
    PartialOrd,
    derive_more::From,
    derive_more::Into,
    derive_more::AsRef,
    derive_more::Deref,
    derive_more::FromStr,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct AccountId(near_primitives::types::AccountId);

impl fmt::Debug for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

use near_primitives::{namespace::Namespace, routing_table::RoutingTable};
use paperclip::v2::{models::DataType, schema::TypedData};
use serde::{Deserialize, Serialize};
impl TypedData for AccountId {
    fn data_type() -> DataType {
        DataType::String
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Default)]
pub(crate) struct TypedNamespace(Namespace);

impl From<Namespace> for TypedNamespace {
    fn from(value: Namespace) -> Self {
        Self(value)
    }
}

impl From<TypedNamespace> for Namespace {
    fn from(value: TypedNamespace) -> Self {
        value.0
    }
}

impl Deref for TypedNamespace {
    type Target = Namespace;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TypedData for TypedNamespace {
    fn data_type() -> DataType {
        DataType::String
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Default)]
pub(crate) struct TypedRoutingTable(RoutingTable);

impl From<RoutingTable> for TypedRoutingTable {
    fn from(value: RoutingTable) -> Self {
        Self(value)
    }
}

impl From<TypedRoutingTable> for RoutingTable {
    fn from(value: TypedRoutingTable) -> Self {
        value.0
    }
}

impl Deref for TypedRoutingTable {
    type Target = RoutingTable;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TypedData for TypedRoutingTable {
    fn data_type() -> DataType {
        DataType::Object
    }
}
