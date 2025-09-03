use std::fmt;

use utoipa::openapi::RefOr;
use utoipa::openapi::schema::{Object, Schema, SchemaType, Type};
use utoipa::{PartialSchema, ToSchema};

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

impl PartialSchema for AccountId {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(Object::with_type(SchemaType::Type(Type::String))))
    }
}

impl ToSchema for AccountId {}
