use std::fmt;

use utoipa::ToSchema;
use utoipa::openapi::RefOr;
use utoipa::openapi::schema::{ObjectBuilder, Schema, SchemaType};

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

impl ToSchema<'_> for AccountId {
    fn schema() -> (&'static str, RefOr<Schema>) {
        (
            "AccountId",
            RefOr::T(Schema::Object(ObjectBuilder::new().schema_type(SchemaType::String).build())),
        )
    }
}
