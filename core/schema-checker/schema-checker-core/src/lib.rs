#![cfg_attr(enable_const_type_id, feature(const_type_id))]

use std::any::TypeId;

pub type TypeName = &'static str;
pub type FieldName = &'static str;
pub type VariantName = &'static str;
pub type Variant = Option<&'static [(FieldName, FieldTypeInfo)]>;

/// Type name and its decomposition into type ids.
/// Decomposition is defined recursively, starting from type id of the type
/// itself, followed by decompositions of its generic parameters, respectively.
/// For example, for `Vec<Vec<u8>>` it will be `[TypeId::of::<Vec<Vec<u8>>>(),
/// TypeId::of::<Vec<u8>>(), TypeId::of::<u8>()]`.
// TODO (#11755): consider better candidates for decomposition. For example,
// `Vec<u8>` is not expected to implement `ProtocolSchema`, so its type id
// won't help to identify changes in the outer struct.
pub type FieldTypeInfo = (TypeName, &'static [TypeId]);

#[derive(Debug, Copy, Clone)]
pub enum ProtocolSchemaInfo {
    Struct { name: FieldName, type_id: TypeId, fields: &'static [(FieldName, FieldTypeInfo)] },
    Enum { name: FieldName, type_id: TypeId, variants: &'static [(VariantName, Variant)] },
}

impl ProtocolSchemaInfo {
    pub fn type_id(&self) -> TypeId {
        match self {
            ProtocolSchemaInfo::Struct { type_id, .. } => *type_id,
            ProtocolSchemaInfo::Enum { type_id, .. } => *type_id,
        }
    }

    pub fn type_name(&self) -> TypeName {
        match self {
            ProtocolSchemaInfo::Struct { name, .. } => name,
            ProtocolSchemaInfo::Enum { name, .. } => name,
        }
    }
}

#[cfg(feature = "protocol_schema")]
inventory::collect!(ProtocolSchemaInfo);

pub trait ProtocolSchema {}

/// Implementation for primitive types.
macro_rules! primitive_impl {
    ($($t:ty),*) => {
        $(
            impl ProtocolSchema for $t {}

            #[cfg(all(enable_const_type_id, feature = "protocol_schema"))]
            inventory::submit! {
                ProtocolSchemaInfo::Struct {
                    name: stringify!($t),
                    type_id: TypeId::of::<$t>(),
                    fields: &[],
                }
            }
        )*
    }
}

primitive_impl!(bool, u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, String);
