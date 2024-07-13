#![cfg_attr(enable_const_type_id, feature(const_type_id))]

use std::any::TypeId;

pub type TypeName = &'static str;
pub type FieldName = &'static str;
pub type EnumVariant = Option<&'static [(FieldName, TypeId)]>;

pub type TypeInfo = (&'static str, [Option<TypeId>; 4]);

#[derive(Copy, Clone)]
pub enum ProtocolStructInfo {
    Struct { name: FieldName, type_id: TypeId, fields: &'static [(FieldName, TypeInfo)] },
    Enum { name: FieldName, type_id: TypeId, variants: &'static [(FieldName, Option<&'static [(FieldName, TypeInfo)]>)] },
}

impl ProtocolStructInfo {
    pub fn type_id(&self) -> TypeId {
        match self {
            ProtocolStructInfo::Struct { type_id, .. } => *type_id,
            ProtocolStructInfo::Enum { type_id, .. } => *type_id,
        }
    }

    pub fn type_name(&self) -> TypeName {
        match self {
            ProtocolStructInfo::Struct { name, .. } => name,
            ProtocolStructInfo::Enum { name, .. } => name,
        }
    }
}

#[cfg(feature = "protocol_schema")]
inventory::collect!(ProtocolStructInfo);

pub trait ProtocolStruct {}

/// Implementation for primitive types.
macro_rules! impl_for_int {
    ($($t:ty),*) => {
        $(
            impl ProtocolStruct for $t {}

            #[cfg(feature = "protocol_schema")]
            inventory::submit! {
                ProtocolStructInfo::Struct {
                    name: stringify!($t),
                    type_id: TypeId::of::<$t>(),
                    fields: &[],
                }
            }
        )*
    }
}

impl_for_int!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);
