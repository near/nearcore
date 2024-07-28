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
pub type FieldTypeInfo = (TypeName, &'static [TypeId]);

#[derive(Copy, Clone)]
pub enum ProtocolStructInfo {
    Struct {
        name: FieldName,
        type_id: TypeId,
        fields: &'static [(FieldName, FieldTypeInfo)],
    },
    Enum {
        name: FieldName,
        type_id: TypeId,
        variants: &'static [(VariantName, Variant)],
    },
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
macro_rules! primitive_impl {
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

primitive_impl!(bool, u8, u16, u32, u64, u128, i8, i16, i32, i64, i128);
