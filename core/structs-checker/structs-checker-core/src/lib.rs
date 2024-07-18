use std::any::TypeId;

pub type FieldName = &'static str;
pub type EnumVariant = Option<&'static [(FieldName, TypeId)]>;

#[derive(Copy, Clone)]
pub enum ProtocolStructInfo {
    Struct { name: FieldName, type_id: TypeId, fields: &'static [(FieldName, TypeId)] },
    Enum { name: FieldName, type_id: TypeId, variants: &'static [(FieldName, EnumVariant)] },
}

impl ProtocolStructInfo {
    pub fn type_id(&self) -> TypeId {
        match self {
            ProtocolStructInfo::Struct { type_id, .. } => *type_id,
            ProtocolStructInfo::Enum { type_id, .. } => *type_id,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            ProtocolStructInfo::Struct { name, .. } => name,
            ProtocolStructInfo::Enum { name, .. } => name,
        }
    }
}

#[cfg(feature = "protocol_schema")]
inventory::collect!(ProtocolStructInfo);

pub trait ProtocolStruct {}
