use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Copy, Clone, Serialize)]
pub enum ProtocolStructInfo {
    Struct {
        name: &'static str,
        fields: &'static [(&'static str, &'static str)],
    },
    Enum {
        name: &'static str,
        variants: &'static [(&'static str, Option<&'static [(&'static str, &'static str)]>)],
    },
}

impl ProtocolStructInfo {
    pub fn name(&self) -> &'static str {
        match self {
            ProtocolStructInfo::Struct { name, .. } => name,
            ProtocolStructInfo::Enum { name, .. } => name,
        }
    }
}

inventory::collect!(ProtocolStructInfo);

pub trait ProtocolStruct {}
