use serde::{Deserialize, Serialize};
use std::any::TypeId;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use once_cell::sync::Lazy;
use std::sync::Mutex;

#[derive(Clone, Serialize, Deserialize)]
pub struct FieldInfo {
    pub name: String,
    pub type_name: String,
    pub hash: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ProtocolStructInfo {
    pub name: String,
    pub hash: u64,
    pub fields: Vec<FieldInfo>,
}

pub trait ProtocolStruct {
    fn type_name() -> String {
        std::any::type_name::<Self>().to_string()
    }

    fn calculate_hash() -> u64 {
        let mut hasher = DefaultHasher::new();
        Self::type_name().hash(&mut hasher);
        hasher.finish()
    }

    fn get_info() -> ProtocolStructInfo {
        ProtocolStructInfo { name: Self::type_name(), hash: Self::calculate_hash(), fields: vec![] }
    }
}

// Implement ProtocolStruct for primitive types
macro_rules! impl_protocol_struct_for_primitive {
    ($($t:ty),*) => {
        $(
            impl ProtocolStruct for $t {}
        )*
    }
}

impl_protocol_struct_for_primitive!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, bool, String);

// Implement for arrays
impl<T: ProtocolStruct, const N: usize> ProtocolStruct for [T; N] {}

pub fn calculate_struct_hash<T: 'static>() -> u64 {
    let mut hasher = DefaultHasher::new();
    TypeId::of::<T>().hash(&mut hasher);
    hasher.finish()
}

static PROTOCOL_STRUCTS: Lazy<Mutex<Vec<ProtocolStructInfo>>> =
    Lazy::new(|| Mutex::new(Vec::new()));

pub fn register_protocol_struct(info: ProtocolStructInfo) {
    PROTOCOL_STRUCTS.lock().unwrap().push(info);
}

pub fn collect_protocol_structs() -> Vec<ProtocolStructInfo> {
    PROTOCOL_STRUCTS.lock().unwrap().clone()
}
