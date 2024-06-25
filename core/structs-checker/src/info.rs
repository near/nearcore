use std::any::TypeId;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct ProtocolStructInfo {
    pub name: &'static str,
    pub hash: u64,
}

pub fn calculate_struct_hash<T: 'static>() -> u64 {
    let mut hasher = DefaultHasher::new();
    TypeId::of::<T>().hash(&mut hasher);
    hasher.finish()
}
