use near_primitives::hash::CryptoHash;
use near_structs_checker::ProtocolStruct;
use near_structs_checker_core::ProtocolStructInfo;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
// #[derive(Copy, Clone, Debug, ProtocolStruct)]
// pub struct CryptoHash(pub [u8; 33]);

fn compute_hash(
    info: &ProtocolStructInfo,
    structs: &HashMap<&'static str, &'static ProtocolStructInfo>,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    match info {
        ProtocolStructInfo::Struct { name, fields } => {
            name.hash(&mut hasher);
            for (field_name, field_type) in *fields {
                field_name.hash(&mut hasher);
                compute_type_hash(field_type, structs, &mut hasher);
            }
        }
        ProtocolStructInfo::Enum { name, variants } => {
            name.hash(&mut hasher);
            for (variant_name, variant_fields) in *variants {
                variant_name.hash(&mut hasher);
                if let Some(fields) = variant_fields {
                    for (field_name, field_type) in *fields {
                        // println!("Field: {} {}", field_name, field_type);
                        field_name.hash(&mut hasher);
                        compute_type_hash(field_type, structs, &mut hasher);
                    }
                }
            }
        }
    }
    hasher.finish()
}

fn compute_type_hash(
    ty: &str,
    structs: &HashMap<&'static str, &'static ProtocolStructInfo>,
    hasher: &mut DefaultHasher,
) {
    // println!("Computing hash for {}", ty);
    if let Some(nested_info) = structs.get(ty) {
        compute_hash(nested_info, structs).hash(hasher);
    } else {
        // Handle generic types
        let parts: Vec<&str> = ty.split('<').collect();
        parts[0].hash(hasher);
        if parts.len() > 1 {
            for part in parts[1].trim_end_matches('>').split(',') {
                compute_type_hash(part.trim(), structs, hasher);
            }
        } else {
            // println!("Warning: {} is primitive", ty);
        }
    }
}

fn main() {
    let stored_hashes: HashMap<String, u64> = serde_json::from_str(
        &fs::read_to_string("protocol_structs.json").unwrap_or_else(|_| "{}".to_string()),
    )
    .unwrap();

    let structs: HashMap<&'static str, &'static ProtocolStructInfo> =
        inventory::iter::<ProtocolStructInfo>.into_iter().map(|info| (info.name(), info)).collect();

    let mut current_hashes = HashMap::new();
    for (name, info) in &structs {
        let hash = compute_hash(info, &structs);
        current_hashes.insert((*name).to_string(), hash);
    }

    let mut has_changes = false;
    for (name, hash) in &current_hashes {
        match stored_hashes.get(name) {
            Some(stored_hash) if stored_hash != hash => {
                println!("Hash mismatch for {}: stored {}, current {}", name, stored_hash, hash);
                has_changes = true;
            }
            None => {
                println!("New struct: {} with hash {}", name, hash);
                has_changes = true;
            }
            _ => {}
        }
    }

    let current_keys: HashSet<_> = current_hashes.keys().collect();
    let stored_keys: HashSet<_> = stored_hashes.keys().collect();
    for removed in stored_keys.difference(&current_keys) {
        println!("Struct removed: {}", removed);
        has_changes = true;
    }

    if has_changes {
        fs::write("protocol_structs.json", serde_json::to_string_pretty(&current_hashes).unwrap())
            .unwrap();
        println!("Updated protocol_structs.json");
    } else {
        println!("No changes detected in protocol structs");
    }
}
