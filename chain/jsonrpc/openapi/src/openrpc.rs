//! Generates an OpenRPC specification for the NEAR JSON-RPC API.
//!
//! This produces a schema conforming to OpenRPC 1.3.2 that can be used for:
//! - Interactive documentation and playground (https://playground.open-rpc.org)
//! - TypeScript and Rust client code generation
//! - Service discovery via `rpc.discover` method
//!
//! Usage:
//!   cargo run -p near-jsonrpc-openapi-spec --bin near-openrpc > openrpc.json

use schemars::JsonSchema;
use schemars::transform::transform_subschemas;
use serde_json::json;

use near_chain_configs::GenesisConfig;
use near_jsonrpc_primitives::types::blocks::{RpcBlockRequest, RpcBlockResponse};
use near_jsonrpc_primitives::types::changes::{
    RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockByTypeResponse,
    RpcStateChangesInBlockRequest, RpcStateChangesInBlockResponse,
};
use near_jsonrpc_primitives::types::chunks::{RpcChunkRequest, RpcChunkResponse};
use near_jsonrpc_primitives::types::client_config::RpcClientConfigResponse;
use near_jsonrpc_primitives::types::config::{RpcProtocolConfigRequest, RpcProtocolConfigResponse};
use near_jsonrpc_primitives::types::congestion::{
    RpcCongestionLevelRequest, RpcCongestionLevelResponse,
};
use near_jsonrpc_primitives::types::gas_price::{RpcGasPriceRequest, RpcGasPriceResponse};
use near_jsonrpc_primitives::types::light_client::{
    RpcLightClientBlockProofRequest, RpcLightClientBlockProofResponse,
    RpcLightClientExecutionProofRequest, RpcLightClientExecutionProofResponse,
    RpcLightClientNextBlockRequest, RpcLightClientNextBlockResponse,
};
use near_jsonrpc_primitives::types::maintenance::{
    RpcMaintenanceWindowsRequest, RpcMaintenanceWindowsResponse,
};
use near_jsonrpc_primitives::types::network_info::RpcNetworkInfoResponse;
use near_jsonrpc_primitives::types::query::{RpcQueryRequest, RpcQueryResponse};
use near_jsonrpc_primitives::types::receipts::{RpcReceiptRequest, RpcReceiptResponse};
use near_jsonrpc_primitives::types::split_storage::{
    RpcSplitStorageInfoRequest, RpcSplitStorageInfoResponse,
};
use near_jsonrpc_primitives::types::status::{RpcHealthResponse, RpcStatusResponse};
use near_jsonrpc_primitives::types::transactions::{
    RpcSendTransactionRequest, RpcTransactionResponse, RpcTransactionStatusRequest,
};
use near_jsonrpc_primitives::types::validator::{
    RpcValidatorRequest, RpcValidatorResponse, RpcValidatorsOrderedRequest,
    RpcValidatorsOrderedResponse,
};
use near_primitives::hash::CryptoHash;

// Request types that are just empty structs
#[derive(JsonSchema)]
struct RpcStatusRequest {}

#[derive(JsonSchema)]
struct RpcHealthRequest {}

#[derive(JsonSchema)]
struct RpcNetworkInfoRequest {}

#[derive(JsonSchema)]
struct RpcClientConfigRequest {}

#[derive(JsonSchema)]
struct GenesisConfigRequest {}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().chain(chars).collect(),
            }
        })
        .collect()
}

/// Transform that fixes `#[serde(flatten)] Option<UntaggedEnum>` patterns for code generators.
#[derive(Debug, Clone)]
struct FlattenOptionFix;

impl FlattenOptionFix {
    fn extract_ref_name(value: &serde_json::Value) -> Option<String> {
        value
            .get("$ref")
            .and_then(|r| r.as_str())
            .and_then(|s| s.rsplit('/').next())
            .map(|s| s.to_string())
    }
}

impl schemars::transform::Transform for FlattenOptionFix {
    fn transform(&mut self, schema: &mut schemars::Schema) {
        transform_subschemas(self, schema);

        if schema.get("anyOf").is_none() {
            return;
        }

        let has_properties = schema.get("properties").is_some();

        if let Some(serde_json::Value::Array(arr)) = schema.get_mut("anyOf") {
            for variant in arr.iter_mut() {
                let Some(obj) = variant.as_object_mut() else { continue };
                let Some(serde_json::Value::String(title)) = obj.get("title") else { continue };
                if title.contains('_') {
                    obj.insert("title".to_string(), serde_json::json!(to_pascal_case(title)));
                }
            }
        }

        if !has_properties {
            if let Some(any_of) = schema.remove("anyOf") {
                schema.insert("oneOf".to_string(), any_of);
            }
            return;
        }

        let any_of = schema.remove("anyOf");
        let properties = schema.remove("properties");
        let required = schema.remove("required");
        let typ = schema.get("type").cloned();

        let mut base = serde_json::Map::new();
        if let Some(t) = typ {
            base.insert("type".to_string(), t);
        }
        if let Some(p) = properties {
            base.insert("properties".to_string(), p);
        }
        if let Some(r) = required {
            base.insert("required".to_string(), r);
        }
        let base_value = serde_json::Value::Object(base.clone());

        let expanded_variants = if let Some(serde_json::Value::Array(arr)) = any_of {
            arr.into_iter()
                .map(|v| {
                    if v.as_object().map(|o| o.is_empty()).unwrap_or(false) {
                        let mut result = base.clone();
                        result.insert("title".to_string(), serde_json::json!("Empty"));
                        serde_json::Value::Object(result)
                    } else {
                        let title = Self::extract_ref_name(&v);
                        let mut variant = serde_json::Map::new();
                        variant.insert(
                            "allOf".to_string(),
                            serde_json::json!([base_value.clone(), v]),
                        );
                        if let Some(name) = title {
                            variant.insert("title".to_string(), serde_json::json!(name));
                        }
                        serde_json::Value::Object(variant)
                    }
                })
                .collect::<Vec<_>>()
        } else {
            vec![base_value]
        };

        schema.insert("oneOf".to_string(), serde_json::Value::Array(expanded_variants));
    }
}

/// Transform that adds `title` to oneOf/anyOf variants for better code generation.
/// This handles internally-tagged enums, externally-tagged enums, and adjacently-tagged enums.
#[derive(Debug, Clone)]
struct AddVariantTitles;

impl AddVariantTitles {
    /// Extract a title for a variant based on its structure.
    /// This looks at various patterns to find a suitable name:
    /// - $ref names
    /// - const values (internally tagged enums)
    /// - single-property names (externally tagged enums)
    /// - titles nested in allOf schemas
    fn extract_variant_title(variant: &serde_json::Value) -> Option<String> {
        let obj = variant.as_object()?;

        // Already has a title at top level - skip
        if obj.contains_key("title") {
            return None;
        }

        // Check for $ref - use the referenced type name
        if let Some(ref_str) = obj.get("$ref").and_then(|r| r.as_str()) {
            return ref_str.rsplit('/').next().map(|s| s.to_string());
        }

        // Check for allOf - look for title in any subschema, or extract from subschema structure
        if let Some(all_of) = obj.get("allOf").and_then(|a| a.as_array()) {
            // First, look for any subschema that has a title
            for sub in all_of {
                if let Some(title) = sub.get("title").and_then(|t| t.as_str()) {
                    return Some(title.to_string());
                }
            }
            // If no title found, try to extract from subschema properties
            for sub in all_of {
                if let Some(title) = Self::extract_title_from_object(sub) {
                    return Some(title);
                }
            }
        }

        // Try to extract from this object's properties directly
        Self::extract_title_from_object(variant)
    }

    /// Extract a title from an object schema based on its properties
    fn extract_title_from_object(schema: &serde_json::Value) -> Option<String> {
        let obj = schema.as_object()?;

        // Check for internally tagged enum (has a property with `const` value)
        // e.g., { "properties": { "request_type": { "const": "view_account" } } }
        if let Some(props) = obj.get("properties").and_then(|p| p.as_object()) {
            for (_prop_name, prop_value) in props {
                if let Some(const_val) = prop_value.get("const").and_then(|c| c.as_str()) {
                    // Use the const value as the title
                    return Some(to_pascal_case(const_val));
                }
                // Check for enum with single value (alternative to const)
                if let Some(enum_arr) = prop_value.get("enum").and_then(|e| e.as_array()) {
                    if enum_arr.len() == 1 {
                        if let Some(val) = enum_arr[0].as_str() {
                            return Some(to_pascal_case(val));
                        }
                    }
                }
            }

            // Externally tagged enum or struct variant: use required properties
            // e.g., { "properties": { "Action": { ... } }, "required": ["Action"] }
            // or { "properties": { "tx_hash": ..., "sender_account_id": ... }, "required": ["tx_hash", "sender_account_id"] }
            if let Some(required) = obj.get("required").and_then(|r| r.as_array()) {
                // Filter out common non-discriminating property names
                let discriminating: Vec<&str> = required
                    .iter()
                    .filter_map(|v| v.as_str())
                    .filter(|name| !["type", "kind", "value", "data"].contains(name))
                    .collect();

                if !discriminating.is_empty() {
                    // For a single property, use it directly
                    // For multiple properties, combine them (up to 2 for readability)
                    let name = if discriminating.len() == 1 {
                        to_pascal_case(discriminating[0])
                    } else {
                        // Combine first two required properties
                        discriminating.iter().take(2).map(|s| to_pascal_case(s)).collect::<String>()
                    };
                    return Some(name);
                }
            }
        }

        None
    }

    fn add_names_to_variants(variants: &mut Vec<serde_json::Value>) {
        for variant in variants.iter_mut() {
            let Some(obj) = variant.as_object_mut() else { continue };

            // Skip if already has title or is just a $ref
            if obj.contains_key("title") || obj.contains_key("$ref") {
                continue;
            }

            // Try to extract a name for this variant
            if let Some(name) = Self::extract_variant_title(&serde_json::Value::Object(obj.clone()))
            {
                obj.insert("title".to_string(), serde_json::json!(name));
            }
        }
    }
}

impl schemars::transform::Transform for AddVariantTitles {
    fn transform(&mut self, schema: &mut schemars::Schema) {
        // Transform subschemas first
        transform_subschemas(self, schema);

        // Add names to oneOf variants
        if let Some(serde_json::Value::Array(variants)) = schema.get_mut("oneOf") {
            Self::add_names_to_variants(variants);
        }

        // Add names to anyOf variants
        if let Some(serde_json::Value::Array(variants)) = schema.get_mut("anyOf") {
            Self::add_names_to_variants(variants);
        }

        // Handle allOf with nested oneOf (common pattern)
        if let Some(serde_json::Value::Array(all_of)) = schema.get_mut("allOf") {
            for item in all_of.iter_mut() {
                if let Some(serde_json::Value::Array(variants)) = item.get_mut("oneOf") {
                    Self::add_names_to_variants(variants);
                }
                if let Some(serde_json::Value::Array(variants)) = item.get_mut("anyOf") {
                    Self::add_names_to_variants(variants);
                }
            }
        }
    }
}

/// Transform that cleans up $ref objects by removing sibling properties.
/// In JSON Schema, $ref should be the only property - siblings are ignored
/// but cause code generators to create duplicate types.
#[derive(Debug, Clone)]
struct CleanupRefs;

impl schemars::transform::Transform for CleanupRefs {
    fn transform(&mut self, schema: &mut schemars::Schema) {
        transform_subschemas(self, schema);

        let Some(obj) = schema.as_object_mut() else { return };
        if obj.contains_key("$ref") {
            obj.remove("title");
        }
    }
}

/// Transform that expands `allOf[oneOf, oneOf]` into a single `oneOf` with cartesian product.
/// This creates explicit combined variants with proper names, making code generation much cleaner.
#[derive(Debug, Clone)]
struct ExpandAllOfOneOf;

impl ExpandAllOfOneOf {
    fn merge_objects(
        a: &serde_json::Map<String, serde_json::Value>,
        b: &serde_json::Map<String, serde_json::Value>,
    ) -> serde_json::Map<String, serde_json::Value> {
        let mut result = a.clone();

        for (key, value) in b {
            match key.as_str() {
                "properties" => {
                    // Merge properties objects
                    let props = result.entry("properties").or_insert(json!({}));
                    if let (Some(props_obj), Some(value_obj)) =
                        (props.as_object_mut(), value.as_object())
                    {
                        for (k, v) in value_obj {
                            props_obj.insert(k.clone(), v.clone());
                        }
                    }
                }
                "required" => {
                    // Merge required arrays
                    let req = result.entry("required").or_insert(json!([]));
                    if let (Some(req_arr), Some(value_arr)) = (req.as_array_mut(), value.as_array())
                    {
                        for v in value_arr {
                            if !req_arr.contains(v) {
                                req_arr.push(v.clone());
                            }
                        }
                    }
                }
                "title" => {
                    // Skip - we'll set this ourselves
                }
                _ => {
                    // For other keys, prefer value from b (or keep a's)
                    result.insert(key.clone(), value.clone());
                }
            }
        }

        result
    }

    fn get_variant_name(obj: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
        obj.get("title").and_then(|v| v.as_str()).map(|s| s.to_string())
    }
}

impl schemars::transform::Transform for ExpandAllOfOneOf {
    fn transform(&mut self, schema: &mut schemars::Schema) {
        // Transform subschemas first
        transform_subschemas(self, schema);

        // Check if this is allOf with multiple oneOf subschemas
        let all_of = match schema.get("allOf") {
            Some(serde_json::Value::Array(arr)) => arr.clone(),
            _ => return,
        };

        // Collect all oneOf arrays from allOf
        let mut one_of_arrays: Vec<Vec<serde_json::Value>> = Vec::new();
        let mut other_schemas: Vec<serde_json::Value> = Vec::new();

        for item in &all_of {
            if let Some(one_of) = item.get("oneOf").and_then(|v| v.as_array()) {
                one_of_arrays.push(one_of.clone());
            } else if let Some(any_of) = item.get("anyOf").and_then(|v| v.as_array()) {
                one_of_arrays.push(any_of.clone());
            } else {
                other_schemas.push(item.clone());
            }
        }

        // Only expand if we have 2+ oneOf arrays (cartesian product case)
        if one_of_arrays.len() < 2 {
            return;
        }

        // Compute cartesian product of all oneOf variants
        let mut combined_variants: Vec<serde_json::Value> = vec![json!({})];

        for one_of in &one_of_arrays {
            let mut new_combined = Vec::new();

            for existing in &combined_variants {
                for variant in one_of {
                    let Some(existing_obj) = existing.as_object() else { continue };
                    let Some(variant_obj) = variant.as_object() else { continue };

                    let merged = Self::merge_objects(existing_obj, variant_obj);

                    // Combine variant names
                    let existing_name = Self::get_variant_name(existing_obj);
                    let variant_name = Self::get_variant_name(variant_obj);

                    let mut result = merged;
                    let combined_name = match (existing_name, variant_name) {
                        (Some(a), Some(b)) if !a.is_empty() => Some(format!("{}{}", a, b)),
                        (None, Some(b)) => Some(b),
                        (Some(a), None) if !a.is_empty() => Some(a),
                        _ => None,
                    };

                    if let Some(name) = combined_name {
                        result.insert("title".to_string(), json!(name));
                    }

                    new_combined.push(serde_json::Value::Object(result));
                }
            }

            combined_variants = new_combined;
        }

        // Merge any non-oneOf schemas into each variant
        for variant in &mut combined_variants {
            let Some(variant_obj) = variant.as_object_mut() else { continue };
            for other in &other_schemas {
                let Some(other_obj) = other.as_object() else { continue };
                let merged = Self::merge_objects(variant_obj, other_obj);
                *variant_obj = merged;
            }
        }

        // Replace allOf with oneOf
        if let Some(obj) = schema.as_object_mut() {
            obj.remove("allOf");
            obj.insert("oneOf".to_string(), json!(combined_variants));
        }
    }
}

/// Transform that merges top-level properties into oneOf variants.
/// This handles the pattern where a struct with #[serde(flatten)] on an enum
/// generates a schema with both `properties` and `oneOf` at the same level.
#[derive(Debug, Clone)]
struct MergePropertiesIntoOneOf;

impl MergePropertiesIntoOneOf {
    fn merge_objects(
        base: &serde_json::Map<String, serde_json::Value>,
        variant: &serde_json::Map<String, serde_json::Value>,
    ) -> serde_json::Map<String, serde_json::Value> {
        let mut result = variant.clone();

        // Merge properties
        if let Some(base_props) = base.get("properties").and_then(|p| p.as_object()) {
            let props = result.entry("properties").or_insert(json!({}));
            if let Some(props_obj) = props.as_object_mut() {
                for (k, v) in base_props {
                    if !props_obj.contains_key(k) {
                        props_obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // Merge required
        if let Some(base_required) = base.get("required").and_then(|r| r.as_array()) {
            let req = result.entry("required").or_insert(json!([]));
            if let Some(req_arr) = req.as_array_mut() {
                for v in base_required {
                    if !req_arr.contains(v) {
                        req_arr.push(v.clone());
                    }
                }
            }
        }

        result
    }
}

impl schemars::transform::Transform for MergePropertiesIntoOneOf {
    fn transform(&mut self, schema: &mut schemars::Schema) {
        // Transform subschemas first
        transform_subschemas(self, schema);

        // Check if schema has both oneOf and properties at top level
        let has_one_of = schema.get("oneOf").and_then(|v| v.as_array()).is_some();
        let has_properties = schema.get("properties").is_some();

        if !has_one_of || !has_properties {
            return;
        }

        // Extract the base properties/required
        let base = schema.as_object().map(|obj| {
            let mut base = serde_json::Map::new();
            if let Some(props) = obj.get("properties") {
                base.insert("properties".to_string(), props.clone());
            }
            if let Some(req) = obj.get("required") {
                base.insert("required".to_string(), req.clone());
            }
            base
        });

        let Some(base) = base else { return };

        // Skip if there are no base properties to merge
        if base.get("properties").and_then(|p| p.as_object()).map(|p| p.is_empty()).unwrap_or(true)
        {
            return;
        }

        // Merge base into each oneOf variant
        if let Some(serde_json::Value::Array(variants)) = schema.get_mut("oneOf") {
            for variant in variants.iter_mut() {
                if let Some(variant_obj) = variant.as_object() {
                    *variant = serde_json::Value::Object(Self::merge_objects(&base, variant_obj));
                }
            }
        }

        // Remove top-level properties and required since they're now in each variant
        if let Some(obj) = schema.as_object_mut() {
            obj.remove("properties");
            obj.remove("required");
        }
    }
}

/// Configuration for collapsing a cartesian product explosion back to composed types.
struct CartesianCollapseConfig {
    /// Name of the type with the explosion (e.g., "RpcQueryRequest")
    type_name: &'static str,
    /// Component types that were flattened together
    components: &'static [ComponentConfig],
}

struct ComponentConfig {
    /// Name of the component type (e.g., "BlockReference")
    name: &'static str,
    /// Properties that discriminate this component's variants
    discriminator_props: &'static [&'static str],
    /// If internally tagged, the tag property name (e.g., "request_type")
    tag_prop: Option<&'static str>,
}

/// Known cartesian product explosions to collapse
const CARTESIAN_COLLAPSE_CONFIGS: &[CartesianCollapseConfig] = &[
    CartesianCollapseConfig {
        type_name: "RpcQueryRequest",
        components: &[
            ComponentConfig {
                name: "BlockReference",
                discriminator_props: &["block_id", "finality", "sync_checkpoint"],
                tag_prop: None,
            },
            ComponentConfig {
                name: "QueryRequest",
                discriminator_props: &["request_type"],
                tag_prop: Some("request_type"),
            },
        ],
    },
    CartesianCollapseConfig {
        type_name: "RpcStateChangesInBlockByTypeRequest",
        components: &[
            ComponentConfig {
                name: "BlockReference",
                discriminator_props: &["block_id", "finality", "sync_checkpoint"],
                tag_prop: None,
            },
            ComponentConfig {
                name: "StateChangesRequestView",
                discriminator_props: &["changes_type"],
                tag_prop: Some("changes_type"),
            },
        ],
    },
];

/// Collapse cartesian product explosions in the schema.
fn collapse_cartesian_products(schemas: &mut serde_json::Map<String, serde_json::Value>) {
    for config in CARTESIAN_COLLAPSE_CONFIGS {
        let type_schema = match schemas.get(config.type_name) {
            Some(s) => s.clone(),
            None => continue,
        };

        let variants = match type_schema.get("oneOf").and_then(|v| v.as_array()) {
            Some(arr) => arr.clone(),
            None => continue,
        };

        // Extract unique variants for each component
        let mut component_variants: std::collections::HashMap<&str, Vec<serde_json::Value>> =
            std::collections::HashMap::new();
        let mut seen: std::collections::HashMap<&str, std::collections::HashSet<String>> =
            std::collections::HashMap::new();

        for comp in config.components {
            component_variants.insert(comp.name, Vec::new());
            seen.insert(comp.name, std::collections::HashSet::new());
        }

        for variant in &variants {
            // Handle both direct properties and allOf-wrapped variants
            let props = if let Some(p) = variant.get("properties").and_then(|p| p.as_object()) {
                p.clone()
            } else if let Some(all_of) = variant.get("allOf").and_then(|a| a.as_array()) {
                // Merge all properties from allOf subschemas
                let mut merged = serde_json::Map::new();
                for sub in all_of {
                    if let Some(sub_props) = sub.get("properties").and_then(|p| p.as_object()) {
                        for (k, v) in sub_props {
                            merged.insert(k.clone(), v.clone());
                        }
                    }
                }
                merged
            } else {
                continue;
            };

            let required = variant
                .get("required")
                .and_then(|r| r.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                .or_else(|| {
                    // Also check allOf subschemas for required
                    variant.get("allOf").and_then(|a| a.as_array()).map(|arr| {
                        arr.iter()
                            .filter_map(|sub| sub.get("required").and_then(|r| r.as_array()))
                            .flat_map(|r| r.iter().filter_map(|v| v.as_str()))
                            .collect::<Vec<_>>()
                    })
                })
                .unwrap_or_default();

            for comp in config.components {
                // Find which discriminator property this variant has for this component
                let discriminator_prop =
                    comp.discriminator_props.iter().find(|&&p| props.contains_key(p));
                let discriminator_prop = match discriminator_prop {
                    Some(p) => *p,
                    None => continue,
                };

                // Build signature for deduplication
                let signature = if let Some(tag_prop) = comp.tag_prop {
                    if let Some(tag_schema) = props.get(tag_prop) {
                        let tag_value =
                            tag_schema.get("const").and_then(|v| v.as_str()).or_else(|| {
                                tag_schema
                                    .get("enum")
                                    .and_then(|e| e.as_array())
                                    .and_then(|arr| arr.first())
                                    .and_then(|v| v.as_str())
                            });
                        match tag_value {
                            Some(v) => format!("{}:{}", tag_prop, v),
                            None => discriminator_prop.to_string(),
                        }
                    } else {
                        discriminator_prop.to_string()
                    }
                } else {
                    discriminator_prop.to_string()
                };

                if seen.get(comp.name).unwrap().contains(&signature) {
                    continue;
                }
                seen.get_mut(comp.name).unwrap().insert(signature.clone());

                // Extract only the properties relevant to this component
                let mut comp_props = serde_json::Map::new();
                let mut comp_required = Vec::new();

                for (prop_name, prop_value) in &props {
                    // Check if this property belongs to this component
                    let belongs_to_comp = comp.discriminator_props.contains(&prop_name.as_str());

                    // For internally tagged enums, also include non-discriminator props
                    let is_shared = !config
                        .components
                        .iter()
                        .any(|c| c.discriminator_props.contains(&prop_name.as_str()));

                    if belongs_to_comp || (comp.tag_prop.is_some() && is_shared) {
                        comp_props.insert(prop_name.clone(), prop_value.clone());
                        if required.contains(&prop_name.as_str()) {
                            comp_required.push(prop_name.clone());
                        }
                    }
                }

                // Only add if we have meaningful properties
                if comp_props.is_empty() {
                    continue;
                }

                let mut comp_variant = serde_json::Map::new();
                comp_variant.insert("type".to_string(), json!("object"));
                comp_variant
                    .insert("properties".to_string(), serde_json::Value::Object(comp_props));
                if !comp_required.is_empty() {
                    comp_variant.insert("required".to_string(), json!(comp_required));
                }

                // Add title based on signature
                let title = if let Some(tag_prop) = comp.tag_prop {
                    if let Some(tag_schema) = props.get(tag_prop) {
                        tag_schema
                            .get("const")
                            .and_then(|v| v.as_str())
                            .or_else(|| {
                                tag_schema
                                    .get("enum")
                                    .and_then(|e| e.as_array())
                                    .and_then(|arr| arr.first())
                                    .and_then(|v| v.as_str())
                            })
                            .map(|v| to_pascal_case(v))
                    } else {
                        Some(to_pascal_case(discriminator_prop))
                    }
                } else {
                    Some(to_pascal_case(discriminator_prop))
                };

                if let Some(t) = title {
                    comp_variant.insert("title".to_string(), json!(t));
                }

                component_variants
                    .get_mut(comp.name)
                    .unwrap()
                    .push(serde_json::Value::Object(comp_variant));
            }
        }

        // Add component schemas if they don't exist
        for comp in config.components {
            let comp_variants = component_variants.get(comp.name).unwrap();
            if comp_variants.is_empty() {
                continue;
            }

            if !schemas.contains_key(comp.name) {
                schemas.insert(
                    comp.name.to_string(),
                    json!({
                        "oneOf": comp_variants,
                        "title": comp.name
                    }),
                );
            }
        }

        // Replace the explosion with allOf refs
        let all_of_refs: Vec<serde_json::Value> = config
            .components
            .iter()
            .map(|comp| json!({"$ref": format!("#/components/schemas/{}", comp.name)}))
            .collect();

        if let Some(type_schema) = schemas.get_mut(config.type_name) {
            let Some(obj) = type_schema.as_object_mut() else { continue };
            obj.remove("oneOf");
            obj.insert("allOf".to_string(), json!(all_of_refs));
        }
    }
}

/// Generates the OpenRPC specification.
pub fn generate_openrpc() -> serde_json::Value {
    let mut methods = Vec::new();
    let mut all_schemas = serde_json::Map::new();

    /// Adds a method to the OpenRPC spec
    fn add_method<Params: JsonSchema, Result: JsonSchema>(
        methods: &mut Vec<serde_json::Value>,
        all_schemas: &mut serde_json::Map<String, serde_json::Value>,
        method_name: &str,
        summary: &str,
        deprecated: bool,
        tags: &[&str],
    ) {
        // Generate schemas using Draft-07 for OpenRPC compatibility
        let mut settings = schemars::generate::SchemaSettings::draft07();
        settings.transforms.push(Box::new(FlattenOptionFix));
        settings.transforms.push(Box::new(MergePropertiesIntoOneOf));
        settings.transforms.push(Box::new(AddVariantTitles));
        settings.transforms.push(Box::new(ExpandAllOfOneOf));
        settings.transforms.push(Box::new(CleanupRefs));
        let generator = schemars::SchemaGenerator::new(settings);

        let params_schema = generator.clone().into_root_schema_for::<Params>();
        let result_schema = generator.into_root_schema_for::<Result>();

        // Helper to clean schema
        let clean_schema = |v: &serde_json::Value, title: &str| -> serde_json::Value {
            let mut clean = v.clone();
            if let Some(obj) = clean.as_object_mut() {
                obj.remove("$schema");
                obj.remove("definitions");
                if !obj.contains_key("title") {
                    obj.insert("title".to_string(), json!(title));
                }
            }
            clean
        };

        // Extract definitions and merge into all_schemas
        for schema in [&params_schema, &result_schema] {
            let Some(defs) = schema.as_value().get("definitions") else { continue };
            let Some(defs_obj) = defs.as_object() else { continue };
            for (k, v) in defs_obj {
                all_schemas.insert(k.clone(), clean_schema(v, k));
            }
        }

        // Add root schemas to definitions
        let params_name = Params::schema_name().to_string();
        let result_name = Result::schema_name().to_string();

        all_schemas
            .insert(params_name.clone(), clean_schema(params_schema.as_value(), &params_name));
        all_schemas
            .insert(result_name.clone(), clean_schema(result_schema.as_value(), &result_name));

        // Build OpenRPC method object
        // NEAR uses by-name params where the entire params object is passed
        // We represent this as a single content descriptor referencing the request schema
        let mut method = serde_json::Map::new();
        method.insert("name".to_string(), json!(method_name));
        method.insert("summary".to_string(), json!(summary));
        method.insert("paramStructure".to_string(), json!("by-name"));

        // Single param that references the full request type schema
        // The schema itself defines what properties are valid
        method.insert(
            "params".to_string(),
            json!([
                {
                    "name": "request",
                    "description": format!("Request parameters for {}", method_name),
                    "required": true,
                    "schema": { "$ref": format!("#/components/schemas/{}", params_name) }
                }
            ]),
        );

        // Result content descriptor
        method.insert(
            "result".to_string(),
            json!({
                "name": "result",
                "schema": { "$ref": format!("#/components/schemas/{}", result_name) }
            }),
        );

        if deprecated {
            method.insert("deprecated".to_string(), json!(true));
        }

        if !tags.is_empty() {
            method.insert(
                "tags".to_string(),
                json!(tags.iter().map(|t| json!({"name": t})).collect::<Vec<_>>()),
            );
        }

        methods.push(serde_json::Value::Object(method));
    }

    // ==================== Core RPC Methods ====================

    add_method::<RpcBlockRequest, RpcBlockResponse>(
        &mut methods,
        &mut all_schemas,
        "block",
        "Returns block details for given height or hash",
        false,
        &["block"],
    );
    add_method::<RpcChunkRequest, RpcChunkResponse>(
        &mut methods,
        &mut all_schemas,
        "chunk",
        "Returns details of a specific chunk",
        false,
        &["chunk"],
    );
    add_method::<RpcGasPriceRequest, RpcGasPriceResponse>(
        &mut methods,
        &mut all_schemas,
        "gas_price",
        "Returns gas price for a specific block_height or block_hash",
        false,
        &["gas"],
    );
    add_method::<RpcQueryRequest, RpcQueryResponse>(
        &mut methods,
        &mut all_schemas,
        "query",
        "Query the blockchain state (view account, call function, etc.)",
        false,
        &["query"],
    );
    add_method::<RpcSendTransactionRequest, RpcTransactionResponse>(
        &mut methods,
        &mut all_schemas,
        "send_tx",
        "Sends a transaction and optionally waits for execution",
        false,
        &["transaction"],
    );
    add_method::<RpcTransactionStatusRequest, RpcTransactionResponse>(
        &mut methods,
        &mut all_schemas,
        "tx",
        "Queries status of a transaction by hash",
        false,
        &["transaction"],
    );
    add_method::<RpcStatusRequest, RpcStatusResponse>(
        &mut methods,
        &mut all_schemas,
        "status",
        "Returns the status of the RPC node",
        false,
        &["network"],
    );
    add_method::<RpcValidatorRequest, RpcValidatorResponse>(
        &mut methods,
        &mut all_schemas,
        "validators",
        "Queries active validators on the network",
        false,
        &["validator"],
    );
    add_method::<RpcNetworkInfoRequest, RpcNetworkInfoResponse>(
        &mut methods,
        &mut all_schemas,
        "network_info",
        "Queries the current state of node network connections",
        false,
        &["network"],
    );
    add_method::<RpcHealthRequest, RpcHealthResponse>(
        &mut methods,
        &mut all_schemas,
        "health",
        "Returns health status of the node",
        false,
        &["network"],
    );

    // ==================== Light Client Methods ====================

    add_method::<RpcLightClientExecutionProofRequest, RpcLightClientExecutionProofResponse>(
        &mut methods,
        &mut all_schemas,
        "light_client_proof",
        "Returns execution proof for light clients",
        false,
        &["light_client"],
    );
    add_method::<RpcLightClientNextBlockRequest, RpcLightClientNextBlockResponse>(
        &mut methods,
        &mut all_schemas,
        "next_light_client_block",
        "Returns the next light client block",
        false,
        &["light_client"],
    );
    add_method::<RpcLightClientBlockProofRequest, RpcLightClientBlockProofResponse>(
        &mut methods,
        &mut all_schemas,
        "light_client_block_proof",
        "Returns block proof for light clients",
        false,
        &["light_client"],
    );

    // ==================== EXPERIMENTAL Methods ====================

    add_method::<RpcStateChangesInBlockRequest, RpcStateChangesInBlockByTypeResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_changes_in_block",
        "Returns changes in block for given block height or hash",
        false,
        &["changes", "experimental"],
    );
    add_method::<RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_changes",
        "Returns state changes for specific state change kinds",
        false,
        &["changes", "experimental"],
    );
    add_method::<RpcProtocolConfigRequest, RpcProtocolConfigResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_protocol_config",
        "Returns protocol configuration for given block",
        false,
        &["config", "experimental"],
    );
    add_method::<GenesisConfigRequest, GenesisConfig>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_genesis_config",
        "Returns genesis configuration of the network",
        false,
        &["config", "experimental"],
    );
    add_method::<RpcReceiptRequest, RpcReceiptResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_receipt",
        "Returns receipt by receipt_id",
        false,
        &["receipt", "experimental"],
    );
    add_method::<RpcMaintenanceWindowsRequest, RpcMaintenanceWindowsResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_maintenance_windows",
        "Returns maintenance windows for validators",
        false,
        &["validator", "experimental"],
    );
    add_method::<RpcSplitStorageInfoRequest, RpcSplitStorageInfoResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_split_storage_info",
        "Returns split storage information",
        false,
        &["storage", "experimental"],
    );
    add_method::<RpcCongestionLevelRequest, RpcCongestionLevelResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_congestion_level",
        "Returns congestion level for a chunk",
        false,
        &["chunk", "experimental"],
    );
    add_method::<RpcValidatorsOrderedRequest, RpcValidatorsOrderedResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_validators_ordered",
        "Returns validators ordered by stake for given epoch",
        false,
        &["validator", "experimental"],
    );
    add_method::<RpcClientConfigRequest, RpcClientConfigResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_client_config",
        "Returns client configuration",
        false,
        &["config", "experimental"],
    );
    add_method::<RpcTransactionStatusRequest, RpcTransactionResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_tx_status",
        "Queries status of a transaction by hash (alias for tx)",
        false,
        &["transaction", "experimental"],
    );
    add_method::<RpcLightClientExecutionProofRequest, RpcLightClientExecutionProofResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_light_client_proof",
        "Returns execution proof for light clients",
        false,
        &["light_client", "experimental"],
    );
    add_method::<RpcLightClientBlockProofRequest, RpcLightClientBlockProofResponse>(
        &mut methods,
        &mut all_schemas,
        "EXPERIMENTAL_light_client_block_proof",
        "Returns block proof for light clients",
        false,
        &["light_client", "experimental"],
    );

    // ==================== Aliases ====================

    add_method::<RpcStateChangesInBlockRequest, RpcStateChangesInBlockByTypeResponse>(
        &mut methods,
        &mut all_schemas,
        "block_effects",
        "Returns changes in block (alias for EXPERIMENTAL_changes_in_block)",
        false,
        &["changes"],
    );
    add_method::<RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockResponse>(
        &mut methods,
        &mut all_schemas,
        "changes",
        "Returns state changes (alias for EXPERIMENTAL_changes)",
        false,
        &["changes"],
    );
    add_method::<GenesisConfigRequest, GenesisConfig>(
        &mut methods,
        &mut all_schemas,
        "genesis_config",
        "Returns genesis configuration (alias for EXPERIMENTAL_genesis_config)",
        false,
        &["config"],
    );
    add_method::<RpcClientConfigRequest, RpcClientConfigResponse>(
        &mut methods,
        &mut all_schemas,
        "client_config",
        "Returns client configuration (alias for EXPERIMENTAL_client_config)",
        false,
        &["config"],
    );
    add_method::<RpcMaintenanceWindowsRequest, RpcMaintenanceWindowsResponse>(
        &mut methods,
        &mut all_schemas,
        "maintenance_windows",
        "Returns maintenance windows (alias for EXPERIMENTAL_maintenance_windows)",
        false,
        &["validator"],
    );

    // ==================== Deprecated Methods ====================

    add_method::<RpcSendTransactionRequest, CryptoHash>(
        &mut methods,
        &mut all_schemas,
        "broadcast_tx_async",
        "Sends a transaction and immediately returns hash (deprecated: use send_tx)",
        true,
        &["transaction", "deprecated"],
    );
    add_method::<RpcSendTransactionRequest, RpcTransactionResponse>(
        &mut methods,
        &mut all_schemas,
        "broadcast_tx_commit",
        "Sends a transaction and waits for completion (deprecated: use send_tx)",
        true,
        &["transaction", "deprecated"],
    );

    // ==================== Post-processing ====================

    // Rename ugly auto-generated type names
    let type_renames: &[(&str, &str)] = &[
        ("Array_of_Range_of_uint64", "BlockHeightRanges"),
        ("Range_of_uint64", "BlockHeightRange"),
        ("Array_of_ValidatorStakeView", "ValidatorStakeViews"),
        ("Array_of_bool", "BoolArray"),
        ("Nullable_CryptoHash", "OptionalCryptoHash"),
        ("Nullable_AccountId", "OptionalAccountId"),
    ];

    fn rename_refs_in_value(value: &mut serde_json::Value, renames: &[(&str, &str)]) {
        match value {
            serde_json::Value::Object(obj) => {
                if let Some(serde_json::Value::String(ref_str)) = obj.get_mut("$ref") {
                    // Convert #/definitions/ to #/components/schemas/ for OpenRPC compatibility
                    if ref_str.starts_with("#/definitions/") {
                        *ref_str = ref_str.replace("#/definitions/", "#/components/schemas/");
                    }
                    // Apply type renames
                    for (old, new) in renames {
                        if ref_str.ends_with(&format!("/{}", old)) {
                            *ref_str = ref_str.replace(old, new);
                            break;
                        }
                    }
                }
                for v in obj.values_mut() {
                    rename_refs_in_value(v, renames);
                }
            }
            serde_json::Value::Array(arr) => {
                for v in arr {
                    rename_refs_in_value(v, renames);
                }
            }
            _ => {}
        }
    }

    // Rename keys in schemas
    for (old_name, new_name) in type_renames {
        if let Some(def) = all_schemas.remove(*old_name) {
            let mut updated_def = def;
            if let Some(obj) = updated_def.as_object_mut() {
                obj.insert("title".to_string(), json!(new_name));
            }
            all_schemas.insert((*new_name).to_string(), updated_def);
        }
    }

    // Collapse cartesian product explosions back into composed allOf references.
    // When Rust has `#[serde(flatten)]` on multiple enum fields, schemars generates
    // the cartesian product (e.g., 3 × 8 = 24 variants). We detect these patterns
    // and collapse them back to `allOf[BlockReference, QueryRequest]`.
    collapse_cartesian_products(&mut all_schemas);

    // Build final OpenRPC document
    let mut openrpc = json!({
        "openrpc": "1.3.2",
        "info": {
            "title": "NEAR Protocol JSON-RPC API",
            "version": "1.0.0",
            "description": "JSON-RPC API for interacting with the NEAR Protocol blockchain",
            "license": {
                "name": "Apache-2.0",
                "url": "https://www.apache.org/licenses/LICENSE-2.0"
            },
            "contact": {
                "name": "NEAR Protocol",
                "url": "https://near.org"
            }
        },
        "servers": [
            {
                "name": "mainnet",
                "url": "https://rpc.mainnet.near.org",
                "summary": "NEAR Mainnet RPC endpoint"
            },
            {
                "name": "testnet",
                "url": "https://rpc.testnet.near.org",
                "summary": "NEAR Testnet RPC endpoint"
            }
        ],
        "methods": methods,
        "components": {
            "schemas": all_schemas
        }
    });

    rename_refs_in_value(&mut openrpc, type_renames);

    openrpc
}
