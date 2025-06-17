use okapi::openapi3::{OpenApi, SchemaObject};
use schemars::JsonSchema;
use schemars::transform::transform_subschemas;
use serde_json::json;

use near_jsonrpc_primitives::types::{
    blocks::{RpcBlockRequest, RpcBlockResponse},
    changes::{
        RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockByTypeResponse,
        RpcStateChangesInBlockRequest,
    },
    chunks::{RpcChunkRequest, RpcChunkResponse},
    client_config::RpcClientConfigResponse,
    config::{RpcProtocolConfigRequest, RpcProtocolConfigResponse},
    congestion::{RpcCongestionLevelRequest, RpcCongestionLevelResponse},
    gas_price::{RpcGasPriceRequest, RpcGasPriceResponse},
    light_client::{
        RpcLightClientBlockProofRequest, RpcLightClientBlockProofResponse,
        RpcLightClientExecutionProofResponse, RpcLightClientNextBlockRequest,
        RpcLightClientNextBlockResponse,
    },
    maintenance::{RpcMaintenanceWindowsRequest, RpcMaintenanceWindowsResponse},
    network_info::RpcNetworkInfoResponse,
    query::{RpcQueryRequest, RpcQueryResponse},
    receipts::{RpcReceiptRequest, RpcReceiptResponse},
    split_storage::{RpcSplitStorageInfoRequest, RpcSplitStorageInfoResponse},
    status::{RpcHealthResponse, RpcStatusResponse},
    transactions::{
        RpcSendTransactionRequest, RpcTransactionResponse, RpcTransactionStatusRequest,
    },
    validator::{
        RpcValidatorRequest, RpcValidatorResponse, RpcValidatorsOrderedRequest,
        RpcValidatorsOrderedResponse,
    },
};
use near_primitives::hash::CryptoHash;

use near_chain_configs::GenesisConfig;

#[cfg(not(feature = "progenitor"))]
use near_jsonrpc_primitives::{
    errors::RpcError,
    types::{
        changes::RpcStateChangesInBlockResponse, light_client::RpcLightClientExecutionProofRequest,
    },
};

mod progenitor_types;
#[cfg(feature = "progenitor")]
use progenitor_types::{
    RpcError, RpcLightClientExecutionProofRequest, RpcStateChangesInBlockResponse,
};

#[derive(JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResponseEither<T, E> {
    Result(T),
    Error(E),
}

#[derive(JsonSchema)]
#[allow(dead_code)]
struct JsonRpcResponse<T, E> {
    jsonrpc: String,
    id: String,
    #[serde(flatten)]
    response_or_error: ResponseEither<T, E>,
}

type SchemasMap = serde_json::Value;
type PathsMap = okapi::Map<String, okapi::openapi3::PathItem>;

#[derive(Debug, Clone)]
pub struct ReplaceNullType;

impl schemars::transform::Transform for ReplaceNullType {
    fn transform(&mut self, schema: &mut schemars::Schema) {
        transform_subschemas(self, schema);

        if let Some(value) = schema.get("type") {
            if value == "null" {
                schema.insert("type".into(), serde_json::Value::String("object".to_string()));
            }
        }
    }
}

fn schemas_map<T: JsonSchema>() -> SchemasMap {
    let mut settings = schemars::generate::SchemaSettings::openapi3();
    settings.transforms.insert(
        0,
        Box::new(|s: &mut schemars::Schema| {
            let obj = s.ensure_object();
            if let Some(components) = obj.get("components") {
                if let Some(_) = components.get("schemas") {
                    let defs = obj["components"]["schemas"].take();
                    obj.insert("$defs".to_owned(), defs);
                }
            }
        }),
    );
    settings.transforms.push(Box::new(ReplaceNullType));
    settings.transforms.push(Box::new(|s: &mut schemars::Schema| {
        let obj = s.ensure_object();
        if !obj.get("$defs").is_none() {
            obj["components"]["schemas"] = obj.remove("$defs").unwrap();
        }
    }));
    let generator = schemars::SchemaGenerator::new(settings);

    let root_schema = generator.into_root_schema_for::<T>();

    let the_schema = root_schema.as_value();

    let mut result: SchemasMap = if let Some(components) = the_schema.get("components") {
        components.get("schemas").unwrap().clone()
    } else {
        json!({})
    };
    let root_schema_name = the_schema.get("title").unwrap().as_str().unwrap();

    let mut the_schema = the_schema.clone();
    let mutable = the_schema.as_object_mut().unwrap();
    mutable.remove("components");
    mutable.remove("$schema");

    result
        .as_object_mut()
        .unwrap()
        .insert(root_schema_name.into(), serde_json::to_value(mutable).unwrap());

    result
}

fn paths_map(
    request_schema_name: String,
    response_schema_name: String,
    method_name: String,
) -> PathsMap {
    let request_body = okapi::openapi3::RequestBody {
        required: true,
        content: {
            let mut map = okapi::Map::new();
            map.insert(
                "application/json".to_string(),
                okapi::openapi3::MediaType {
                    schema: Some(SchemaObject {
                        reference: Some(request_schema_name),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
            map
        },
        ..Default::default()
    };

    let mut responses = okapi::openapi3::Responses::default();
    responses.responses.insert(
        "200".to_string(),
        okapi::openapi3::Response {
            content: {
                let mut map = okapi::Map::new();
                map.insert(
                    "application/json".to_string(),
                    okapi::openapi3::MediaType {
                        schema: Some(SchemaObject {
                            reference: Some(response_schema_name),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                );
                map
            },
            ..Default::default()
        }
        .into(),
    );

    let operation = okapi::openapi3::Operation {
        operation_id: Some(method_name.clone()),
        request_body: Some(request_body.into()),
        responses,
        ..Default::default()
    };

    let mut paths = PathsMap::new();
    paths.insert(
        format!("/{}", method_name),
        okapi::openapi3::PathItem { post: Some(operation), ..Default::default() },
    );

    paths
}

fn add_spec_for_path_internal<RequestType: JsonSchema, ResponseType: JsonSchema>(
    all_schemas: &mut SchemasMap,
    all_paths: &mut PathsMap,
    method_name: String,
) {
    let mut request_map = schemas_map::<RequestType>();
    let response_map = schemas_map::<ResponseType>();

    let request_struct_name = format!("JsonRpcRequest_for_{}", method_name);
    let json_rpc_request = json!({
        "properties": {
            "id": {
                "type": "string"
            },
            "jsonrpc": {
                "type": "string"
            },
            "method": {
                "enum": [
                    method_name
                ],
                "type": "string"
            },
            "params": {
                "$ref": format!("#/components/schemas/{}", RequestType::schema_name())
            }
        },
        "required": [
            "jsonrpc",
            "id",
            "params",
            "method"
        ],
        "title": request_struct_name,
        "type": "object"
    });
    if let Some(obj) = request_map.as_object_mut() {
        obj.insert(request_struct_name.clone(), json_rpc_request);
    }

    let mut schemas = request_map;
    okapi::merge::merge_map_json(&mut schemas, response_map, "name");

    let paths = paths_map(
        format!("#/components/schemas/{}", request_struct_name),
        format!("#/components/schemas/{}", ResponseType::schema_name()),
        method_name,
    );

    okapi::merge::merge_map_json(all_schemas, schemas.clone(), "name");
    all_paths.extend(paths);
}

fn add_spec_for_path<Request: JsonSchema, Response: JsonSchema>(
    all_schemas: &mut SchemasMap,
    all_paths: &mut PathsMap,
    method_name: String,
) {
    add_spec_for_path_internal::<Request, JsonRpcResponse<Response, RpcError>>(
        all_schemas,
        all_paths,
        method_name,
    )
}

fn whole_spec(all_schemas: SchemasMap, all_paths: PathsMap) -> OpenApi {
    OpenApi {
        openapi: "3.0.0".to_string(),
        info: okapi::openapi3::Info {
            title: "NEAR Protocol JSON RPC API".to_string(),
            version: "1.0.0".to_string(),
            ..Default::default()
        },
        paths: all_paths,
        components: Some(okapi::openapi3::Components {
            schemas: all_schemas,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[derive(JsonSchema)]
struct RpcHealthRequest;

#[derive(JsonSchema)]
struct RpcStatusRequest;

#[derive(JsonSchema)]
struct RpcNetworkInfoRequest;

#[derive(JsonSchema)]
struct RpcClientConfigRequest;

#[derive(JsonSchema)]
struct GenesisConfigRequest;

fn main() {
    let mut all_schemas = json!({});
    let mut all_paths = PathsMap::new();

    add_spec_for_path::<RpcBlockRequest, RpcBlockResponse>(
        &mut all_schemas,
        &mut all_paths,
        "block".to_string(),
    );
    add_spec_for_path::<RpcSendTransactionRequest, CryptoHash>(
        &mut all_schemas,
        &mut all_paths,
        "broadcast_tx_async".to_string(),
    );
    add_spec_for_path::<RpcSendTransactionRequest, RpcTransactionResponse>(
        &mut all_schemas,
        &mut all_paths,
        "broadcast_tx_commit".to_string(),
    );
    add_spec_for_path::<RpcChunkRequest, RpcChunkResponse>(
        &mut all_schemas,
        &mut all_paths,
        "chunk".to_string(),
    );
    add_spec_for_path::<RpcGasPriceRequest, RpcGasPriceResponse>(
        &mut all_schemas,
        &mut all_paths,
        "gas_price".to_string(),
    );
    add_spec_for_path::<RpcHealthRequest, Option<RpcHealthResponse>>(
        &mut all_schemas,
        &mut all_paths,
        "health".to_string(),
    );
    add_spec_for_path::<RpcLightClientExecutionProofRequest, RpcLightClientExecutionProofResponse>(
        &mut all_schemas,
        &mut all_paths,
        "light_client_proof".to_string(),
    );
    add_spec_for_path::<RpcLightClientNextBlockRequest, RpcLightClientNextBlockResponse>(
        &mut all_schemas,
        &mut all_paths,
        "next_light_client_block".to_string(),
    );
    add_spec_for_path::<RpcNetworkInfoRequest, RpcNetworkInfoResponse>(
        &mut all_schemas,
        &mut all_paths,
        "network_info".to_string(),
    );
    add_spec_for_path::<RpcSendTransactionRequest, RpcTransactionResponse>(
        &mut all_schemas,
        &mut all_paths,
        "send_tx".to_string(),
    );
    add_spec_for_path::<RpcStatusRequest, RpcStatusResponse>(
        &mut all_schemas,
        &mut all_paths,
        "status".to_string(),
    );
    add_spec_for_path::<RpcTransactionStatusRequest, RpcTransactionResponse>(
        &mut all_schemas,
        &mut all_paths,
        "tx".to_string(),
    );
    add_spec_for_path::<RpcValidatorRequest, RpcValidatorResponse>(
        &mut all_schemas,
        &mut all_paths,
        "validators".to_string(),
    );
    add_spec_for_path::<RpcClientConfigRequest, RpcClientConfigResponse>(
        &mut all_schemas,
        &mut all_paths,
        "client_config".to_string(),
    );
    add_spec_for_path::<RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_changes".to_string(),
    );
    add_spec_for_path::<RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockResponse>(
        &mut all_schemas,
        &mut all_paths,
        "changes".to_string(),
    );
    add_spec_for_path::<RpcStateChangesInBlockRequest, RpcStateChangesInBlockByTypeResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_changes_in_block".to_string(),
    );
    add_spec_for_path::<RpcCongestionLevelRequest, RpcCongestionLevelResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_congestion_level".to_string(),
    );
    add_spec_for_path::<GenesisConfigRequest, GenesisConfig>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_genesis_config".to_string(),
    );
    add_spec_for_path::<RpcLightClientExecutionProofRequest, RpcLightClientExecutionProofResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_light_client_proof".to_string(),
    );
    add_spec_for_path::<RpcLightClientBlockProofRequest, RpcLightClientBlockProofResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_light_client_block_proof".to_string(),
    );
    add_spec_for_path::<RpcProtocolConfigRequest, RpcProtocolConfigResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_protocol_config".to_string(),
    );
    add_spec_for_path::<RpcReceiptRequest, RpcReceiptResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_receipt".to_string(),
    );
    add_spec_for_path::<RpcTransactionStatusRequest, RpcTransactionResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_tx_status".to_string(),
    );
    add_spec_for_path::<RpcValidatorsOrderedRequest, RpcValidatorsOrderedResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_validators_ordered".to_string(),
    );
    add_spec_for_path::<RpcMaintenanceWindowsRequest, RpcMaintenanceWindowsResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_maintenance_windows".to_string(),
    );
    add_spec_for_path::<RpcSplitStorageInfoRequest, RpcSplitStorageInfoResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_split_storage_info".to_string(),
    );
    add_spec_for_path::<RpcQueryRequest, RpcQueryResponse>(
        &mut all_schemas,
        &mut all_paths,
        "query".to_string(),
    );

    let path_schema = whole_spec(all_schemas, all_paths);

    let spec_json = serde_json::to_string_pretty(&path_schema).unwrap();
    println!("{}", spec_json);
}
