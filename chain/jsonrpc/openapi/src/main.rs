use itertools::Itertools;
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
use near_jsonrpc_primitives::{
    errors::RpcError,
    types::{
        changes::RpcStateChangesInBlockResponse, light_client::RpcLightClientExecutionProofRequest,
    },
};

use near_primitives::hash::CryptoHash;

use near_chain_configs::GenesisConfig;

#[derive(JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResponseEither<T, E> {
    Result(T),
    Error(E),
}

#[derive(JsonSchema)]
#[allow(dead_code)]
#[schemars(rename = "JsonRpcResponse_for_{T}_and_{E}")]
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

/// This struct is used to interchange `oneOf` and `allOf` in the OpenAPI schema. And also adds titles to the `allOf` schemas.
/// If you have allOf [oneOf[A, B], oneOf[C, D]] it will transform it into: oneOf [allOf[A, C], allOf[A, D], allOf[B, C], allOf[B, D]].
///
/// For example, it will transform the following schema:
/// ```json
///   "RpcQueryRequest": {
///     "allOf": [
///       {
///         "oneOf": [
///           {
///             "properties": {
///               "block_id": { "$ref": "#/components/schemas/BlockId" }
///             },
///             "required": ["block_id"],
///             "type": "object"
///           },
///           {
///             "properties": {
///               "finality": { "$ref": "#/components/schemas/Finality" }
///             },
///             "required": ["finality"],
///             "type": "object"
///           }
///         ]
///       },
///       {
///         "oneOf": [
///           {
///             "properties": {
///               "request_type": {
///                 "type": "string",
///                 "enum": ["view_account"]
///               }
///             },
///             "required": ["request_type"],
///             "type": "object"
///           },
///           {
///             "properties": {
///               "request_type": {
///                 "type": "string",
///                 "enum": ["view_code"]
///               }
///             },
///             "required": ["request_type"],
///             "type": "object"
///           }
///         ]
///       }
///     ],
///     "title": "RpcQueryRequest",
///     "type": "object"
///   }
/// ```
/// into the following schema:
/// ```json
///   "RpcQueryRequest": {
///     "oneOf": [
///       {
///         "allOf": [
///           {
///             "properties": {
///               "block_id": {
///                 "$ref": "#/components/schemas/BlockId"
///               }
///             },
///             "required": ["block_id"],
///             "type": "object"
///           },
///           {
///             "properties": {
///               "request_type": {
///                 "type": "string",
///                 "enum": ["view_account"]
///               }
///             },
///             "required": ["request_type"],
///             "type": "object"
///           }
///         ],
///         "title": "view_account_by_block_id"
///       },
///       {
///         "allOf": [
///           {
///             "properties": {
///               "block_id": {
///                 "$ref": "#/components/schemas/BlockId"
///               }
///             },
///             "required": ["block_id"],
///             "type": "object"
///           },
///           {
///             "properties": {
///               "request_type": {
///                 "type": "string",
///                 "enum": ["view_code"]
///               }
///             },
///             "required": ["request_type"],
///             "type": "object"
///           }
///         ],
///         "title": "view_code_by_block_id"
///       },
///       {
///         "allOf": [
///           {
///             "properties": {
///               "finality": {
///                 "$ref": "#/components/schemas/Finality"
///               }
///             },
///             "required": ["finality"],
///             "type": "object"
///           },
///           {
///             "properties": {
///               "request_type": {
///                 "type": "string",
///                 "enum": ["view_account"]
///               }
///             },
///             "required": ["request_type"],
///             "type": "object"
///           }
///         ],
///         "title": "view_account_by_finality"
///       },
///       {
///         "allOf": [
///           {
///             "properties": {
///               "finality": {
///                 "$ref": "#/components/schemas/Finality"
///               }
///             },
///             "required": ["finality"],
///             "type": "object"
///           },
///           {
///             "properties": {
///               "request_type": {
///                 "type": "string",
///                 "enum": ["view_code"]
///               }
///             },
///             "required": ["request_type"],
///             "type": "object"
///           }
///         ],
///         "title": "view_code_by_finality"
///       }
///     ],
///     "type": "object",
///     "title": "RpcQueryRequest"
///   }
#[derive(Debug, Clone)]
pub struct InterchangeOneOfsAndAllOfs;

impl schemars::transform::Transform for InterchangeOneOfsAndAllOfs {
    fn transform(&mut self, schema: &mut schemars::Schema) {
        interchange_one_ofs_and_all_ofs(
            schema,
            "RpcStateChangesInBlockByTypeRequest".to_string(),
            "changes_type".to_string(),
        );
        interchange_one_ofs_and_all_ofs(
            schema,
            "RpcQueryRequest".to_string(),
            "request_type".to_string(),
        );
    }
}

/// Adds a title to the `allOf` object based on the first property of the first object and the enum value of the second object.
/// For example, for the following object, the title will be `view_code_by_finality`:
///            "allOf": [
///              {
///                "properties": {
///                  "finality": { "$ref": "#/components/schemas/Finality" }
///                },
///                "required": [
///                  "finality"
///                ],
///                "type": "object"
///              },
///              {
///                "properties": {
///                  "request_type": {
///                    "enum": [ "view_code" ],
///                    "type": "string"
///                  }
///                },
///                "required": [
///                  "request_type",
///                ],
///                "type": "object"
///              }
///            ],
///          },
fn add_title_to_allof(
    allof_obj: &mut serde_json::Map<String, serde_json::Value>,
    enum_name: String,
) {
    if let Some(serde_json::Value::Array(all_of)) = allof_obj.get_mut("allOf") {
        let mut enum_value: Option<String> = None;
        let mut other_props: Vec<String> = Vec::new();

        for item in all_of {
            if let serde_json::Value::Object(item_obj) = item {
                if let Some(serde_json::Value::Object(props)) = item_obj.get("properties") {
                    if let Some(req_type_obj) = props.get(&enum_name).and_then(|v| v.as_object()) {
                        if let Some(serde_json::Value::Array(enum_arr)) = req_type_obj.get("enum") {
                            if let Some(serde_json::Value::String(s)) = enum_arr.get(0) {
                                enum_value = Some(s.clone());
                            }
                        }
                    } else {
                        if let Some((first_prop_name, _)) = props.iter().next() {
                            other_props.push(first_prop_name.clone());
                        }
                    }
                }
            }
        }

        if let Some(enum_val) = enum_value {
            let title = format!("{}_by_{}", enum_val, other_props.join("_and_"));
            allof_obj.insert("title".to_string(), serde_json::Value::String(title));
        }
    }
}

/// Interchanges `oneOf` and `allOf` in the schema for InterchangeOneOfsAndAllOfs transform
fn interchange_one_ofs_and_all_ofs(
    schema: &mut schemars::Schema,
    title_value: String,
    enum_name: String,
) {
    if let Some(value) = schema.get("title") {
        if value == title_value.as_str() {
            match serde_json::to_value(schema.clone()).unwrap() {
                serde_json::Value::Object(mut map) => {
                    if let Some(serde_json::Value::Array(all_of)) = map.get_mut("allOf") {
                        let mut all_one_ofs = vec![];
                        for item in all_of {
                            if let serde_json::Value::Object(sub_obj) = item {
                                if let Some(serde_json::Value::Array(one_of)) = sub_obj.get("oneOf")
                                {
                                    all_one_ofs.push(one_of.clone());
                                }
                            }
                        }
                        let combinations = Itertools::multi_cartesian_product(
                            all_one_ofs.iter().map(|v: &Vec<serde_json::Value>| v.iter()),
                        );
                        let new_oneof: Vec<serde_json::Value> = combinations
                            .into_iter()
                            .map(|combo| {
                                let combo_vals: Vec<serde_json::Value> =
                                    combo.into_iter().cloned().collect();
                                let mut obj = serde_json::Map::new();
                                obj.insert(
                                    "allOf".to_string(),
                                    serde_json::Value::Array(combo_vals),
                                );
                                add_title_to_allof(&mut obj, enum_name.clone());
                                serde_json::Value::Object(obj)
                            })
                            .collect();
                        schema.remove("allOf");
                        schema.insert("oneOf".to_string(), serde_json::Value::Array(new_oneof));
                    }
                }
                _ => {}
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
    settings.transforms.push(Box::new(InterchangeOneOfsAndAllOfs));
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
    doc: String,
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
        description: Some(doc),
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
    doc: String,
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
        doc,
    );

    okapi::merge::merge_map_json(all_schemas, schemas.clone(), "name");
    all_paths.extend(paths);
}

fn add_spec_for_path<Request: JsonSchema, Response: JsonSchema>(
    all_schemas: &mut SchemasMap,
    all_paths: &mut PathsMap,
    method_name: String,
    doc: String,
) {
    add_spec_for_path_internal::<Request, JsonRpcResponse<Response, RpcError>>(
        all_schemas,
        all_paths,
        method_name,
        doc,
    )
}

fn whole_spec(all_schemas: SchemasMap, all_paths: PathsMap) -> OpenApi {
    OpenApi {
        openapi: "3.0.0".to_string(),
        info: okapi::openapi3::Info {
            title: "NEAR Protocol JSON RPC API".to_string(),
            version: "1.1.1".to_string(),
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
        "Returns block details for given height or hash".to_string(),
    );
    add_spec_for_path::<RpcSendTransactionRequest, CryptoHash>(
        &mut all_schemas,
        &mut all_paths,
        "broadcast_tx_async".to_string(),
        "[Deprecated] Sends a transaction and immediately returns transaction hash. Consider using send_tx instead".to_string(),
    );
    add_spec_for_path::<RpcSendTransactionRequest, RpcTransactionResponse>(
        &mut all_schemas,
        &mut all_paths,
        "broadcast_tx_commit".to_string(),
        "[Deprecated] Sends a transaction and waits until transaction is fully complete. (Has a 10 second timeout). Consider using send_tx instead".to_string(),
    );
    add_spec_for_path::<RpcChunkRequest, RpcChunkResponse>(
        &mut all_schemas,
        &mut all_paths,
        "chunk".to_string(),
        "Returns details of a specific chunk. You can run a block details query to get a valid chunk hash.".to_string(),
    );
    add_spec_for_path::<RpcGasPriceRequest, RpcGasPriceResponse>(
        &mut all_schemas,
        &mut all_paths,
        "gas_price".to_string(),
        "Returns gas price for a specific block_height or block_hash. Using [null] will return the most recent block's gas price.".to_string(),
    );
    add_spec_for_path::<RpcHealthRequest, Option<RpcHealthResponse>>(
        &mut all_schemas,
        &mut all_paths,
        "health".to_string(),
        "Returns the current health status of the RPC node the client connects to.".to_string(),
    );
    add_spec_for_path::<RpcLightClientExecutionProofRequest, RpcLightClientExecutionProofResponse>(
        &mut all_schemas,
        &mut all_paths,
        "light_client_proof".to_string(),
        "Returns the proofs for a transaction execution.".to_string(),
    );
    add_spec_for_path::<RpcLightClientNextBlockRequest, RpcLightClientNextBlockResponse>(
        &mut all_schemas,
        &mut all_paths,
        "next_light_client_block".to_string(),
        "Returns the next light client block.".to_string(),
    );
    add_spec_for_path::<RpcNetworkInfoRequest, RpcNetworkInfoResponse>(
        &mut all_schemas,
        &mut all_paths,
        "network_info".to_string(),
        "Queries the current state of node network connections. This includes information about active peers, transmitted data, known producers, etc.".to_string(),
    );
    add_spec_for_path::<RpcSendTransactionRequest, RpcTransactionResponse>(
        &mut all_schemas,
        &mut all_paths,
        "send_tx".to_string(),
        "Sends transaction. Returns the guaranteed execution status and the results the blockchain can provide at the moment.".to_string(),
    );
    add_spec_for_path::<RpcStatusRequest, RpcStatusResponse>(
        &mut all_schemas,
        &mut all_paths,
        "status".to_string(),
        "Requests the status of the connected RPC node. This includes information about sync status, nearcore node version, protocol version, the current set of validators, etc.".to_string(),
    );
    add_spec_for_path::<RpcTransactionStatusRequest, RpcTransactionResponse>(
        &mut all_schemas,
        &mut all_paths,
        "tx".to_string(),
        "Queries status of a transaction by hash and returns the final transaction result."
            .to_string(),
    );
    add_spec_for_path::<RpcValidatorRequest, RpcValidatorResponse>(
        &mut all_schemas,
        &mut all_paths,
        "validators".to_string(),
        "Queries active validators on the network. Returns details and the state of validation on the blockchain.".to_string(),
    );
    add_spec_for_path::<RpcClientConfigRequest, RpcClientConfigResponse>(
        &mut all_schemas,
        &mut all_paths,
        "client_config".to_string(),
        "Queries client node configuration".to_string(),
    );
    add_spec_for_path::<RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_changes".to_string(),
        "Returns changes in block for given block height or hash over all transactions for the current type. Includes changes like account_touched, access_key_touched, data_touched, contract_code_touched".to_string(),
    );
    add_spec_for_path::<RpcStateChangesInBlockByTypeRequest, RpcStateChangesInBlockResponse>(
        &mut all_schemas,
        &mut all_paths,
        "changes".to_string(),
        "Returns changes in block for given block height or hash over all transactions for the current type. Includes changes like account_touched, access_key_touched, data_touched, contract_code_touched".to_string(),
    );
    add_spec_for_path::<RpcStateChangesInBlockRequest, RpcStateChangesInBlockByTypeResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_changes_in_block".to_string(),
        "Returns changes in block for given block height or hash over all transactions for all the types. Includes changes like account_touched, access_key_touched, data_touched, contract_code_touched".to_string(),
    );
    add_spec_for_path::<RpcCongestionLevelRequest, RpcCongestionLevelResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_congestion_level".to_string(),
        "Queries the congestion level of a shard. More info about congestion [here](https://near.github.io/nearcore/architecture/how/receipt-congestion.html?highlight=congestion#receipt-congestion)".to_string(),
    );
    add_spec_for_path::<GenesisConfigRequest, GenesisConfig>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_genesis_config".to_string(),
        "Get initial state and parameters for the genesis block".to_string(),
    );
    add_spec_for_path::<RpcLightClientExecutionProofRequest, RpcLightClientExecutionProofResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_light_client_proof".to_string(),
        "Returns the proofs for a transaction execution.".to_string(),
    );
    add_spec_for_path::<RpcLightClientBlockProofRequest, RpcLightClientBlockProofResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_light_client_block_proof".to_string(),
        "Returns the proofs for a transaction execution.".to_string(),
    );
    add_spec_for_path::<RpcProtocolConfigRequest, RpcProtocolConfigResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_protocol_config".to_string(),
        "A configuration that defines the protocol-level parameters such as gas/storage costs, limits, feature flags, other settings".to_string(),
    );
    add_spec_for_path::<RpcReceiptRequest, RpcReceiptResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_receipt".to_string(),
        "Fetches a receipt by its ID (as is, without a status or execution outcome)".to_string(),
    );
    add_spec_for_path::<RpcTransactionStatusRequest, RpcTransactionResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_tx_status".to_string(),
        "Queries status of a transaction by hash, returning the final transaction result and details of all receipts.".to_string(),
    );
    add_spec_for_path::<RpcValidatorsOrderedRequest, RpcValidatorsOrderedResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_validators_ordered".to_string(),
        "Returns the current epoch validators ordered in the block producer order with repetition. This endpoint is solely used for bridge currently and is not intended for other external use cases.".to_string(),
    );
    add_spec_for_path::<RpcMaintenanceWindowsRequest, RpcMaintenanceWindowsResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_maintenance_windows".to_string(),
        "Returns the future windows for maintenance in current epoch for the specified account. In the maintenance windows, the node will not be block producer or chunk producer".to_string(),
    );
    add_spec_for_path::<RpcSplitStorageInfoRequest, RpcSplitStorageInfoResponse>(
        &mut all_schemas,
        &mut all_paths,
        "EXPERIMENTAL_split_storage_info".to_string(),
        "Contains the split storage information. More info on split storage [here](https://near-nodes.io/archival/split-storage-archival)".to_string(),
    );
    add_spec_for_path::<RpcQueryRequest, RpcQueryResponse>(
        &mut all_schemas,
        &mut all_paths,
        "query".to_string(),
        "This module allows you to make generic requests to the network.

The `RpcQueryRequest` struct takes in a [`BlockReference`](https://docs.rs/near-primitives/0.12.0/near_primitives/types/enum.BlockReference.html) and a [`QueryRequest`](https://docs.rs/near-primitives/0.12.0/near_primitives/views/enum.QueryRequest.html).

The `BlockReference` enum allows you to specify a block by `Finality`, `BlockId` or `SyncCheckpoint`.

The `QueryRequest` enum provides multiple variants for performing the following actions:
 - View an account's details
 - View a contract's code
 - View the state of an account
 - View the `AccessKey` of an account
 - View the `AccessKeyList` of an account
 - Call a function in a contract deployed on the network.".to_string(),
    );

    let path_schema = whole_spec(all_schemas, all_paths);

    let spec_json = serde_json::to_string_pretty(&path_schema).unwrap();
    println!("{}", spec_json);
}
