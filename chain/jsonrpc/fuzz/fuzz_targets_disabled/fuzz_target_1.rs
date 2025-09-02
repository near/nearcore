#![no_main]
use actix::System;
use libfuzzer_sys::{arbitrary, fuzz_target};
use serde::ser::{Serialize, Serializer};
use serde_json::json;
use tokio;

use near_jsonrpc_primitives::types;
use near_jsonrpc_tests as test_utils;
use near_primitives::hash::CryptoHash;
use near_primitives::types::Finality;

static mut NODE_ADDR: Option<String> = None;
static NODE_INIT: std::sync::Once = std::sync::Once::new();

#[derive(Debug, arbitrary::Arbitrary)]
enum JsonRpcRequest {
    Query(RpcQueryRequest),
    Block(types::blocks::RpcBlockRequest),
    Chunk(types::chunks::RpcChunkRequest),
    Tx(RpcTxStatusRequest),
    Validators(types::validator::RpcValidatorRequest),
    GasPrice(types::gas_price::RpcGasPriceRequest),
    BroadcastTxAsync(RpcBroadcastTx),
    BroadcastTxCommit(RpcBroadcastTx),
}

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(tag = "request_type", rename_all = "snake_case")]
enum RpcQueryRequest {
    ViewAccount {
        finality: Finality,
        account_id: String,
    },
    ViewState {
        finality: Finality,
        account_id: String,
        prefix_base64: String,
    },
    ViewAccessKey {
        finality: Finality,
        account_id: String,
        public_key: String,
    },
    ViewAccessKeyList {
        finality: Finality,
        account_id: String,
    },
    CallFunction {
        finality: Finality,
        account_id: String,
        method_name: String,
        args_base64: String,
    },
}

#[derive(Debug, arbitrary::Arbitrary)]
struct Base64String([u8; 32]);

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(untagged)]
enum RpcTxStatusRequest {
    Transaction(CryptoHash, String),
}

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(untagged)]
enum RpcBroadcastTx {
    SignedTransaction([Base64String; 1]),
}

impl JsonRpcRequest {
    fn method_and_params(&self) -> (&str, serde_json::Value) {
        match self {
            JsonRpcRequest::Query(request) => ("query", json!(request)),
            JsonRpcRequest::Block(request) => ("block", json!(request)),
            JsonRpcRequest::Chunk(request) => ("chunk", json!(request)),
            JsonRpcRequest::Tx(request) => ("tx", json!(request)),
            JsonRpcRequest::Validators(request) => ("validators", json!(request)),
            JsonRpcRequest::GasPrice(request) => ("gas_price", json!(request)),
            JsonRpcRequest::BroadcastTxAsync(request) => ("broadcast_tx_async", json!(request)),
            JsonRpcRequest::BroadcastTxCommit(request) => ("broadcast_tx_commit", json!(request)),
        }
    }
}

impl Serialize for Base64String {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let encoded = near_primitives::serialize::to_base64(&self.0);
        serializer.serialize_str(&encoded)
    }
}

/// Simple call_method function for the fuzz target
async fn call_method<T>(
    client: &reqwest::Client,
    server_addr: &str,
    method: &str,
    params: serde_json::Value,
) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": "test",
        "method": method,
        "params": params
    });

    let response =
        client.post(server_addr).json(&request).send().await.map_err(|e| e.to_string())?;

    let json: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;

    if let Some(result) = json.get("result") {
        serde_json::from_value(result.clone()).map_err(|e| e.to_string())
    } else if let Some(error) = json.get("error") {
        Err(format!("RPC Error: {}", error))
    } else {
        Err("Invalid JSON-RPC response".to_string())
    }
}

static RUNTIME: std::sync::LazyLock<parking_lot::Mutex<tokio::runtime::Runtime>> =
    std::sync::LazyLock::new(|| {
        parking_lot::Mutex::new(
            tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap(),
        )
    });

fuzz_target!(|requests: Vec<JsonRpcRequest>| {
    NODE_INIT.call_once(|| {
        std::thread::spawn(|| {
            System::new().block_on(async {
                let setup = test_utils::create_test_setup_with_node_type(
                    test_utils::NodeType::NonValidator,
                );
                unsafe { NODE_ADDR = Some(setup.server_addr.to_string()) }
            });
        });
    });

    for _ in 1..30 {
        unsafe {
            #![allow(static_mut_refs)]
            if let Some(_node_addr) = NODE_ADDR.as_ref() {
                break;
            } else {
                std::thread::sleep(std::time::Duration::from_millis(100)); // ensure node have enough time to start
            }
        }
    }

    RUNTIME.lock().block_on(async move {
        for request in requests {
            let (method, params) = request.method_and_params();
            eprintln!("POST DATA: {{method = {}}} {{params = {}}}", method, params);

            let client = reqwest::Client::new();
            let addr = unsafe {
                #[allow(static_mut_refs)]
                NODE_ADDR.as_ref().unwrap().to_string()
            };
            let result_or_error =
                call_method::<serde_json::Value>(&client, &addr, method, params).await.unwrap();
            eprintln!("RESPONSE: {:#?}", result_or_error);
            assert!(result_or_error["error"] != serde_json::json!(null));
        }
        true
    });
});
