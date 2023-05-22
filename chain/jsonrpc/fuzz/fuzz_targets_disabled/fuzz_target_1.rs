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

static RUNTIME: once_cell::sync::Lazy<std::sync::Mutex<tokio::runtime::Runtime>> =
    once_cell::sync::Lazy::new(|| {
        std::sync::Mutex::new(
            tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap(),
        )
    });

fuzz_target!(|requests: Vec<JsonRpcRequest>| {
    NODE_INIT.call_once(|| {
        std::thread::spawn(|| {
            System::new().block_on(async {
                let (_view_client_addr, addr) =
                    test_utils::start_all(test_utils::NodeType::NonValidator);
                unsafe { NODE_ADDR = Some(addr.to_string()) }
            });
        });
    });

    for _ in 1..30 {
        if let Some(_node_addr) = unsafe { NODE_ADDR.as_ref() } {
            break;
        } else {
            std::thread::sleep(std::time::Duration::from_millis(100)); // ensure node have enough time to start
        }
    }

    RUNTIME.lock().unwrap().block_on(async move {
        for request in requests {
            let (method, params) = request.method_and_params();
            eprintln!("POST DATA: {{method = {}}} {{params = {}}}", method, params);

            let client = awc::Client::new();
            let result_or_error = test_utils::call_method::<serde_json::Value>(
                &client,
                unsafe { NODE_ADDR.as_ref().unwrap() },
                method,
                params,
            )
            .await
            .unwrap();
            eprintln!("RESPONSE: {:#?}", result_or_error);
            assert!(result_or_error["error"] != serde_json::json!(null));
        }
        true
    });
});
