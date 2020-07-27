#![no_main]
use actix::System;
use libfuzzer_sys::{arbitrary, fuzz_target};
use rust_base58::ToBase58;
use serde::ser::{Serialize, Serializer};
use serde_json::json;
use tokio;

use near_jsonrpc_test_utils as test_utils;

static mut NODE_ADDR: Option<String> = None;
static NODE_INIT: std::sync::Once = std::sync::Once::new();

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
enum JsonRpcRequest {
    Query(RpcQueryRequest),
    Block(RpcBlockRequest),
    Chunk(RpcChunkRequest),
    Tx(RpcTxStatusRequest),
    Validators(RpcValidatorsRequest),
    GasPrice(RpcGasPriceRequest),
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

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum Finality {
    Optimistic,
    NearFinal,
    Final,
}

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum RpcBlockRequest {
    BlockId(u64),
    Finality(Finality),
}

#[derive(Debug, arbitrary::Arbitrary)]
struct Base58String([u8; 32]);

#[derive(Debug, arbitrary::Arbitrary)]
struct Base64String([u8; 32]);

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(untagged, rename_all = "snake_case")]
enum RpcChunkRequest {
    ChunkHash([Base58String; 1]),
}

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(untagged)]
enum RpcTxStatusRequest {
    Transaction(Base58String, String),
}

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(untagged)]
enum RpcValidatorsRequest {
    BlockHash([Base58String; 1]),
}

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(untagged)]
enum RpcGasPriceRequest {
    BlockHash([Base58String; 1]),
}

#[derive(Debug, arbitrary::Arbitrary, serde::Serialize)]
#[serde(untagged)]
enum RpcBroadcastTx {
    SignedTransaction([Base64String; 1]),
}

impl JsonRpcRequest {
    fn json(&self) -> serde_json::Value {
        let mut request_data = serde_json::to_value(self).unwrap();
        let request_data_obj = request_data.as_object_mut().unwrap();
        request_data_obj.insert("jsonrpc".to_string(), json!("2.0"));
        request_data_obj.insert("id".to_string(), json!("dontcare"));
        request_data
    }
}

impl Serialize for Base58String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_newtype_struct("Base58String", &self.0.to_base58())
    }
}

impl Serialize for Base64String {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_newtype_struct("Base58String", &base64::encode(&self.0))
    }
}

lazy_static::lazy_static! {
    static ref RUNTIME: std::sync::Mutex<tokio::runtime::Runtime> = {
        std::sync::Mutex::new(
            tokio::runtime::Builder::new()
                .basic_scheduler()
                .threaded_scheduler()
                .enable_all()
                .build()
                .unwrap(),
        )
    };
}

fuzz_target!(|requests: Vec<JsonRpcRequest>| {
    NODE_INIT.call_once(|| {
        std::thread::spawn(|| {
            System::run(|| {
                let (_view_client_addr, addr) =
                    test_utils::start_all(test_utils::NodeType::NonValidator);
                unsafe { NODE_ADDR = Some(addr) }
            })
            .unwrap();
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
            let post_data = request.json();
            eprintln!("POST DATA: {:?} | {}", post_data, post_data.to_string());
            let client = reqwest::Client::new();
            let response = client
                .post(&format!("http://{}", unsafe { NODE_ADDR.as_ref().unwrap() }))
                .json(&post_data)
                .send()
                .await
                .unwrap();
            if response.status() != 200 {
                return false;
            }
            let result_or_error: serde_json::Value = response.json().await.unwrap();
            eprintln!("RESPONSE: {:#?}", result_or_error);
            assert!(result_or_error["error"] != serde_json::json!(null));
        }
        true
    });
});
