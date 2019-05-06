#[derive(Serialize, Deserialize, Debug)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Vec<serde_json::Value>,
    pub id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcResponseError>,
    pub id: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonRpcResponseError {
    code: i64,
    message: String,
    data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ABCIQueryResponse {
    pub code: u32,
    pub log: String,
    pub info: String,
    pub index: i64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub proof: Vec<ProofOp>,
    pub height: i64,
    pub codespace: String,
}

impl ABCIQueryResponse {
    pub fn account<T: serde::Serialize>(key: &str, value: T) -> Self {
        ABCIQueryResponse {
            code: 0,
            log: "exists".to_string(),
            info: "".to_string(),
            index: -1,
            key: key.as_bytes().to_vec(),
            value: serde_json::to_string(&value).unwrap().as_bytes().to_vec(),
            proof: vec![],
            height: 0,
            codespace: "".to_string(),
        }
    }

    pub fn result(key: &str, value: Vec<u8>, logs: Vec<String>) -> Self {
        ABCIQueryResponse {
            code: 0,
            log: logs.join("\n"),
            info: "".to_string(),
            index: -1,
            key: key.as_bytes().to_vec(),
            value,
            proof: vec![],
            height: 0,
            codespace: "".to_string(),
        }
    }

    pub fn result_err(key: &str, message: String, logs: Vec<String>) -> Self {
        ABCIQueryResponse {
            code: 1,
            log: logs.join("\n"),
            info: message,
            index: -1,
            key: key.as_bytes().to_vec(),
            value: vec![],
            proof: vec![],
            height: 0,
            codespace: "".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ProofOp {
    pub field_type: String,
    pub key: Vec<u8>,
    pub data: Vec<u8>,
}
