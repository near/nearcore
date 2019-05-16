pub mod b64_format {
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::serialize::{from_base64, to_base64};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = to_base64(&bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        from_base64(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ABCIQueryResponse {
    pub code: u32,
    pub log: String,
    pub info: String,
    pub index: i64,
    #[serde(with = "b64_format")]
    pub key: Vec<u8>,
    #[serde(with = "b64_format")]
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
