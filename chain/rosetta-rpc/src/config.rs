use crate::models::Currency;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RosettaRpcConfig {
    pub(crate) addr: String,
    pub(crate) cors_allowed_origins: Vec<String>,
    #[serde(default)]
    pub(crate) limits: RosettaRpcLimitsConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) currencies: Option<Vec<Currency>>,
}

impl Default for RosettaRpcConfig {
    fn default() -> Self {
        Self {
            addr: "0.0.0.0:3040".to_owned(),
            cors_allowed_origins: vec!["*".to_owned()],
            limits: RosettaRpcLimitsConfig::default(),
            currencies: None,
        }
    }
}

impl RosettaRpcConfig {
    pub(crate) fn new(addr: &str) -> Self {
        Self { addr: addr.to_owned(), ..Default::default() }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct RosettaRpcLimitsConfig {
    pub(crate) input_payload_max_size: usize,
}

impl Default for RosettaRpcLimitsConfig {
    fn default() -> Self {
        Self { input_payload_max_size: 10 * 1024 * 1024 }
    }
}
