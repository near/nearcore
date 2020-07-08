#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RosettaRpcConfig {
    pub addr: String,
    pub cors_allowed_origins: Vec<String>,
}

impl Default for RosettaRpcConfig {
    fn default() -> Self {
        Self { addr: "0.0.0.0:3040".to_owned(), cors_allowed_origins: vec!["*".to_owned()] }
    }
}

impl RosettaRpcConfig {
    pub fn new(addr: &str) -> Self {
        Self { addr: addr.to_owned(), ..Default::default() }
    }
}
