#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RpcConfig {
    pub addr: String,
}

impl Default for RpcConfig {
    fn default() -> Self {
        RpcConfig { addr: "0.0.0.0:3030".to_owned() }
    }
}

impl RpcConfig {
    pub fn new(addr: &str) -> Self {
        RpcConfig { addr: addr.to_owned(), ..Default::default() }
    }
}
