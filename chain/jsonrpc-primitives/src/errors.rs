use serde::Serialize;

#[derive(Serialize)]
pub struct RpcParseError(pub String);
