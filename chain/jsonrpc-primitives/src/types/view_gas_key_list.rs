#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewGasKeyListRequest {
    #[serde(flatten)]
    pub block_reference: near_primitives::types::BlockReference,
    pub account_id: near_primitives::types::AccountId,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct RpcViewGasKeyListResponse {
    #[serde(flatten)]
    pub gas_key_list: near_primitives::views::GasKeyList,
    pub block_height: near_primitives::types::BlockHeight,
    pub block_hash: near_primitives::hash::CryptoHash,
}
