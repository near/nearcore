#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChainConfig {
    /// Protocol treasury rate
    pub protocol_reward_rate: Rational32,
}
