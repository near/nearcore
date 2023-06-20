use near_primitives::num_rational::Rational32;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ChainConfig {
    /// Protocol treasury rate
    pub protocol_reward_rate: Rational32,
}
