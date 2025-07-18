use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::Gas;
use near_schema_checker_lib::ProtocolSchema;
use std::ops::{Add, AddAssign, Div};

/// Measures the contribution of a sub-trie associated with a given key
/// to the overall utilization of the shard to which it belongs.
/// It is used to trigger resharing and find the right account for a new
/// shard boundary.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    ProtocolSchema,
)]
pub enum ShardUtilization {
    V1(ShardUtilizationV1),
}

impl Default for ShardUtilization {
    fn default() -> Self {
        Self::V1(ShardUtilizationV1::default())
    }
}

impl Add<Self> for ShardUtilization {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::V1(lhs), Self::V1(rhs)) => Self::V1(lhs + rhs),
        }
    }
}

impl AddAssign<Self> for ShardUtilization {
    fn add_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (Self::V1(lhs), Self::V1(rhs)) => *lhs += rhs,
        }
    }
}

impl From<ShardUtilizationV1> for ShardUtilization {
    fn from(value: ShardUtilizationV1) -> Self {
        Self::V1(value)
    }
}

impl Div<u64> for ShardUtilization {
    type Output = Self;
    fn div(self, rhs: u64) -> Self::Output {
        match self {
            ShardUtilization::V1(lhs) => (lhs / rhs).into(),
        }
    }
}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    Copy,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    ProtocolSchema,
)]
pub struct ShardUtilizationV1 {
    gas_usage: Gas,
}

impl Add<Self> for ShardUtilizationV1 {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self { gas_usage: self.gas_usage.saturating_add(rhs.gas_usage) }
    }
}

impl AddAssign<Self> for ShardUtilizationV1 {
    fn add_assign(&mut self, rhs: Self) {
        self.gas_usage += rhs.gas_usage;
    }
}

impl From<Gas> for ShardUtilizationV1 {
    fn from(gas: Gas) -> Self {
        Self { gas_usage: gas }
    }
}

impl Div<u64> for ShardUtilizationV1 {
    type Output = Self;
    fn div(mut self, rhs: u64) -> Self::Output {
        self.gas_usage /= rhs;
        self
    }
}
