use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::Gas;
use near_schema_checker_lib::ProtocolSchema;
use std::ops::{Add, AddAssign, Div};

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
pub enum ShardSizeContribution {
    V1(ShardSizeContributionV1),
}

impl Default for ShardSizeContribution {
    fn default() -> Self {
        Self::V1(ShardSizeContributionV1::default())
    }
}

impl Add<Self> for ShardSizeContribution {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::V1(lhs), Self::V1(rhs)) => Self::V1(lhs + rhs),
        }
    }
}

impl AddAssign<Self> for ShardSizeContribution {
    fn add_assign(&mut self, rhs: Self) {
        match (self, rhs) {
            (Self::V1(lhs), Self::V1(rhs)) => *lhs += rhs,
        }
    }
}

impl From<ShardSizeContributionV1> for ShardSizeContribution {
    fn from(value: ShardSizeContributionV1) -> Self {
        Self::V1(value)
    }
}

impl Div<u64> for ShardSizeContribution {
    type Output = Self;
    fn div(self, rhs: u64) -> Self::Output {
        match self {
            ShardSizeContribution::V1(lhs) => (lhs / rhs).into(),
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
pub struct ShardSizeContributionV1 {
    gas_usage: Gas,
}

impl Add<Self> for ShardSizeContributionV1 {
    type Output = Self;
    fn add(self, rhs: Self) -> Self::Output {
        Self { gas_usage: self.gas_usage.saturating_add(rhs.gas_usage) }
    }
}

impl AddAssign<Self> for ShardSizeContributionV1 {
    fn add_assign(&mut self, rhs: Self) {
        self.gas_usage += rhs.gas_usage;
    }
}

impl From<Gas> for ShardSizeContributionV1 {
    fn from(gas: Gas) -> Self {
        Self { gas_usage: gas }
    }
}

impl Div<u64> for ShardSizeContributionV1 {
    type Output = Self;
    fn div(mut self, rhs: u64) -> Self::Output {
        self.gas_usage /= rhs;
        self
    }
}
