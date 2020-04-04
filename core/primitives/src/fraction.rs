use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::ops::Mul;

/// Represents a rational number. This struct is used exclusively in configs so
/// `u32` provides enough precision. Since we do not expect to have computation
/// that returns a `Fraction`, there is no guarantee that it is an irreducible fraction.
#[derive(Debug, Serialize, Deserialize, Hash, PartialEq, Eq, Clone, Copy)]
pub struct Fraction {
    pub numerator: u32,
    pub denominator: u32,
}

impl Fraction {
    pub fn new(numerator: u32, denominator: u32) -> Self {
        Fraction { numerator, denominator }
    }

    pub fn zero() -> Self {
        Fraction { numerator: 0, denominator: 1 }
    }
}

impl Default for Fraction {
    fn default() -> Self {
        Self::zero()
    }
}

impl Mul<u128> for Fraction {
    type Output = u128;

    fn mul(self, rhs: u128) -> Self::Output {
        rhs * u128::from(self.numerator) / u128::from(self.denominator)
    }
}

impl Mul<Fraction> for u128 {
    type Output = u128;

    fn mul(self, rhs: Fraction) -> Self::Output {
        rhs * self
    }
}
