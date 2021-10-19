use std::cmp::Ordering;
use std::time::Duration;
use std::{fmt, ops};

use near_primitives::types::Gas;
use num_rational::Ratio;

use crate::cases::ratio_to_gas;
use crate::testbed_runners::{end_count, start_count, Consumed, GasMetric};

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GasCost {
    /// The smallest thing we are measuring is one wasm instruction, and it
    /// takes about a nanosecond, so we do need to account for fractional
    /// nanoseconds here!
    value: Ratio<u64>,
    metric: GasMetric,
}

pub(crate) struct GasClock {
    start: Consumed,
    metric: GasMetric,
}

impl GasCost {
    pub(crate) fn from_raw(raw: Ratio<u64>, metric: GasMetric) -> GasCost {
        GasCost { value: raw, metric }
    }

    pub(crate) fn measure(metric: GasMetric) -> GasClock {
        let start = start_count(metric);
        GasClock { start, metric }
    }
    pub(crate) fn zero(metric: GasMetric) -> GasCost {
        GasCost { value: 0.into(), metric }
    }
}

impl GasClock {
    pub(crate) fn elapsed(self) -> GasCost {
        let measured = end_count(self.metric, &self.start);
        GasCost { value: measured.into(), metric: self.metric }
    }
}

impl fmt::Debug for GasCost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.metric {
            GasMetric::ICount => write!(f, "{}i", self.value),
            GasMetric::Time => fmt::Debug::fmt(&Duration::from_nanos(self.value.to_integer()), f),
        }
    }
}

impl ops::Add for GasCost {
    type Output = GasCost;

    fn add(self, rhs: GasCost) -> Self::Output {
        assert_eq!(self.metric, rhs.metric);
        GasCost { value: self.value + rhs.value, metric: self.metric }
    }
}

impl ops::AddAssign for GasCost {
    fn add_assign(&mut self, rhs: GasCost) {
        *self = self.clone() + rhs;
    }
}

impl ops::Sub for GasCost {
    type Output = GasCost;

    fn sub(self, rhs: GasCost) -> Self::Output {
        assert_eq!(self.metric, rhs.metric);
        GasCost { value: self.value - rhs.value, metric: self.metric }
    }
}

impl ops::Mul<u64> for GasCost {
    type Output = GasCost;

    fn mul(self, rhs: u64) -> Self::Output {
        GasCost { value: self.value * rhs, metric: self.metric }
    }
}

impl ops::Div<u64> for GasCost {
    type Output = GasCost;

    fn div(self, rhs: u64) -> Self::Output {
        GasCost { value: self.value / rhs, metric: self.metric }
    }
}

impl PartialOrd for GasCost {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GasCost {
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl GasCost {
    pub(crate) fn to_gas(self) -> Gas {
        ratio_to_gas(self.metric, self.value)
    }
}
