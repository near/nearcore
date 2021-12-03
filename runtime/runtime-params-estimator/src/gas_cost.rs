use std::cmp::Ordering;
use std::convert::TryFrom;
use std::time::{Duration, Instant};
use std::{fmt, ops};

use near_primitives::types::Gas;
use num_rational::Ratio;

use crate::testbed_runners::qemu::{
    end_count_instructions, start_count_instructions, QemuMeasurement,
};
use crate::testbed_runners::GasMetric;

/// Result of cost estimation.
///
/// Holds wall-clock time or number of instructions and can be converted to
/// `Gas`. `GasCost` can also be flagged as "uncertain" if we failed to
/// reproducibly measure it.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct GasCost {
    /// The smallest thing we are measuring is one wasm instruction, and it
    /// takes about a nanosecond, so we do need to account for fractional
    /// nanoseconds here!
    time_ns: Ratio<u64>,
    /// Value used for `GasMetric::ICount`
    qemu_measurement: QemuMeasurement,
    metric: GasMetric,
    /// Signals that the measurement was uncertain (ie, had high variance), and
    /// that the estimation needs to be re-run.
    ///
    /// Each specific cost can use it's own criteria for uncertainty -- the end
    /// result here is just printing UNCERTAIN next to the corresponding cost in
    /// the output.
    uncertain: bool,
}

pub(crate) struct GasClock {
    start: Option<Instant>,
    metric: GasMetric,
}

impl GasCost {
    pub(crate) fn zero(metric: GasMetric) -> GasCost {
        GasCost {
            metric,
            time_ns: 0.into(),
            qemu_measurement: QemuMeasurement::zero(),
            uncertain: false,
        }
    }

    pub(crate) fn measure(metric: GasMetric) -> GasClock {
        let start = match metric {
            GasMetric::ICount => {
                start_count_instructions();
                None
            }
            GasMetric::Time => Some(Instant::now()),
        };
        GasClock { start, metric }
    }

    /// Creates `GasCost` out of raw numeric value. This is required mostly for
    /// compatibility with existing code, prefer using `measure` instead.
    pub(crate) fn from_raw(raw: Ratio<u64>, metric: GasMetric) -> GasCost {
        let mut result = GasCost::zero(metric);
        match metric {
            GasMetric::ICount => result.qemu_measurement.instructions = raw,
            GasMetric::Time => result.time_ns = raw,
        }
        result
    }

    pub(crate) fn is_uncertain(&self) -> bool {
        self.uncertain
    }
    pub(crate) fn set_uncertain(&mut self, uncertain: bool) {
        self.uncertain = uncertain;
    }
}

impl GasClock {
    pub(crate) fn elapsed(self) -> GasCost {
        let mut result = GasCost::zero(self.metric);
        match self.metric {
            GasMetric::ICount => {
                result.qemu_measurement = end_count_instructions();
            }
            GasMetric::Time => {
                let ns: u64 = self
                    .start
                    .expect("instant not initialized")
                    .elapsed()
                    .as_nanos()
                    .try_into()
                    .unwrap();
                result.time_ns = ns.into();
            }
        }
        result
    }
}

impl fmt::Debug for GasCost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.metric {
            GasMetric::ICount => write!(f, "{:?}", self.qemu_measurement),
            GasMetric::Time => fmt::Debug::fmt(&Duration::from_nanos(self.time_ns.to_integer()), f),
        }
    }
}

impl ops::Add for GasCost {
    type Output = GasCost;

    fn add(self, rhs: GasCost) -> Self::Output {
        assert_eq!(self.metric, rhs.metric);
        let uncertain = self.uncertain || rhs.uncertain;
        GasCost {
            time_ns: self.time_ns + rhs.time_ns,
            qemu_measurement: self.qemu_measurement + rhs.qemu_measurement,
            metric: self.metric,
            uncertain,
        }
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
        let uncertain = self.uncertain || rhs.uncertain;
        GasCost {
            time_ns: self.time_ns - rhs.time_ns,
            qemu_measurement: self.qemu_measurement - rhs.qemu_measurement,
            metric: self.metric,
            uncertain,
        }
    }
}

impl ops::Mul<u64> for GasCost {
    type Output = GasCost;

    fn mul(self, rhs: u64) -> Self::Output {
        GasCost {
            time_ns: self.time_ns * rhs,
            qemu_measurement: self.qemu_measurement * rhs,
            ..self
        }
    }
}

impl ops::Div<u64> for GasCost {
    type Output = GasCost;

    fn div(self, rhs: u64) -> Self::Output {
        GasCost {
            time_ns: self.time_ns / rhs,
            qemu_measurement: self.qemu_measurement / rhs,
            ..self
        }
    }
}

impl PartialOrd for GasCost {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GasCost {
    fn cmp(&self, other: &Self) -> Ordering {
        self.scalar_cost().cmp(&other.scalar_cost())
    }
}

// See runtime/runtime-params-estimator/emu-cost/README.md for the motivation of constant values.
const READ_BYTE_COST: u64 = 27;
const WRITE_BYTE_COST: u64 = 47;

impl GasCost {
    pub(crate) fn to_gas(&self) -> Gas {
        ratio_to_gas(self.metric, self.scalar_cost())
    }

    /// Reduces the detailed measurement to a single number.
    pub(crate) fn scalar_cost(&self) -> Ratio<u64> {
        match self.metric {
            GasMetric::ICount => {
                self.qemu_measurement.instructions
                    + self.qemu_measurement.io_r_bytes * READ_BYTE_COST
                    + self.qemu_measurement.io_w_bytes * WRITE_BYTE_COST
            }
            GasMetric::Time => self.time_ns,
        }
    }
}

/// How much gas there is in a nanosecond worth of computation.
const GAS_IN_MEASURE_UNIT: u128 = 1_000_000u128;

pub(crate) fn ratio_to_gas(gas_metric: GasMetric, value: Ratio<u64>) -> Gas {
    let divisor = match gas_metric {
        // We use factor of 8 to approximately match the price of SHA256 operation between
        // time-based and icount-based metric as measured on 3.2Ghz Core i5.
        GasMetric::ICount => 8u128,
        GasMetric::Time => 1u128,
    };
    u64::try_from(
        Ratio::<u128>::new(
            (*value.numer() as u128) * GAS_IN_MEASURE_UNIT,
            (*value.denom() as u128) * divisor,
        )
        .to_integer(),
    )
    .unwrap()
}

pub(crate) fn ratio_to_gas_signed(gas_metric: GasMetric, value: Ratio<i128>) -> i64 {
    let divisor = match gas_metric {
        // We use factor of 8 to approximately match the price of SHA256 operation between
        // time-based and icount-based metric as measured on 3.2Ghz Core i5.
        GasMetric::ICount => 8i128,
        GasMetric::Time => 1i128,
    };
    i64::try_from(
        Ratio::<i128>::new(
            (*value.numer() as i128) * (GAS_IN_MEASURE_UNIT as i128),
            (*value.denom() as i128) * divisor,
        )
        .to_integer(),
    )
    .unwrap()
}
