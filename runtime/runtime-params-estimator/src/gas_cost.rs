use std::cmp::Ordering;
use std::time::{Duration, Instant};
use std::{fmt, ops};

use near_primitives::types::Gas;
use num_rational::Ratio;
use num_traits::ToPrimitive;

use crate::config::GasMetric;
use crate::estimator_params::{GAS_IN_INSTR, GAS_IN_NS, IO_READ_BYTE_COST, IO_WRITE_BYTE_COST};
use crate::qemu::QemuMeasurement;

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
    // Values used for `GasMetric::ICount`
    instructions: Ratio<u64>,
    io_r_bytes: Ratio<u64>,
    io_w_bytes: Ratio<u64>,
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
    start: Instant,
    metric: GasMetric,
}

impl GasCost {
    pub(crate) fn zero(metric: GasMetric) -> GasCost {
        GasCost {
            metric,
            time_ns: 0.into(),
            instructions: 0.into(),
            io_r_bytes: 0.into(),
            io_w_bytes: 0.into(),
            uncertain: false,
        }
    }

    pub(crate) fn measure(metric: GasMetric) -> GasClock {
        let start = Instant::now();
        if let GasMetric::ICount = metric {
            QemuMeasurement::start_count_instructions();
        };
        GasClock { start, metric }
    }

    /// Creates `GasCost` out of raw numeric value of gas. This is required mostly for
    /// compatibility with existing code, prefer using `measure` instead.
    pub(crate) fn from_gas(raw: Ratio<u64>, metric: GasMetric) -> GasCost {
        let mut result = GasCost::zero(metric);
        match metric {
            GasMetric::ICount => result.instructions = raw / GAS_IN_INSTR,
            GasMetric::Time => result.time_ns = raw / GAS_IN_NS,
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
        let ns: u64 = self.start.elapsed().as_nanos().try_into().unwrap();
        result.time_ns = ns.into();

        if let GasMetric::ICount = self.metric {
            let qemu_measurement = QemuMeasurement::end_count_instructions();
            result.instructions = qemu_measurement.instructions.into();
            result.io_r_bytes = qemu_measurement.io_r_bytes.into();
            result.io_w_bytes = qemu_measurement.io_w_bytes.into();
        };

        result
    }
}

impl fmt::Debug for GasCost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.metric {
            GasMetric::ICount => {
                write!(
                    f,
                    "{:0.2}i {:0.2}r {:0.2}w",
                    self.instructions.to_f64().unwrap(),
                    self.io_r_bytes.to_f64().unwrap(),
                    self.io_w_bytes.to_f64().unwrap()
                )
            }
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
            instructions: self.instructions + rhs.instructions,
            io_r_bytes: self.io_r_bytes + rhs.io_r_bytes,
            io_w_bytes: self.io_w_bytes + rhs.io_w_bytes,
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
            instructions: self.instructions - rhs.instructions,
            io_r_bytes: self.io_r_bytes - rhs.io_r_bytes,
            io_w_bytes: self.io_w_bytes - rhs.io_w_bytes,
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
            instructions: self.instructions * rhs,
            io_r_bytes: self.io_r_bytes * rhs,
            io_w_bytes: self.io_w_bytes * rhs,
            ..self
        }
    }
}

impl ops::Div<u64> for GasCost {
    type Output = GasCost;

    fn div(self, rhs: u64) -> Self::Output {
        GasCost {
            time_ns: self.time_ns / rhs,
            instructions: self.instructions / rhs,
            io_r_bytes: self.io_r_bytes / rhs,
            io_w_bytes: self.io_w_bytes / rhs,
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
        self.to_gas().cmp(&other.to_gas())
    }
}

impl GasCost {
    pub(crate) fn to_gas(&self) -> Gas {
        match self.metric {
            GasMetric::ICount => {
                self.instructions * GAS_IN_INSTR
                    + self.io_r_bytes * IO_READ_BYTE_COST
                    + self.io_w_bytes * IO_WRITE_BYTE_COST
            }
            GasMetric::Time => self.time_ns * GAS_IN_NS,
        }
        .to_integer()
    }
}
