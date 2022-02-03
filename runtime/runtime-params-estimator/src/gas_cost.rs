use std::cmp::Ordering;
use std::time::{Duration, Instant};
use std::{fmt, ops};

use near_primitives::types::Gas;
use num_rational::Ratio;
use num_traits::ToPrimitive;

use crate::config::GasMetric;
use crate::estimator_params::{GAS_IN_INSTR, GAS_IN_NS, IO_READ_BYTE_COST, IO_WRITE_BYTE_COST};
use crate::least_squares::least_squares_method;
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

    /// Performs least squares using a separate variable for each component of the gas cost.
    ///
    /// Least-squares linear regression is able to produce negative parameters
    /// even when all input values are negative. However, negative gas costs
    /// make no sense for us. What we really want to solve is non-negative least
    /// squares (NNLS) but that is algorithmically much more complex. Instead,
    /// when we do get negative values, we return (A,B), where A has negative
    /// values set to zero and B contains the values so that solution = A-B.
    pub(crate) fn least_squares_method_gas_cost(
        xs: &[u64],
        ys: &[Self],
    ) -> Result<(Self, Self), ((Self, Self), (Self, Self))> {
        let metric = ys[0].metric;
        let uncertain = ys.iter().any(|y| y.uncertain);

        let mut t = (0.into(), 0.into(), vec![]);
        let mut i = (0.into(), 0.into(), vec![]);
        let mut r = (0.into(), 0.into(), vec![]);
        let mut w = (0.into(), 0.into(), vec![]);

        match metric {
            GasMetric::ICount => {
                i = least_squares_method(
                    xs,
                    &ys.iter()
                        .map(|gas_cost| gas_cost.instructions.to_u64().unwrap())
                        .collect::<Vec<_>>(),
                );
                r = least_squares_method(
                    xs,
                    &ys.iter()
                        .map(|gas_cost| gas_cost.io_r_bytes.to_u64().unwrap())
                        .collect::<Vec<_>>(),
                );
                w = least_squares_method(
                    xs,
                    &ys.iter()
                        .map(|gas_cost| gas_cost.io_w_bytes.to_u64().unwrap())
                        .collect::<Vec<_>>(),
                );
            }
            GasMetric::Time => {
                t = least_squares_method(
                    xs,
                    &ys.iter()
                        .map(|gas_cost| gas_cost.time_ns.to_u64().unwrap())
                        .collect::<Vec<_>>(),
                );
            }
        }

        let (pos_t_base, neg_t_base) = split_pos_neg(t.0);
        let (pos_i_base, neg_i_base) = split_pos_neg(i.0);
        let (pos_r_base, neg_r_base) = split_pos_neg(r.0);
        let (pos_w_base, neg_w_base) = split_pos_neg(w.0);

        let (pos_t_factor, neg_t_factor) = split_pos_neg(t.1);
        let (pos_i_factor, neg_i_factor) = split_pos_neg(i.1);
        let (pos_r_factor, neg_r_factor) = split_pos_neg(r.1);
        let (pos_w_factor, neg_w_factor) = split_pos_neg(w.1);

        let neg_base = GasCost {
            time_ns: neg_t_base,
            instructions: neg_i_base,
            io_r_bytes: neg_r_base,
            io_w_bytes: neg_w_base,
            metric,
            uncertain,
        };
        let neg_factor = GasCost {
            time_ns: neg_t_factor,
            instructions: neg_i_factor,
            io_r_bytes: neg_r_factor,
            io_w_bytes: neg_w_factor,
            metric,
            uncertain,
        };
        let pos_base = GasCost {
            time_ns: pos_t_base,
            instructions: pos_i_base,
            io_r_bytes: pos_r_base,
            io_w_bytes: pos_w_base,
            metric,
            uncertain,
        };
        let pos_factor = GasCost {
            time_ns: pos_t_factor,
            instructions: pos_i_factor,
            io_r_bytes: pos_r_factor,
            io_w_bytes: pos_w_factor,
            metric,
            uncertain,
        };

        if neg_base.to_gas() == 0 && neg_factor.to_gas() == 0 {
            Ok((pos_base, pos_factor))
        } else {
            Err(((pos_base, pos_factor), (neg_base, neg_factor)))
        }
    }
}

/// Transforms input C into two components, where A,B are non-negative and where A-B ~= input.
/// This method intentionally rounds fractions to whole integers, rounding towards zero.
fn split_pos_neg(num: Ratio<i128>) -> (Ratio<u64>, Ratio<u64>) {
    let pos = num.to_integer().to_u64().unwrap_or_default().into();
    let neg = (-num).to_integer().to_u64().unwrap_or_default().into();
    (pos, neg)
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

#[cfg(test)]
impl GasCost {
    fn new_time_based(time_ns: impl Into<Ratio<u64>>) -> Self {
        let mut result = GasCost::zero(GasMetric::Time);
        result.time_ns = time_ns.into();
        result
    }
    fn new_icount_based(
        instructions: impl Into<Ratio<u64>>,
        io_r_bytes: impl Into<Ratio<u64>>,
        io_w_bytes: impl Into<Ratio<u64>>,
    ) -> Self {
        let mut result = GasCost::zero(GasMetric::ICount);
        result.instructions = instructions.into();
        result.io_r_bytes = io_r_bytes.into();
        result.io_w_bytes = io_w_bytes.into();
        result
    }
}

#[test]
fn least_squares_method_gas_cost_time_ok() {
    let xs = [10, 20, 30];

    let ys = [
        GasCost::new_time_based(10_050),
        GasCost::new_time_based(20_050),
        GasCost::new_time_based(30_050),
    ];

    match GasCost::least_squares_method_gas_cost(&xs, &ys) {
        Ok((a, b)) => {
            assert_eq!(a, GasCost::new_time_based(50));
            assert_eq!(b, GasCost::new_time_based(1000));
        }
        Err(((_, _), (a, b))) => {
            panic!("Least squares got negative values where it shouldn't. Negative parts: a={:?} b={:?}", a, b)
        }
    }
}

#[test]
fn least_squares_method_gas_cost_time_neg_base() {
    let xs = [10, 20, 30];

    let ys = [
        GasCost::new_time_based(9_950),
        GasCost::new_time_based(19_950),
        GasCost::new_time_based(29_950),
    ];

    match GasCost::least_squares_method_gas_cost(&xs, &ys) {
        Err(((a_pos, b_pos), (a_neg, b_neg))) => {
            assert_eq!(a_pos, GasCost::new_time_based(0));
            assert_eq!(a_neg, GasCost::new_time_based(50));

            assert_eq!(b_pos, GasCost::new_time_based(1000));
            assert_eq!(b_neg, GasCost::new_time_based(0));
        }
        Ok(_) => panic!("Least squares got no negative values where it should have."),
    }
}
#[test]
fn least_squares_method_gas_cost_time_neg_factor() {
    let xs = [10, 20, 30];

    let ys =
        [GasCost::new_time_based(990), GasCost::new_time_based(980), GasCost::new_time_based(970)];

    match GasCost::least_squares_method_gas_cost(&xs, &ys) {
        Err(((a_pos, b_pos), (a_neg, b_neg))) => {
            assert_eq!(a_pos, GasCost::new_time_based(1000));
            assert_eq!(a_neg, GasCost::new_time_based(0));

            assert_eq!(b_pos, GasCost::new_time_based(0));
            assert_eq!(b_neg, GasCost::new_time_based(1));
        }
        Ok(_) => panic!("Least squares got no negative values where it should have."),
    }
}

#[test]
fn least_squares_method_gas_cost_icount_ok() {
    let xs = [10, 20, 30];

    let ys = [
        GasCost::new_icount_based(10_050, 20_060, 30_070),
        GasCost::new_icount_based(20_050, 40_060, 60_070),
        GasCost::new_icount_based(30_050, 60_060, 90_070),
    ];

    match GasCost::least_squares_method_gas_cost(&xs, &ys) {
        Ok((a, b)) => {
            assert_eq!(a, GasCost::new_icount_based(50, 60, 70));
            assert_eq!(b, GasCost::new_icount_based(1000, 2000, 3000));
        }
        Err(((_, _), (a, b))) => {
            panic!("Least squares got negative values where it shouldn't. Negative parts: a={:?} b={:?}", a, b)
        }
    }
}

#[test]
fn least_squares_method_gas_cost_icount_neg_base() {
    let xs = [10, 20, 30];

    let ys = [
        GasCost::new_icount_based(9_950, 19_960, 29_970),
        GasCost::new_icount_based(19_950, 39_960, 59_970),
        GasCost::new_icount_based(29_950, 59_960, 89_970),
    ];

    match GasCost::least_squares_method_gas_cost(&xs, &ys) {
        Err(((a_pos, b_pos), (a_neg, b_neg))) => {
            assert_eq!(a_pos, GasCost::new_icount_based(0, 0, 0));
            assert_eq!(a_neg, GasCost::new_icount_based(50, 40, 30));

            assert_eq!(b_pos, GasCost::new_icount_based(1000, 2000, 3000));
            assert_eq!(b_neg, GasCost::new_icount_based(0, 0, 0));
        }
        Ok(_) => panic!("Least squares got no negative values where it should have."),
    }
}
#[test]
fn least_squares_method_gas_cost_icount_neg_factor() {
    let xs = [10, 20, 30];

    let ys = [
        GasCost::new_icount_based(990, 981, 972),
        GasCost::new_icount_based(980, 961, 942),
        GasCost::new_icount_based(970, 941, 912),
    ];

    match GasCost::least_squares_method_gas_cost(&xs, &ys) {
        Err(((a_pos, b_pos), (a_neg, b_neg))) => {
            assert_eq!(a_pos, GasCost::new_icount_based(1000, 1001, 1002));
            assert_eq!(a_neg, GasCost::new_icount_based(0, 0, 0));

            assert_eq!(b_pos, GasCost::new_icount_based(0, 0, 0));
            assert_eq!(b_neg, GasCost::new_icount_based(1, 2, 3));
        }
        Ok(_) => panic!("Least squares got no negative values where it should have."),
    }
}
