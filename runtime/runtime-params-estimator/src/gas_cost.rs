use std::cmp::Ordering;
use std::panic::Location;
use std::time::{Duration, Instant};
use std::{fmt, iter, ops};

use near_primitives::types::Gas;
use num_rational::Ratio;
use num_traits::ToPrimitive;
use serde_json::json;

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
    time_ns: Option<Ratio<u64>>,
    // Values used for `GasMetric::ICount`
    qemu: Option<QemuMeasurement>,
    /// Signals that the measurement was uncertain (ie, had high variance), and
    /// that the estimation needs to be re-run.
    ///
    /// Each specific cost can use it's own criteria for uncertainty -- the end
    /// result here is just printing UNCERTAIN next to the corresponding cost in
    /// the output. `uncertain_message` can be called to display the reason and
    /// code location of where the uncertainty has been set.
    uncertain: Option<MeasurementUncertainty>,
}

pub(crate) struct GasClock {
    start: Instant,
    metric: GasMetric,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct MeasurementUncertainty {
    reason: &'static str,
    location: &'static Location<'static>,
}

impl GasCost {
    pub(crate) fn zero() -> GasCost {
        GasCost { time_ns: None, qemu: None, uncertain: None }
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
        let mut result = GasCost::zero();
        match metric {
            GasMetric::ICount => {
                result.qemu = Some(QemuMeasurement {
                    instructions: raw / GAS_IN_INSTR,
                    io_r_bytes: 0.into(),
                    io_w_bytes: 0.into(),
                })
            }
            GasMetric::Time => result.time_ns = Some(raw / GAS_IN_NS),
        }
        result
    }

    /// Like [`std::cmp::Ord::min`] but operates on heterogenous types ([`GasCost`] + [`Gas`]).
    pub(crate) fn min_gas(mut self, gas: Gas) -> Self {
        let Some(to_add) = gas.checked_sub(self.to_gas()) else { return self; };
        if let Some(qemu) = &mut self.qemu {
            // QEMU gas is split across multiple components (instructions
            // and IO). When rounding up to an amount of gas, the assumption
            // is that the caller does not care about how the extra gas is
            // distributed. So we add it to the instruction counter and
            // don't touch the other values.
            qemu.instructions += Ratio::from(to_add) / GAS_IN_INSTR;
        } else {
            // Time is a single component that we can just set directly.
            self.time_ns = Some(Ratio::from(gas) / GAS_IN_NS);
        }
        self
    }

    pub(crate) fn is_uncertain(&self) -> bool {
        self.uncertain.is_some()
    }
    pub(crate) fn uncertain_message(&self) -> Option<String> {
        self.uncertain
            .map(|MeasurementUncertainty { reason, location }| format!("{reason}: {location}"))
    }
    #[track_caller]
    pub(crate) fn set_uncertain(&mut self, reason: &'static str) {
        self.uncertain = Some(MeasurementUncertainty { reason, location: Location::caller() });
    }
    /// Performs least squares using a separate variable for each component of the gas cost.
    ///
    /// Least-squares linear regression sometimes to produces negative
    /// parameters even when all input values are positive. However, negative
    /// gas costs make no sense for us. What we really want to solve is
    /// non-negative least squares (NNLS) but that is algorithmically much more
    /// complex. To keep it simpler, instead of solving NNLS, the caller has a
    /// couple of choices how to handle negative solutions.
    #[track_caller]
    pub(crate) fn least_squares_method_gas_cost(
        xs: &[u64],
        ys: &[Self],
        tolerance: &LeastSquaresTolerance,
        verbose: bool,
    ) -> (Self, Self) {
        if verbose {
            eprintln!("Least squares input:");
            eprint!("[");
            for x in xs {
                eprint!("{x},");
            }
            eprintln!("] * x =");
            eprint!("[");
            for y in ys {
                eprint!("{},", y.to_gas());
            }
            eprintln!("]");
        }
        match least_squares_method_gas_cost_pos_neg(xs, ys, verbose) {
            Ok(res) => res,
            Err((mut pos, neg)) => {
                // On negative parameters, return positive part and mark as uncertain if necessary
                if !tolerance.tolerates(&pos, &neg) {
                    pos.0.set_uncertain("NEG-LEAST-SQUARES");
                    pos.1.set_uncertain("NEG-LEAST-SQUARES");
                }
                pos
            }
        }
    }

    /// Subtracts two gas costs from each other without panicking on an arithmetic underflow.
    /// If the given tolerance is breached, the result will be marked as uncertain.
    #[track_caller]
    pub(crate) fn saturating_sub(&self, rhs: &Self, tolerance: &NonNegativeTolerance) -> Self {
        let mut pos = self.saturating_sub_no_uncertain_check(rhs);
        let neg = rhs.saturating_sub_no_uncertain_check(self);
        if !tolerance.tolerates(&pos, &neg) {
            pos.set_uncertain("SUBTRACTION-UNDERFLOW");
        }
        pos.combine_uncertain(self);
        pos.combine_uncertain(rhs);
        pos
    }

    fn saturating_sub_no_uncertain_check(&self, rhs: &Self) -> Self {
        let qemu = match (&self.qemu, &rhs.qemu) {
            (Some(lhs), Some(rhs)) => Some(QemuMeasurement {
                instructions: saturating_sub(lhs.instructions, rhs.instructions),
                io_r_bytes: saturating_sub(lhs.io_r_bytes, rhs.io_r_bytes),
                io_w_bytes: saturating_sub(lhs.io_w_bytes, rhs.io_w_bytes),
            }),
            (any_lhs, _any_rhs) => any_lhs.clone(),
        };
        let time_ns = match (self.time_ns, rhs.time_ns) {
            (Some(lhs), Some(rhs)) => Some(saturating_sub(lhs, rhs)),
            (any_lhs, _any_rhs) => any_lhs,
        };
        GasCost { time_ns, qemu, uncertain: None }
    }

    /// Does nothing if `GasCost` is already uncertain, otherise copies
    /// uncertain from other `GasCost`.
    fn combine_uncertain(&mut self, rhs: &Self) {
        if !self.is_uncertain() {
            self.uncertain = rhs.uncertain;
        }
    }
    /// JSON representation of the gas cost. This is intended to be used by
    /// other scripts, such as the continuous estimation pipeline. Consumers
    /// should expect more fields to be added. But existing fields should remain
    /// stable.

    pub fn to_json(&self) -> serde_json::Value {
        if let Some(qemu) = &self.qemu {
            json!({
                "gas": self.to_gas(),
                "metric": "icount",
                "instructions": qemu.instructions.to_f64(),
                "io_r_bytes": qemu.io_r_bytes.to_f64(),
                "io_w_bytes": qemu.io_w_bytes.to_f64(),
                // `None` will be printed as `null`
                "uncertain_reason": self.uncertain.map(|u| u.reason),
            })
        } else if let Some(time_ns) = self.time_ns {
            json!({
                "gas": self.to_gas(),
                "metric": "time",
                "time_ns": time_ns.to_f64(),
                // `None` will be printed as `null`
                "uncertain_reason": self.uncertain.map(|u| u.reason),
            })
        } else {
            serde_json::Value::Null
        }
    }
}

/// Defines what negative solutions are allowed in a least-squares result.
/// Default is all negative values are treated as errors.
#[derive(Clone, PartialEq)]
pub(crate) struct LeastSquaresTolerance {
    base_nn_tolerance: NonNegativeTolerance,
    factor_nn_tolerance: NonNegativeTolerance,
}

impl Default for LeastSquaresTolerance {
    fn default() -> Self {
        Self {
            base_nn_tolerance: NonNegativeTolerance::Strict,
            factor_nn_tolerance: NonNegativeTolerance::Strict,
        }
    }
}
impl LeastSquaresTolerance {
    /// Tolerate negative values in base cost up to a factor of total cost
    #[allow(dead_code)]
    pub(crate) fn base_rel_nn_tolerance(mut self, rel_tolerance: f64) -> Self {
        self.base_nn_tolerance = NonNegativeTolerance::RelativeTolerance(rel_tolerance);
        self
    }
    /// Tolerate negative values in base cost up to a factor of total cost
    pub(crate) fn factor_rel_nn_tolerance(mut self, rel_tolerance: f64) -> Self {
        self.factor_nn_tolerance = NonNegativeTolerance::RelativeTolerance(rel_tolerance);
        self
    }
    /// Tolerate negative values in base cost up to a fixed gas value
    pub(crate) fn base_abs_nn_tolerance(mut self, abs_tolerance: Gas) -> Self {
        self.base_nn_tolerance = NonNegativeTolerance::AbsoluteTolerance(abs_tolerance);
        self
    }
    /// Tolerate negative values in base cost up to a fixed gas value
    #[allow(dead_code)]
    pub(crate) fn factor_abs_nn_tolerance(mut self, abs_tolerance: Gas) -> Self {
        self.factor_nn_tolerance = NonNegativeTolerance::AbsoluteTolerance(abs_tolerance);
        self
    }
}

/// Defines what negative solutions are allowed in a least-squares result
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum NonNegativeTolerance {
    /// Allow no negative values
    Strict,
    /// Tolerate negative values if it changes the total gas by less than X times.
    RelativeTolerance(f64),
    /// Tolerate negative values if they are below X gas
    AbsoluteTolerance(Gas),
}

impl LeastSquaresTolerance {
    fn tolerates(&self, pos: &(GasCost, GasCost), neg: &(GasCost, GasCost)) -> bool {
        self.base_nn_tolerance.tolerates(&pos.0, &neg.0)
            && self.factor_nn_tolerance.tolerates(&pos.1, &neg.1)
    }
}
impl NonNegativeTolerance {
    /// Tolerate negative values if they account for less than 0.1% of the total
    pub(crate) const PER_MILLE: NonNegativeTolerance =
        NonNegativeTolerance::RelativeTolerance(0.001);

    fn tolerates(&self, pos: &GasCost, neg: &GasCost) -> bool {
        match self {
            NonNegativeTolerance::Strict => neg.to_gas() == 0,
            NonNegativeTolerance::RelativeTolerance(rel_tolerance) => {
                pos.to_gas() > 0
                    && Ratio::new(neg.to_gas(), pos.to_gas()).to_f64().unwrap() <= *rel_tolerance
            }
            NonNegativeTolerance::AbsoluteTolerance(gas_threshold) => {
                neg.to_gas() <= *gas_threshold
            }
        }
    }
}

/// Like GasCost::least_squares_method_gas_cost but in the case of a solution
/// with negative parameters, it returns  (A,B), where A has negative values set
/// to zero and B contains the values so that solution = A-B.
fn least_squares_method_gas_cost_pos_neg(
    xs: &[u64],
    ys: &[GasCost],
    verbose: bool,
) -> Result<(GasCost, GasCost), ((GasCost, GasCost), (GasCost, GasCost))> {
    let uncertain = ys.iter().find_map(|cost| cost.uncertain);

    let mut pos_base = GasCost::zero();
    let mut pos_factor = GasCost::zero();
    let mut neg_base = GasCost::zero();
    let mut neg_factor = GasCost::zero();

    pos_base.uncertain = uncertain;
    pos_factor.uncertain = uncertain;

    if let Some(first) = ys.get(0) {
        if first.qemu.is_some() {
            assert!(
                ys.iter().all(|y| y.qemu.is_some() || y.to_gas() == 0),
                "least square expects homogenous data"
            );

            let qemu_ys = ys
                .iter()
                .map(|y| y.qemu.clone().unwrap_or_else(|| QemuMeasurement::zero()))
                .collect::<Vec<_>>();

            let (pos, neg) = crate::least_squares::qemu_measurement_least_squares(xs, &qemu_ys);
            pos_base.qemu = Some(pos.0);
            pos_factor.qemu = Some(pos.1);
            neg_base.qemu = Some(neg.0);
            neg_factor.qemu = Some(neg.1);
        }
        if first.time_ns.is_some() {
            assert!(
                ys.iter().all(|y| y.time_ns.is_some() || y.to_gas() == 0),
                "least square expects homogenous data"
            );
            let time_ys = ys.iter().map(|y| y.time_ns.unwrap_or(0.into())).collect::<Vec<_>>();
            let (pos, neg) = crate::least_squares::time_measurement_least_squares(xs, &time_ys);
            pos_base.time_ns = Some(pos.0);
            pos_factor.time_ns = Some(pos.1);
            neg_base.time_ns = Some(neg.0);
            neg_factor.time_ns = Some(neg.1);
        }
    }

    if neg_base.to_gas() == 0 && neg_factor.to_gas() == 0 {
        if verbose {
            eprintln!("Least-squares output: {pos_base:?} + N * {pos_factor:?}",);
        }
        Ok((pos_base, pos_factor))
    } else {
        if verbose {
            eprintln!(
                "Least-squares had negative parameters: ({pos_base:?} - {neg_base:?}) + N * ({pos_factor:?} - {neg_factor:?})",
            );
        }
        Err(((pos_base, pos_factor), (neg_base, neg_factor)))
    }
}

impl GasClock {
    pub(crate) fn elapsed(self) -> GasCost {
        let mut result = GasCost::zero();

        match self.metric {
            GasMetric::ICount => {
                let qemu = QemuMeasurement::end_count_instructions();
                result.qemu = Some(qemu);
            }
            GasMetric::Time => {
                let ns: u64 = self.start.elapsed().as_nanos().try_into().unwrap();
                result.time_ns = Some(ns.into());
            }
        }

        result
    }
}

impl fmt::Debug for GasCost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(qemu) = &self.qemu {
            write!(
                f,
                "{:0.2}i {:0.2}r {:0.2}w",
                qemu.instructions.to_f64().unwrap(),
                qemu.io_r_bytes.to_f64().unwrap(),
                qemu.io_w_bytes.to_f64().unwrap()
            )
        } else if let Some(time_ns) = self.time_ns {
            if time_ns >= 1.into() {
                fmt::Debug::fmt(&Duration::from_nanos(time_ns.round().to_integer()), f)
            } else {
                // Sometimes, dividing costs yields results smaller than one ns.
                write!(f, "{}ps", (time_ns * 1000).to_integer())
            }
        } else {
            write!(f, "empty-measurement")
        }
    }
}

impl ops::Add for GasCost {
    type Output = GasCost;

    fn add(mut self, rhs: GasCost) -> Self::Output {
        self.combine_uncertain(&rhs);
        let qemu = match (self.qemu, rhs.qemu) {
            (None, None) => None,
            (Some(lhs), Some(rhs)) => Some(lhs + rhs),
            (single_value, None) | (None, single_value) => single_value,
        };
        let time_ns = match (self.time_ns, rhs.time_ns) {
            (None, None) => None,
            (Some(lhs), Some(rhs)) => Some(lhs + rhs),
            (single_value, None) | (None, single_value) => single_value,
        };
        GasCost { time_ns, qemu, uncertain: self.uncertain }
    }
}

impl ops::AddAssign for GasCost {
    fn add_assign(&mut self, rhs: GasCost) {
        *self = self.clone() + rhs;
    }
}

impl iter::Sum for GasCost {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        let mut accum = GasCost::zero();
        for gas in iter {
            accum += gas;
        }
        accum
    }
}

impl ops::Sub for GasCost {
    type Output = GasCost;

    #[track_caller]
    fn sub(self, rhs: GasCost) -> Self::Output {
        self.saturating_sub(&rhs, &NonNegativeTolerance::Strict)
    }
}

impl ops::Mul<u64> for GasCost {
    type Output = GasCost;

    fn mul(mut self, rhs: u64) -> Self::Output {
        if let Some(qemu) = &mut self.qemu {
            qemu.instructions *= rhs;
            qemu.io_r_bytes *= rhs;
            qemu.io_w_bytes *= rhs;
        }
        if let Some(time_ns) = &mut self.time_ns {
            *time_ns *= rhs;
        }
        self
    }
}

impl ops::Div<u64> for GasCost {
    type Output = GasCost;

    fn div(mut self, rhs: u64) -> Self::Output {
        if let Some(qemu) = &mut self.qemu {
            qemu.instructions /= rhs;
            qemu.io_r_bytes /= rhs;
            qemu.io_w_bytes /= rhs;
        }
        if let Some(time_ns) = &mut self.time_ns {
            *time_ns /= rhs;
        }
        self
    }
}

fn saturating_sub(a: Ratio<u64>, b: Ratio<u64>) -> Ratio<u64> {
    if a < b {
        0.into()
    } else {
        a - b
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
        if let Some(qemu) = &self.qemu {
            (GAS_IN_INSTR * qemu.instructions
                + IO_READ_BYTE_COST * qemu.io_r_bytes
                + IO_WRITE_BYTE_COST * qemu.io_w_bytes)
                .to_integer()
        } else if let Some(ns) = self.time_ns {
            (GAS_IN_NS * ns).to_integer()
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{least_squares_method_gas_cost_pos_neg, GasCost, LeastSquaresTolerance};
    use crate::estimator_params::{GAS_IN_INSTR, GAS_IN_NS, IO_READ_BYTE_COST, IO_WRITE_BYTE_COST};
    use crate::qemu::QemuMeasurement;
    use near_primitives::types::Gas;
    use num_rational::Ratio;
    use num_traits::{ToPrimitive, Zero};

    #[track_caller]
    fn check_uncertainty(
        xs: &[u64],
        ys: &[GasCost],
        tolerance: LeastSquaresTolerance,
        expected_uncertain: bool,
    ) {
        let result = GasCost::least_squares_method_gas_cost(xs, ys, &tolerance, true);
        assert_eq!(result.0.is_uncertain(), expected_uncertain);
        assert_eq!(result.1.is_uncertain(), expected_uncertain);
    }

    #[track_caller]
    fn check_least_squares_method_gas_cost_pos_neg(
        xs: &[u64],
        ys: &[GasCost],
        expected: Result<(GasCost, GasCost), ((GasCost, GasCost), (GasCost, GasCost))>,
    ) {
        let result = least_squares_method_gas_cost_pos_neg(xs, ys, true);
        assert_eq!(result, expected);
    }

    impl GasCost {
        pub(crate) fn new_time_based(time_ns: impl Into<Ratio<u64>>) -> Self {
            let mut result = GasCost::zero();
            result.time_ns = Some(time_ns.into());
            result
        }
        pub(crate) fn new_icount_based(
            instructions: impl Into<Ratio<u64>>,
            io_r_bytes: impl Into<Ratio<u64>>,
            io_w_bytes: impl Into<Ratio<u64>>,
        ) -> Self {
            let mut result = GasCost::zero();
            let qemu = QemuMeasurement {
                instructions: instructions.into(),
                io_r_bytes: io_r_bytes.into(),
                io_w_bytes: io_w_bytes.into(),
            };
            result.qemu = Some(qemu);
            result
        }
    }

    fn abs_tolerance(base: Gas, factor: Gas) -> LeastSquaresTolerance {
        LeastSquaresTolerance::default().base_abs_nn_tolerance(base).factor_abs_nn_tolerance(factor)
    }
    fn rel_tolerance(base: f64, factor: f64) -> LeastSquaresTolerance {
        LeastSquaresTolerance::default().base_rel_nn_tolerance(base).factor_rel_nn_tolerance(factor)
    }

    #[test]
    fn least_squares_method_gas_cost_time_ok() {
        let xs = [10, 20, 30];

        let ys = [
            GasCost::new_time_based(10_050),
            GasCost::new_time_based(20_050),
            GasCost::new_time_based(30_050),
        ];

        let expected = Ok((GasCost::new_time_based(50), GasCost::new_time_based(1000)));
        // Check low-level least-squares method
        check_least_squares_method_gas_cost_pos_neg(&xs, &ys, expected);

        // Also check applied tolerance strategies, in  this case they should always be certain as the solution is positive
        check_uncertainty(&xs, &ys, Default::default(), false);
        check_uncertainty(&xs, &ys, abs_tolerance(1, 1), false);
        check_uncertainty(&xs, &ys, rel_tolerance(0.1, 0.1), false);
    }

    #[test]
    fn least_squares_method_gas_cost_time_below_ns() {
        let xs = [1, 2, 3];

        let ys = [
            GasCost::new_time_based(Ratio::new(11, 10)),
            GasCost::new_time_based(Ratio::new(12, 10)),
            GasCost::new_time_based(Ratio::new(13, 10)),
        ];

        let expected = Ok((GasCost::new_time_based(1), GasCost::new_time_based(Ratio::new(1, 10))));
        check_least_squares_method_gas_cost_pos_neg(&xs, &ys, expected);
    }

    #[test]
    fn least_squares_method_gas_cost_time_neg_base() {
        let xs = [10, 20, 30];

        let ys = [
            GasCost::new_time_based(9_950),
            GasCost::new_time_based(19_950),
            GasCost::new_time_based(29_950),
        ];

        let expected = Err((
            (GasCost::new_time_based(0), GasCost::new_time_based(1000)),
            (GasCost::new_time_based(50), GasCost::new_time_based(0)),
        ));
        check_least_squares_method_gas_cost_pos_neg(&xs, &ys, expected);

        check_uncertainty(&xs, &ys, Default::default(), true);
        check_uncertainty(&xs, &ys, abs_tolerance(1, 1), true);
        check_uncertainty(&xs, &ys, abs_tolerance((GAS_IN_NS * 50).to_integer(), 1), false);
        check_uncertainty(&xs, &ys, rel_tolerance(0.1, 0.1), true);
    }

    #[test]
    fn least_squares_method_gas_cost_time_neg_factor() {
        let xs = [10, 20, 30];

        let ys = [
            GasCost::new_time_based(990),
            GasCost::new_time_based(980),
            GasCost::new_time_based(970),
        ];

        let expected = Err((
            (GasCost::new_time_based(1000), GasCost::new_time_based(0)),
            (GasCost::new_time_based(0), GasCost::new_time_based(1)),
        ));
        check_least_squares_method_gas_cost_pos_neg(&xs, &ys, expected);

        check_uncertainty(&xs, &ys, Default::default(), true);
        check_uncertainty(&xs, &ys, abs_tolerance(1, 1), true);
        check_uncertainty(&xs, &ys, abs_tolerance(1, 1_000_000), false);
        check_uncertainty(&xs, &ys, rel_tolerance(0.1, 0.1), true);
    }

    #[test]
    fn least_squares_method_gas_cost_icount_ok() {
        let xs = [10, 20, 30];

        let ys = [
            GasCost::new_icount_based(10_050, 20_060, 30_070),
            GasCost::new_icount_based(20_050, 40_060, 60_070),
            GasCost::new_icount_based(30_050, 60_060, 90_070),
        ];

        let expected = Ok((
            GasCost::new_icount_based(50, 60, 70),
            GasCost::new_icount_based(1000, 2000, 3000),
        ));
        check_least_squares_method_gas_cost_pos_neg(&xs, &ys, expected);

        check_uncertainty(&xs, &ys, Default::default(), false);
        check_uncertainty(&xs, &ys, abs_tolerance(1, 1), false);
        check_uncertainty(&xs, &ys, rel_tolerance(0.1, 0.1), false);
    }

    #[test]
    fn least_squares_method_gas_cost_icount_neg_base() {
        let xs = [10, 20, 30];

        let ys = [
            GasCost::new_icount_based(9_950, 19_960, 29_970),
            GasCost::new_icount_based(19_950, 39_960, 59_970),
            GasCost::new_icount_based(29_950, 59_960, 89_970),
        ];

        let expected = Err((
            (GasCost::new_icount_based(0, 0, 0), GasCost::new_icount_based(1000, 2000, 3000)),
            (GasCost::new_icount_based(50, 40, 30), GasCost::new_icount_based(0, 0, 0)),
        ));
        check_least_squares_method_gas_cost_pos_neg(&xs, &ys, expected);

        check_uncertainty(&xs, &ys, Default::default(), true);
        check_uncertainty(&xs, &ys, abs_tolerance(1, 1), true);
        check_uncertainty(&xs, &ys, rel_tolerance(0.1, 0.1), true);
        check_uncertainty(
            &xs,
            &ys,
            abs_tolerance(
                (GAS_IN_INSTR * 50 + IO_READ_BYTE_COST * 40 + IO_WRITE_BYTE_COST * 30)
                    .ceil()
                    .to_integer(),
                0,
            ),
            false,
        );
    }

    #[test]
    fn least_squares_method_gas_cost_icount_neg_factor() {
        let xs = [10, 20, 30];

        let ys = [
            GasCost::new_icount_based(990, 981, 972),
            GasCost::new_icount_based(980, 961, 942),
            GasCost::new_icount_based(970, 941, 912),
        ];

        let expected = Err((
            (GasCost::new_icount_based(1000, 1001, 1002), GasCost::new_icount_based(0, 0, 0)),
            (GasCost::new_icount_based(0, 0, 0), GasCost::new_icount_based(1, 2, 3)),
        ));
        check_least_squares_method_gas_cost_pos_neg(&xs, &ys, expected);

        check_uncertainty(&xs, &ys, Default::default(), true);
        check_uncertainty(&xs, &ys, abs_tolerance(1, 1), true);
        check_uncertainty(&xs, &ys, rel_tolerance(0.1, 0.1), true);
        check_uncertainty(
            &xs,
            &ys,
            abs_tolerance(
                0,
                (GAS_IN_INSTR * 1 + IO_READ_BYTE_COST * 2 + IO_WRITE_BYTE_COST * 3)
                    .ceil()
                    .to_integer(),
            ),
            false,
        );
    }

    #[test]
    fn least_squares_method_gas_cost_icount_mixed_neg_pos() {
        let xs = [10, 20, 30];

        let ys = [
            GasCost::new_icount_based(990, 1010, 0),
            GasCost::new_icount_based(980, 1020, 10),
            GasCost::new_icount_based(970, 1030, 20),
        ];

        let expected = Err((
            (GasCost::new_icount_based(1000, 1000, 0), GasCost::new_icount_based(0, 1, 1)),
            (GasCost::new_icount_based(0, 0, 10), GasCost::new_icount_based(1, 0, 0)),
        ));
        check_least_squares_method_gas_cost_pos_neg(&xs, &ys, expected);

        check_uncertainty(&xs, &ys, Default::default(), true);
        check_uncertainty(&xs, &ys, abs_tolerance(1, 1), true);
        check_uncertainty(
            &xs,
            &ys,
            abs_tolerance(
                (IO_WRITE_BYTE_COST * 10).ceil().to_integer(),
                (GAS_IN_INSTR * 1).ceil().to_integer(),
            ),
            false,
        );

        if IO_READ_BYTE_COST + IO_WRITE_BYTE_COST == Ratio::zero() || GAS_IN_INSTR == Ratio::zero()
        {
            // Relative tolerance only makes sense if two different scalars of the vector cost can be non-zero.
            // Otherwise, one of the (pos,neg) pair has to be 0.
            return;
        }

        // Compute relative thresholds based on estimator params
        let rel_base = (IO_WRITE_BYTE_COST * 10) / (GAS_IN_INSTR * 1000 + IO_READ_BYTE_COST * 1000);
        let rel_factor = (GAS_IN_INSTR * 1) / (IO_READ_BYTE_COST * 1 + IO_WRITE_BYTE_COST * 1);
        check_uncertainty(
            &xs,
            &ys,
            rel_tolerance(rel_base.to_f64().unwrap() * 1.1, rel_factor.to_f64().unwrap() * 1.1),
            false,
        );
        check_uncertainty(
            &xs,
            &ys,
            rel_tolerance(rel_base.to_f64().unwrap() * 0.9, rel_factor.to_f64().unwrap() * 1.1),
            true,
        );
        check_uncertainty(
            &xs,
            &ys,
            rel_tolerance(rel_base.to_f64().unwrap() * 1.1, rel_factor.to_f64().unwrap() * 0.9),
            true,
        );
    }
}
