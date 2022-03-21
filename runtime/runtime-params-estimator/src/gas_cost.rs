use std::cmp::Ordering;
use std::panic::Location;
use std::time::{Duration, Instant};
use std::{fmt, ops};

use near_primitives::types::Gas;
use num_rational::Ratio;
use num_traits::ToPrimitive;
use serde_json::json;

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
    pub(crate) fn zero(metric: GasMetric) -> GasCost {
        GasCost {
            metric,
            time_ns: 0.into(),
            instructions: 0.into(),
            io_r_bytes: 0.into(),
            io_w_bytes: 0.into(),
            uncertain: None,
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
        assert_eq!(self.metric, rhs.metric);
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
        assert_eq!(self.metric, rhs.metric);
        GasCost {
            time_ns: saturating_sub(self.time_ns, rhs.time_ns),
            instructions: saturating_sub(self.instructions, rhs.instructions),
            io_r_bytes: saturating_sub(self.io_r_bytes, rhs.io_r_bytes),
            io_w_bytes: saturating_sub(self.io_w_bytes, rhs.io_w_bytes),
            metric: self.metric,
            uncertain: None,
        }
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
        match self.metric {
            GasMetric::ICount => json!({
                "gas": self.to_gas(),
                "metric": "icount",
                "instructions": self.instructions.to_f64(),
                "io_r_bytes": self.io_r_bytes.to_f64(),
                "io_w_bytes": self.io_w_bytes.to_f64(),
                // `None` will be printed as `null`
                "uncertain_reason": self.uncertain.map(|u| u.reason),
            }),
            GasMetric::Time => json!({
                "gas": self.to_gas(),
                "metric": "time",
                "time_ns": self.time_ns.to_f64(),
                "uncertain": self.uncertain.is_some(),
                "uncertain_reason": self.uncertain.map(|u| u.reason),
            }),
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

/// Like GasCost::least_squares_method_gas_cost but in the case of a solution with negative parameters, it returns  (A,B), where A has negative values set to zero and B contains the
/// values so that solution = A-B.
fn least_squares_method_gas_cost_pos_neg(
    xs: &[u64],
    ys: &[GasCost],
    verbose: bool,
) -> Result<(GasCost, GasCost), ((GasCost, GasCost), (GasCost, GasCost))> {
    let metric = ys[0].metric;
    let uncertain = ys.iter().find_map(|cost| cost.uncertain);

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
                &ys.iter().map(|gas_cost| gas_cost.time_ns.to_u64().unwrap()).collect::<Vec<_>>(),
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
        if verbose {
            eprintln!(
                "Least-squares had negative parameters: ({:?} - {:?}) + N * ({:?} - {:?})",
                pos_base, neg_base, pos_factor, neg_factor
            );
        }
        Err(((pos_base, pos_factor), (neg_base, neg_factor)))
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

    fn add(mut self, rhs: GasCost) -> Self::Output {
        assert_eq!(self.metric, rhs.metric);
        self.combine_uncertain(&rhs);
        GasCost {
            time_ns: self.time_ns + rhs.time_ns,
            instructions: self.instructions + rhs.instructions,
            io_r_bytes: self.io_r_bytes + rhs.io_r_bytes,
            io_w_bytes: self.io_w_bytes + rhs.io_w_bytes,
            metric: self.metric,
            uncertain: self.uncertain,
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

    #[track_caller]
    fn sub(self, rhs: GasCost) -> Self::Output {
        self.saturating_sub(&rhs, &NonNegativeTolerance::Strict)
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
mod tests {
    use super::{least_squares_method_gas_cost_pos_neg, GasCost, LeastSquaresTolerance};
    use crate::{
        config::GasMetric,
        estimator_params::{GAS_IN_INSTR, GAS_IN_NS, IO_READ_BYTE_COST, IO_WRITE_BYTE_COST},
    };
    use near_primitives::types::Gas;
    use num_rational::Ratio;
    use num_traits::ToPrimitive;

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
            let mut result = GasCost::zero(GasMetric::Time);
            result.time_ns = time_ns.into();
            result
        }
        pub(crate) fn new_icount_based(
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
