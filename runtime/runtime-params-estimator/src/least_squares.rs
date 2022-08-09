use crate::estimator_params::GAS_IN_NS;
use crate::qemu::QemuMeasurement;
use num_rational::Ratio;
use num_traits::ToPrimitive;

pub(crate) fn least_squares_method(
    xs: &[u64],
    ys: &[u64],
) -> (Ratio<i128>, Ratio<i128>, Vec<i128>) {
    let n = xs.len();
    let n128 = n as i128;

    let mut sum_prod = 0 as i128; // Sum of x * y.
    for i in 0..n {
        sum_prod = sum_prod + (xs[i] as i128) * (ys[i] as i128);
    }
    let mut sum_x = 0 as i128; // Sum of x.
    for i in 0..n {
        sum_x = sum_x + (xs[i] as i128);
    }
    let mut sum_y = 0 as i128; // Sum of y.
    for i in 0..n {
        sum_y = sum_y + (ys[i] as i128);
    }
    let mut sum_x_square = 0 as i128; // Sum of x^2.
    for i in 0..n {
        sum_x_square = sum_x_square + (xs[i] as i128) * (xs[i] as i128);
    }
    let b = Ratio::new(n128 * sum_prod - sum_x * sum_y, n128 * sum_x_square - sum_x * sum_x);
    let a = Ratio::new(sum_y * b.denom() - b.numer() * sum_x, n128 * b.denom());

    // Compute error estimations
    let mut errs = vec![];
    for i in 0..n {
        let expect = a + b * (xs[i] as i128);
        let diff = expect - (ys[i] as i128);
        errs.push(diff.round().to_integer());
    }

    (a, b, errs)
}

pub(crate) fn time_measurement_least_squares(
    xs: &[u64],
    ys: &[Ratio<u64>],
) -> ((Ratio<u64>, Ratio<u64>), (Ratio<u64>, Ratio<u64>)) {
    let t = least_squares_method(
        xs,
        &ys.iter().map(|ns| (ns * GAS_IN_NS).to_integer()).collect::<Vec<_>>(),
    );
    let (pos_t_base, neg_t_base) = split_pos_neg(t.0);
    let (pos_t_factor, neg_t_factor) = split_pos_neg(t.1);

    let neg_base = neg_t_base / GAS_IN_NS;
    let neg_factor = neg_t_factor / GAS_IN_NS;
    let pos_base = pos_t_base / GAS_IN_NS;
    let pos_factor = pos_t_factor / GAS_IN_NS;

    ((pos_base, pos_factor), (neg_base, neg_factor))
}

pub(crate) fn qemu_measurement_least_squares(
    xs: &[u64],
    ys: &[QemuMeasurement],
) -> ((QemuMeasurement, QemuMeasurement), (QemuMeasurement, QemuMeasurement)) {
    let i = least_squares_method(
        xs,
        &ys.iter().map(|q| q.instructions.to_integer()).collect::<Vec<_>>(),
    );
    let r =
        least_squares_method(xs, &ys.iter().map(|q| q.io_r_bytes.to_integer()).collect::<Vec<_>>());
    let w =
        least_squares_method(xs, &ys.iter().map(|q| q.io_w_bytes.to_integer()).collect::<Vec<_>>());

    let (pos_i_base, neg_i_base) = split_pos_neg(i.0);
    let (pos_r_base, neg_r_base) = split_pos_neg(r.0);
    let (pos_w_base, neg_w_base) = split_pos_neg(w.0);

    let (pos_i_factor, neg_i_factor) = split_pos_neg(i.1);
    let (pos_r_factor, neg_r_factor) = split_pos_neg(r.1);
    let (pos_w_factor, neg_w_factor) = split_pos_neg(w.1);

    let neg_base = QemuMeasurement {
        instructions: neg_i_base,
        io_r_bytes: neg_r_base,
        io_w_bytes: neg_w_base,
    };
    let neg_factor = QemuMeasurement {
        instructions: neg_i_factor,
        io_r_bytes: neg_r_factor,
        io_w_bytes: neg_w_factor,
    };
    let pos_base = QemuMeasurement {
        instructions: pos_i_base,
        io_r_bytes: pos_r_base,
        io_w_bytes: pos_w_base,
    };
    let pos_factor = QemuMeasurement {
        instructions: pos_i_factor,
        io_r_bytes: pos_r_factor,
        io_w_bytes: pos_w_factor,
    };
    ((pos_base, pos_factor), (neg_base, neg_factor))
}

/// Transforms input C into two components, where A,B are non-negative and where A-B ~= input.
/// This method intentionally rounds fractions to whole integers, rounding towards zero.
fn split_pos_neg(num: Ratio<i128>) -> (Ratio<u64>, Ratio<u64>) {
    let pos = num.to_integer().to_u64().unwrap_or_default().into();
    let neg = (-num).to_integer().to_u64().unwrap_or_default().into();
    (pos, neg)
}

#[cfg(test)]
mod tests {
    use num_traits::ToPrimitive;

    use super::*;

    #[track_caller]
    fn check_least_squares_method(xs: &[u64], ys: &[u64], expected: (f64, f64, &[f64])) {
        let (a, b, r) = least_squares_method(&xs, &ys);

        assert_eq!(a.to_f64().unwrap(), expected.0);
        assert_eq!(b.to_f64().unwrap(), expected.1);
        for (&expected, &actual) in expected.2.iter().zip(r.iter()) {
            assert_eq!(actual, expected.round().to_i128().unwrap());
        }
    }

    #[test]
    fn test_least_squares_method_perfect_fit() {
        let xs = [10, 20, 30];
        let ys = [990, 980, 970];

        let a = 1000.0f64;
        let b = -1.0f64;
        let error = [0.0, 0.0, 0.0];

        check_least_squares_method(&xs, &ys, (a, b, &error));
    }

    #[test]
    fn test_least_squares_method_imperfect_fit() {
        let xs = [10, 1000, 2000];
        let ys = [1, 101, 198];

        let a = 0.6784115012962526;
        let b = 0.09899161644389078;
        let error = [0.668327661, -1.329972499, 0.661643501];

        check_least_squares_method(&xs, &ys, (a, b, &error));
    }

    #[test]
    fn test_large_numbers() {
        let xs = [1, 1000000, 4000000];
        let ys = [5663378466666, 5718530400000, 6235718400000];

        let a = 5622793876267.89;
        let b = 149849.09760264654;
        let error = [-40584440549.0127, 54112573870.5361, -13528133321.5244];

        check_least_squares_method(&xs, &ys, (a, b, &error));
    }
}
