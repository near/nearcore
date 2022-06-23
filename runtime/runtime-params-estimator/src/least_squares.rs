use num_rational::Ratio;

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
