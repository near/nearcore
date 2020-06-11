use num_rational::Ratio;
use num_traits::cast::FromPrimitive;
use num_traits::Num;

/// `exp` is accurate to at least as many digits as zeros in this number.
const PRECISION: i128 = 1_000_000_000_000;

/// Computes `exp(x)`, returning a fraction which approximates the result accurately
/// to 7 decimal places or better on the interval `[-29, 11]`.
pub fn exp(x: Ratio<i128>) -> Ratio<u128> {
    let y = expm1(x);
    Ratio::new((*y.denom() + *y.numer()) as u128, *y.denom() as u128)
}

/// Computes `ln(x)`, returning a fraction which approximates the result accurately
/// to 7 decimal places or better on the interval `[1e-10, 1e+10]`.
pub fn ln(x: Ratio<u128>) -> Ratio<i128> {
    let precision_u128 = PRECISION as u128;
    let (n, d) = round_to_precision(x, precision_u128);
    let result = iln(n, d);
    Ratio::new(result, d as i128)
}

/// Computes exp(x) - 1
fn expm1(x: Ratio<i128>) -> Ratio<i128> {
    // Short circuit extreme inputs.
    let whole_part = x.numer() / x.denom();
    if whole_part < -29 {
        return Ratio::from_integer(-1);
    } else if whole_part > 88 {
        return Ratio::from_integer(i128::MAX);
    }

    // Keep size of denominator ~= PRECISION
    let (n, d) = round_to_precision(x, PRECISION);

    let result = iexpm1(n, d as u128);
    Ratio::new(result, d)
}

trait SaturatingMul {
    fn saturating_mul(self, other: Self) -> Self;
}

impl SaturatingMul for u128 {
    fn saturating_mul(self, other: Self) -> Self {
        u128::saturating_mul(self, other)
    }
}

impl SaturatingMul for i128 {
    fn saturating_mul(self, other: Self) -> Self {
        i128::saturating_mul(self, other)
    }
}

/// Used to keep the denominator of the fractions passed to
/// `exp` and `ln` of order `precision`. This prevents accuracy
/// problems introduced by overflow.
fn round_to_precision<T: Copy + Num + Ord + FromPrimitive + SaturatingMul>(
    x: Ratio<T>,
    precision: T,
) -> (T, T) {
    let n = *x.numer();
    let d = *x.denom();
    let ten = T::from_u32(10).unwrap();

    if d < precision {
        let f = precision / d;
        (f.saturating_mul(n), f.saturating_mul(d))
    } else if d > ten * precision {
        let f = d / precision;
        (n / f, d / f)
    } else {
        (n, d)
    }
}

/// When using Taylor Series to compute iexpm1 or ilnp1, require `|input| <= 2^{-TAYLOR_BOUND}`.
const TAYLOR_BOUND: u32 = 8;

/// Computes `denom * expm1(numer / denom)` to the nearest integer, where `expm1(x) = exp(x) - 1`.
/// Overflows are handled using saturating operations, this means the result may be unreliable if
/// attempting to obtain too much precision (i.e. `numer` and `denom` are both very large).
fn iexpm1(numer: i128, denom: u128) -> i128 {
    // If the denominator overflows i128, then drop the least significant bit from both
    // numerator and denominator.
    let (numer, denom) = if denom > (i128::MAX as u128) {
        (numer >> 1, (denom >> 1) as i128)
    } else {
        (numer, denom as i128)
    };

    let abs_numer = abs(numer);

    // Find a value of `r` such that `|numer / denom| * 2^{-r} < 2^{-TAYLOR_BOUND}`
    let r = (TAYLOR_BOUND + next_power_of_two(abs_numer))
        .saturating_sub(next_power_of_two(denom as u128));

    // Use the Taylor series to compute 2^{r} * denom * expm1(numer / (2^r * denom))
    let scaled_denom = saturating_shl(denom, r);
    let mut taylor_result = 0i128;
    let mut curr_term = numer;
    let mut term_count = 1;
    while curr_term.abs() > 0 {
        taylor_result += curr_term;
        term_count += 1;
        curr_term =
            div_nearest(curr_term.saturating_mul(numer), scaled_denom.saturating_mul(term_count));
        if term_count > 100 {
            break;
        }
    }

    // Use the identity `expm1(2x) = expm1(x) * (expm1(x) + 2)`
    // to compute the desired d * expm1(n/d) from the Taylor series result.
    let mut result = taylor_result;
    for k in (0..r).rev() {
        let scaled_denom = saturating_shl(denom, k + 2);
        result =
            div_nearest(result.saturating_mul(result.saturating_add(scaled_denom)), scaled_denom);
    }

    result
}

/// Computes `denom * ln(numer / denom)` to the nearest integer.
fn iln(numer: u128, denom: u128) -> i128 {
    // If either value overflows i128 then drop the least significant bit on both to roughly
    // keep the same fraction `numer / denom`.
    let (numer, denom) = if numer > (i128::MAX as u128) || denom > (i128::MAX as u128) {
        ((numer >> 1) as i128, (denom >> 1) as i128)
    } else {
        (numer as i128, denom as i128)
    };

    // Take care of extreme values first
    if numer == 0 {
        // lim_{x -> 0} ln(x) = -inf
        return i128::MIN;
    } else if denom == 0 {
        // lim_{x -> 0} x * ln(1 / x) = 0
        return 0;
    }

    // let x = numer - denom, then we write
    // denom * ln(numer / denom) = denom * ln((x + denom) / denom) = denom * ln(1 + x/denom)
    // Define lnp1(y) = lnp1(1 + y), then we can instead compute
    // `denom * lnp1(x / denom)`  since it is equivalent.
    let mut x = numer - denom;
    if x == 0 {
        // lnp1(0) = 0
        return 0;
    }

    // Use the identity
    // `lnp1(y) = 2 * lnp1(y / (1 + sqrt(1 + y)))`
    // to shrink the input until we can apply the Taylor series.
    // We require `x / denom <= 2^-{TAYLOR_BOUND}`.
    let d_sq = denom.saturating_mul(denom);
    let mut transform_count = 0;

    while saturating_shl(x.abs(), TAYLOR_BOUND) > denom {
        x = sqrt_nearest(d_sq.saturating_add(denom.saturating_mul(x))).saturating_sub(denom);
        transform_count += 1;
    }

    // compute denom * lnp1(x / denom) using the Taylor Series
    let mut taylor_result = 0i128;
    let mut curr_term = x;
    let mut term_count = 1;
    while curr_term.abs() > 0 {
        taylor_result += div_nearest(curr_term, term_count);
        term_count += 1;
        curr_term = div_nearest(curr_term.saturating_mul(-x), denom);
        if term_count > 100 {
            break;
        }
    }

    // shift the Taylor series result depending on how much we had to shrink the input first
    saturating_shl(taylor_result, transform_count)
}

fn sqrt_nearest(x: i128) -> i128 {
    // Handle small cases
    if x == 0 {
        return 0;
    } else if x <= 2 {
        return 1;
    }

    let mut a = x / 2;
    let mut b = 0;

    while a != b {
        b = a;
        a = (a - (-x).div_euclid(a)) >> 1;
    }

    a
}

fn saturating_shl(x: i128, n: u32) -> i128 {
    match x.overflowing_shl(n) {
        (y, false) => y,
        (_, true) => i128::MAX,
    }
}

/// Returns the closest integer to (a / b), returning an even number in the case
/// of a tie.
fn div_nearest(a: i128, b: i128) -> i128 {
    match a.overflowing_div_euclid(b) {
        (q, false) => {
            let r = a.rem_euclid(b);
            let adj = if 2 * r + (q & 1) > b { 1 } else { 0 };
            q + adj
        }

        (_, true) => {
            // An overflow can only happen if `a = i128::MIN` and `b = -1`.
            // In this case the result would be `i128::MAX + 1`, but we will
            // take the saturating addition approach and return only i128::MAX
            i128::MAX
        }
    }
}

pub fn abs(x: i128) -> u128 {
    match x.overflowing_abs() {
        (abs_x, false) => abs_x as u128,
        (_, true) => {
            // overflow means x == i128::MIN == -2^127
            1u128 << 127
        }
    }
}

const U128_WIDTH: u32 = 128;

/// Returns the smallest `n` such that 2^n > x
fn next_power_of_two(x: u128) -> u32 {
    U128_WIDTH - x.leading_zeros()
}

#[cfg(test)]
mod test {
    use num_rational::Ratio;
    const INPUTS: [(u128, u128); 9] =
        [(1, 1000), (1, 100), (1, 10), (1, 5), (1, 2), (1, 1), (2, 1), (5, 1), (10, 1)];
    const TOLERANCE: f64 = 1e-7;

    #[test]
    fn test_ln_extremes() {
        let x = Ratio::new(1, 10_000_000_000);
        let y = x.recip();
        let ln_x = super::ln(x);
        let ln_y = super::ln(y);

        let expected_ln_x = -23.02585092994;
        let expected_ln_y = -expected_ln_x;

        let diff = expected_ln_x - i128_to_f64(ln_x);
        assert!(diff.abs() < TOLERANCE);

        let diff = expected_ln_y - i128_to_f64(ln_y);
        assert!(diff.abs() < TOLERANCE);
    }

    #[test]
    fn test_exp_extremes() {
        let x = Ratio::new(11, 1);
        let y = Ratio::new(-29, 1);
        let exp_x = super::exp(x);
        let exp_y = super::exp(y);

        let expected_exp_x = 11f64.exp();
        let expected_exp_y = (-29f64).exp();

        let diff = expected_exp_x - to_f64(exp_x);
        assert!(diff.abs() < TOLERANCE);

        let diff = expected_exp_y - to_f64(exp_y);
        assert!(diff.abs() < TOLERANCE);
    }

    #[test]
    fn test_ln_exp() {
        // exp(ln(x)) should be x
        for (n, d) in INPUTS.iter() {
            let x_f64 = to_f64(Ratio::new(*n, *d));
            let ln_x = super::ln(Ratio::new(*n, *d));
            let exp_ln_x = super::exp(ln_x);

            let diff = x_f64 - to_f64(exp_ln_x);
            assert!(diff.abs() < TOLERANCE);
        }
    }

    #[test]
    fn test_exp_ln() {
        // ln(exp(x)) should be x
        fn exp_ln_common(n: i128, d: i128) {
            let x_f64 = i128_to_f64(Ratio::new(n, d));
            let exp_x = super::exp(Ratio::new(n, d));
            let ln_exp_x = super::ln(exp_x);

            let diff = x_f64 - i128_to_f64(ln_exp_x);
            assert!(diff.abs() < TOLERANCE);
        }

        for (n, d) in INPUTS.iter() {
            let n = *n as i128;
            let d = *d as i128;

            exp_ln_common(n, d);
            exp_ln_common(-n, d);
        }
    }

    #[test]
    fn test_ln() {
        // ln(x) should be accurate
        for (n, d) in INPUTS.iter() {
            test_ln_common(Ratio::new(*n, *d));
        }
    }

    #[test]
    fn test_exp() {
        // exp(x) should be accurate
        for (n, d) in INPUTS.iter() {
            let n = *n as i128;
            let d = *d as i128;
            test_exp_common(Ratio::new(n, d));
            // check negative values too
            test_exp_common(Ratio::new(-n, d));
        }
    }

    fn test_exp_common(input: Ratio<i128>) {
        let expected = i128_to_f64(input).exp();
        let actual = super::exp(input);
        let approximate = to_f64(actual);

        let diff = (expected - approximate).abs();
        assert!(diff < TOLERANCE);
    }

    fn test_ln_common(input: Ratio<u128>) {
        let expected = to_f64(input).ln();
        let actual = super::ln(input);
        let approximate = i128_to_f64(actual);

        let diff = (expected - approximate).abs();
        assert!(diff < TOLERANCE);
    }

    fn i128_to_f64(x: Ratio<i128>) -> f64 {
        if x.numer() < &0 {
            -to_f64(Ratio::new(super::abs(*x.numer()), *x.denom() as u128))
        } else {
            to_f64(Ratio::new(*x.numer() as u128, *x.denom() as u128))
        }
    }

    fn to_f64(x: Ratio<u128>) -> f64 {
        // f64::from(u32) exists, but not for larger numbers,
        // so we possibly need to truncate the fraction.
        let u32_max: u128 = u32::MAX.into();
        let whole_part = x.numer() / x.denom();
        if whole_part > u32_max {
            panic!("Fraction is too large to convert to f64");
        }

        let mut fractional_part: Ratio<u128> = x.fract();
        let reduced_fractional_part = loop {
            if fractional_part.numer() < &u32_max && fractional_part.denom() < &u32_max {
                break fractional_part;
            } else {
                fractional_part =
                    Ratio::new(*fractional_part.numer() / 2, *fractional_part.denom() / 2);
            }
        };

        let approx_fractional_part = f64::from(*reduced_fractional_part.numer() as u32)
            / f64::from(*reduced_fractional_part.denom() as u32);

        approx_fractional_part + f64::from(whole_part as u32)
    }
}
