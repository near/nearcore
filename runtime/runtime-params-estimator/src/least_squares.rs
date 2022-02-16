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
        errs.push(diff.to_integer());
    }

    (a, b, errs)
}

#[test]
fn test_least_squares_method_perfect_fit() {
    let xs = [10, 20, 30];
    let ys = [990, 980, 970];

    let (a, b, r) = least_squares_method(&xs, &ys);

    assert_eq!(a, Ratio::new(1000, 1));
    assert_eq!(b, Ratio::new(-1, 1));
    assert_eq!(r[0], 0);
    assert_eq!(r[1], 0);
    assert_eq!(r[2], 0);
}

#[test]
fn test_least_squares_method_imperfect_fit() {
    let xs = [10, 1000, 2000];
    let ys = [1, 101, 198];

    let (a, b, r) = least_squares_method(&xs, &ys);

    assert_eq!(a, Ratio::new(5757, 8486)); // 0.678411501
    assert_eq!(b, Ratio::new(58803, 594020)); // 0.098991616
    assert_eq!(r[0], 0); // 0.668327661
    assert_eq!(r[1], -1); // -1.329972499
    assert_eq!(r[2], 0); // 0.661643501
}
