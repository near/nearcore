#[test]
fn test_overflow() {
    let a = u64::MAX;
    let b = 5u64;
    std::panic::catch_unwind(move || {
        let c = u128::from(a + b);
        println!("{} + {} = {}", a, b, c);
    })
    .unwrap_err();
}

#[test]
fn test_underflow() {
    let a = 10u64;
    let b = 5u64;
    std::panic::catch_unwind(move || {
        let c = u128::from(b - a);
        println!("{} - {} = {}", b, a, c);
    })
    .unwrap_err();
}
