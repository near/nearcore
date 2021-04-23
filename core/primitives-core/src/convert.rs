use crate::types::Gas;

pub trait ToGas<T> {
    fn tera(_: T) -> Self;
}

impl ToGas<f64> for Gas {
    fn tera(tera_gas: f64) -> Self {
        (tera_gas * f64::powf(10_f64, 12_f64)) as Gas
    }
}

#[cfg(test)]
mod unit {
    use super::*;
    #[test]
    fn tera_to_gas() {
        assert_eq!(0, Gas::tera(0.0000000000001));
        assert_eq!(1, Gas::tera(0.000000000001));
        assert_eq!(1000000000000, Gas::tera(1.0));
        assert_eq!(1234567891234, Gas::tera(1.2345678912341));
        assert_eq!(9999999999999, Gas::tera(9.99999999999991));
    }
}
