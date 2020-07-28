#[derive(Debug, Clone)]
pub(crate) struct SignedDiff<T>
where
    T: std::ops::Sub<Output = T> + std::cmp::PartialOrd + std::fmt::Display,
{
    is_positive: bool,
    absolute_difference: T,
}

impl<T> SignedDiff<T>
where
    T: std::ops::Sub<Output = T> + std::cmp::Ord + std::fmt::Display,
{
    pub(crate) fn cmp(lhs: T, rhs: T) -> Self {
        if lhs <= rhs {
            Self { is_positive: true, absolute_difference: rhs - lhs }
        } else {
            Self { is_positive: false, absolute_difference: lhs - rhs }
        }
    }
}

impl<T> std::fmt::Display for SignedDiff<T>
where
    T: std::ops::Sub<Output = T> + std::cmp::Ord + std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", if self.is_positive { "" } else { "-" }, self.absolute_difference)
    }
}

impl<T> std::ops::Neg for SignedDiff<T>
where
    T: std::ops::Sub<Output = T> + std::cmp::Ord + std::fmt::Display,
{
    type Output = Self;

    fn neg(mut self) -> Self::Output {
        self.is_positive = !self.is_positive;
        self
    }
}
