/// Handy utility to create maps.
macro_rules! map(
        { $($key:expr => $value:expr),+ } => {
            {
                let mut m = ::std::collections::HashMap::new();
                $(
                    m.insert($key, $value);
                )+
                m
            }
        };
    );

/// Handy utility to create sets.
macro_rules! set(
        { $($el:expr),+ } => {
            {
                let mut s = ::std::collections::HashSet::new();
                $(
                    s.insert($el);
                )+
                s
            }
        };
    );


#[cfg(test)]
mod tests {
    use std::hash::{Hash, Hasher};

    #[derive(Debug)]
    struct ComplexType{
        pub value: u64,
    }
    impl PartialEq for ComplexType {
        fn eq(&self, other: &Self) -> bool {
            self.value == other.value
        }
    }
    impl Hash for ComplexType {
        fn hash<H: Hasher>(&self, state: &mut H) {
            state.write_u64(self.value);
        }
    }
    impl Eq for ComplexType {}

    #[test]
    fn simple_map() {
        let m1 = map!{0 => 1, 2 => 3};
        let m2 = map!{2 => 3, 0 => 1};
        assert_eq!(m1, m2);
    }

    #[test]
    fn complex_map() {
        let m1 = map!{ComplexType{value: 0} => ComplexType{value: 1},
                     ComplexType{value: 2} => ComplexType{value: 3}};
        let m2 = map!{ComplexType{value: 2} => ComplexType{value: 3},
            ComplexType{value: 0} => ComplexType{value: 1}};
        assert_eq!(m1, m2);
    }

    #[test]
    fn simple_set() {
        let s1 = set!{0, 1};
        let s2 = set!{1, 0};
        assert_eq!(s1, s2);
    }

    #[test]
    fn complex_set() {
        let s1 = set!{ComplexType{value: 0}, ComplexType{value: 1}};
        let s2 = set!{ComplexType{value: 1}, ComplexType{value: 0}};
        assert_eq!(s1, s2);
    }
}
