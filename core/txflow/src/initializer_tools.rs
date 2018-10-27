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
