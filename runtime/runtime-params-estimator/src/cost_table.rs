use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use near_primitives::types::Gas;

use crate::cost::Cost;

/// For each [`Cost`], the price of a single unit in [`Gas`].
///
/// This is the ultimate result of a parameter estimator run.
#[derive(Default)]
pub struct CostTable {
    map: BTreeMap<Cost, Gas>,
}

impl CostTable {
    pub(crate) fn add(&mut self, cost: Cost, value: Gas) {
        let prev = self.map.insert(cost, value);
        assert!(prev.is_none())
    }
    pub(crate) fn get(&self, cost: Cost) -> Option<Gas> {
        self.map.get(&cost).copied()
    }
}

impl FromStr for CostTable {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut res = CostTable::default();
        for line in s.lines() {
            let mut words = line.split_ascii_whitespace();
            let cost = words.next().ok_or(())?;
            let gas = words.next().ok_or(())?;
            if words.next().is_some() {
                return Err(());
            }

            let cost = cost.parse()?;
            let value = gas.replace('_', "").parse().map_err(drop)?;

            res.add(cost, value)
        }
        Ok(res)
    }
}

impl fmt::Display for CostTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for cost in Cost::all() {
            if let Some(gas) = self.get(cost) {
                let gas = separate_thousands(gas);
                writeln!(f, "{:<35} {:>25}", cost.to_string(), gas)?
            }
        }
        Ok(())
    }
}

fn separate_thousands(mut n: u64) -> String {
    let mut parts = Vec::new();
    while n >= 1000 {
        parts.push(format!("{:03?}", n % 1000));
        n /= 1000;
    }
    parts.push(n.to_string());
    parts.reverse();
    parts.join("_")
}

#[test]
fn test_separate_thousands() {
    assert_eq!(separate_thousands(0).as_str(), "0");
    assert_eq!(separate_thousands(999).as_str(), "999");
    assert_eq!(separate_thousands(1000).as_str(), "1_000");
    assert_eq!(separate_thousands(u64::MAX).as_str(), "18_446_744_073_709_551_615");
}
