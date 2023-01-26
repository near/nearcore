use anyhow::Context;
use near_primitives::types::Gas;
use num_rational::Ratio;
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use crate::cost::Cost;

/// For each [`Cost`], the price of a single unit in [`Gas`].
///
/// This is the ultimate result of a parameter estimator run.
#[derive(Default)]
pub struct CostTable {
    map: BTreeMap<Cost, Gas>,
}

#[derive(Default)]
pub struct CostTableDiff {
    map: BTreeMap<Cost, (Gas, Gas)>,
}

impl CostTable {
    pub(crate) fn add(&mut self, cost: Cost, value: Gas) {
        let prev = self.map.insert(cost, value);
        assert!(prev.is_none())
    }
    pub(crate) fn get(&self, cost: Cost) -> Option<Gas> {
        self.map.get(&cost).copied()
    }
    pub fn diff(&self, other: &CostTable) -> CostTableDiff {
        let mut res = CostTableDiff::default();
        for (&cost, &x) in &self.map {
            if let Some(&y) = other.map.get(&cost) {
                res.map.insert(cost, (x, y));
            }
        }
        res
    }
}

impl FromStr for CostTable {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut res = CostTable::default();
        for line in s.lines() {
            let mut words = line.split_ascii_whitespace();
            let cost = words.next().context("expected cost name")?;
            let gas = words.next().context("expected gas value")?;
            if let Some(word) = words.next() {
                anyhow::bail!("unexpected token {word}");
            }

            let cost = cost.parse()?;
            let value = gas.replace('_', "").parse()?;

            res.add(cost, value)
        }
        Ok(res)
    }
}

impl fmt::Display for CostTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for cost in Cost::all() {
            if let Some(gas) = self.get(cost) {
                let gas = format_gas(gas);
                writeln!(f, "{:<35} {:>25}", cost.to_string(), gas)?
            }
        }
        Ok(())
    }
}

impl fmt::Display for CostTableDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{:<35} {:>25} {:>25} {:>13}", "Cost", "First", "Second", "Second/First")?;

        let mut biggest_diff_first = self.map.iter().collect::<Vec<_>>();
        biggest_diff_first.sort_by_key(|(_, &(f, s))| Ratio::new(f, s).max(Ratio::new(s, f)));
        biggest_diff_first.reverse();

        for (&cost, &(first, second)) in biggest_diff_first {
            writeln!(
                f,
                "{:<35} {:>25} {:>25} {:>13.2}",
                cost.to_string(),
                format_gas(first),
                format_gas(second),
                second as f64 / first as f64,
            )?
        }
        Ok(())
    }
}

pub(crate) fn format_gas(mut n: Gas) -> String {
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
    assert_eq!(format_gas(0).as_str(), "0");
    assert_eq!(format_gas(999).as_str(), "999");
    assert_eq!(format_gas(1000).as_str(), "1_000");
    assert_eq!(format_gas(u64::MAX).as_str(), "18_446_744_073_709_551_615");
}
