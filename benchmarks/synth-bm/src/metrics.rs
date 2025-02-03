use std::fmt::Debug;
use std::str::FromStr;

use anyhow::Context;
use serde::ser::StdError;

pub type Report<'a> = &'a str;

/// Defines metrics for which parsing has been implemented.
///
/// A node's `/metrics` endpoint returns structured text from which metrics can be extracted.
#[derive(Copy, Clone, Debug)]
pub enum MetricName {
    /// The number of successfull transactions since the node was started.
    SuccessfulTransactions,
}

impl MetricName {
    fn report_name(self) -> &'static str {
        match self {
            MetricName::SuccessfulTransactions => "near_transaction_processed_successfully_total",
        }
    }
}

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum MetricValue {
    SuccessfulTransactions {
        /// Absence of the metric in a [`Report`] implies the node has not (yet) successfully
        /// processed any transactions.
        num: u64,
    },
}

pub fn get_metric(name: MetricName, report: Report) -> anyhow::Result<MetricValue> {
    let lines = report.lines();
    match name {
        MetricName::SuccessfulTransactions => {
            let line = lines.filter(|line| line.starts_with(name.report_name())).next();
            let num = if let Some(line) = line {
                parse_at_eol(line)?
            } else {
                // Absence means the node did not yet process successful transactions.
                0
            };
            Ok(MetricValue::SuccessfulTransactions { num })
        }
    }
}

/// Parses `F` on the last word in the `line`.
fn parse_at_eol<F>(line: &str) -> anyhow::Result<F>
where
    F: FromStr,
    <F as FromStr>::Err: Debug,
{
    let result = match line.rsplit_once(" ") {
        None => line.parse(),
        Some((_, last_word)) => last_word.parse(),
    };
    result.map_err(|err| anyhow::anyhow!("Failed to parse at end of line: {:?}", err))
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn test_get_metric_successful_transactions() -> anyhow::Result<()> {
        let report_some = indoc! {r#"
            # HELP near_transaction_processed_successfully_total The number of transactions processed successfully since starting this node
            # TYPE near_transaction_processed_successfully_total counter
            near_transaction_processed_successfully_total 100
        "#};
        assert_eq!(
            get_metric(MetricName::SuccessfulTransactions, report_some)?,
            MetricValue::SuccessfulTransactions { num: 100 }
        );

        let report_zero = indoc! {r#"
            # HELP near_transaction_processed_successfully_total The number of transactions processed successfully since starting this node
            # TYPE near_transaction_processed_successfully_total counter
            near_transaction_processed_successfully_total 0
        "#};
        assert_eq!(
            get_metric(MetricName::SuccessfulTransactions, report_zero)?,
            MetricValue::SuccessfulTransactions { num: 0 }
        );

        let report_none = indoc! {r#"
            # A report which does not contain any metrics.
        "#};
        assert_eq!(
            get_metric(MetricName::SuccessfulTransactions, report_none)?,
            MetricValue::SuccessfulTransactions { num: 0 }
        );

        Ok(())
    }
}
