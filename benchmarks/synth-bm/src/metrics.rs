use std::fmt::Debug;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::Context;
use log::info;
use reqwest::Client;
use serde::ser::StdError;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::Interval;

/// The response sent by a NEAR node for `/metrics` queries.
///
/// Structured text, containing comments prefixed by `#` and lines like
/// ```
/// metric_name{optional_specifiers} metric_value
/// ```
pub type Report<'a> = &'a str;

/// Defines metrics for which parsing from a `Report` has been implemented.
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

pub struct TransactionStatisticsService {
    refresh_interval: Interval,
    metrics_url: String,
    start_time: Instant,
    num_start: u64,
    num_successful_transactions: u64,
    http_client: Client,
}

impl TransactionStatisticsService {
    /// Construction must be followed by a call to [`Self::start`].
    pub fn new(rpc_url: String, refresh_interval: Duration) -> Self {
        let mut metrics_url = rpc_url;
        metrics_url.push_str("/metrics");
        Self {
            refresh_interval: time::interval(refresh_interval),
            metrics_url,
            start_time: Instant::now(),
            num_start: 0,
            num_successful_transactions: 0,
            http_client: Client::new(),
        }
    }

    pub async fn start(mut self) -> JoinHandle<()> {
        // TODO return result from JoinHandle and get rid of unwraps.
        let handle = tokio::spawn(async move {
            // Wait for transactions to start.
            let initial_count = self.num_successful_transactions;
            let mut interval_wait_txs_start = time::interval(Duration::from_millis(100));
            info!("Waiting for transaction processing to start");
            loop {
                interval_wait_txs_start.tick().await;
                let report = self.get_report().await.unwrap();
                let metric =
                    get_metric(MetricName::SuccessfulTransactions, report.as_ref()).unwrap();
                // TODO refactor `Metric*` to avoid such matching.
                let num = match metric {
                    MetricValue::SuccessfulTransactions { num } => num,
                };
                info!("observing num = {num}");
                if num > initial_count {
                    self.num_start = num;
                    self.start_time = Instant::now();
                    info!("Observed successful transactions");
                    break;
                }
            }

            // Measure TPS.
            loop {
                self.refresh_interval.tick().await;
                let report = self.get_report().await.unwrap();
                let metric =
                    get_metric(MetricName::SuccessfulTransactions, report.as_ref()).unwrap();
                // TODO refactor `Metric*` to avoid such matching.
                let num = match metric {
                    MetricValue::SuccessfulTransactions { num } => num,
                };
                let last_num = self.num_successful_transactions;
                self.num_successful_transactions = num;
                if last_num == self.num_successful_transactions {
                    break;
                }
                let elapsed_secs = self.start_time.elapsed().as_secs();
                if elapsed_secs > 0 {
                    info!("TPS: {}", (num - self.num_start) / elapsed_secs);
                }
            }
        });
        handle
    }

    /*
    async fn wait_for_transaction_processing(&mut self) -> anyhow::Result<()> {
            let handle: JoinHandle<anyhow::Result<u64>> = tokio::spawn(async {
                let initial_count = self.num_successful_transactions;
                let mut interval = time::interval(Duration::from_millis(500));
                loop {
                    interval.tick().await;
                    let report = self.get_report().await?;
                    let metric = get_metric(MetricName::SuccessfulTransactions, report.as_ref())?;
                    // TODO refactor `Metric*` to avoid such matching.
                    let num = match metric {
                        MetricValue::SuccessfulTransactions { num } => num,
                    };
                    if num > initial_count {
                        return Ok(num);
                    }
                }
            });
            Ok(())
        }
        */

    async fn get_report(&self) -> anyhow::Result<String> {
        self.http_client.get(&self.metrics_url).send().await?.text().await.map_err(Into::into)
    }
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
