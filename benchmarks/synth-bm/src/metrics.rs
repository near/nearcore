use std::fmt::Debug;
use std::str::FromStr;
use std::time::{Duration, Instant};

use log::info;
use reqwest::Client;
use tokio::time;
use tokio::time::Interval;

/// The response sent by a NEAR node for `/metrics` queries.
///
/// Structured text, containing comments prefixed by `#` and lines like
/// ```
/// metric_name{optional_specifiers} metric_value
/// ```
pub type Report<'a> = &'a str;

trait Metric: Sized {
    fn name_in_report() -> &'static str;
    fn from_report(report: Report, time: Instant) -> anyhow::Result<Self>;
}

#[derive(Debug, Clone, Copy)]
struct SuccessfulTxsMetric {
    num: u64,
    time: Instant,
}

impl SuccessfulTxsMetric {
    fn new(num: u64, time: Instant) -> Self {
        Self { num, time }
    }
}

impl Default for SuccessfulTxsMetric {
    fn default() -> Self {
        Self { num: 0, time: Instant::now() }
    }
}

impl Metric for SuccessfulTxsMetric {
    fn name_in_report() -> &'static str {
        "near_transaction_processed_successfully_total"
    }

    fn from_report(report: Report, time: Instant) -> anyhow::Result<Self> {
        let lines = report.lines();
        let line = lines.filter(|line| line.starts_with(Self::name_in_report())).next();
        let num = if let Some(line) = line {
            parse_at_eol(line)?
        } else {
            // Absence means the node did not yet process successful transactions since it was
            // started.
            0
        };
        Ok(Self { num, time })
    }
}

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

#[derive(Debug, Copy, Clone)]
struct SuccessfulTxsData {
    num: u64,
    time: Instant,
}

impl SuccessfulTxsData {
    fn new_now(num: u64) -> Self {
        Self { num, time: Instant::now() }
    }
}

/// Provides information regarding the status of transactions.
pub struct TransactionStatisticsService {
    refresh_interval: Interval,
    metrics_url: String,
    /// Data for the beginning of the observed interval.
    data_t0: SuccessfulTxsMetric,
    /// Data at time `t1`, where transaction processing was still ongoing after `t1` .
    data_t1: SuccessfulTxsMetric,
    /// Data at time `t2`. Transaction processing might have stopped in the interval `[t1, t2]`.
    data_t2: SuccessfulTxsMetric,
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
            data_t0: Default::default(),
            data_t1: Default::default(),
            data_t2: Default::default(),
            http_client: Client::new(),
        }
    }

    /// Measures and reports statistics related to the number of successfully processed
    /// transactions.
    ///
    /// Should be called inside a tokio task, as it waits for transaction processing to end.
    pub async fn start(mut self) {
        // TODO return result and get rid of unwraps.
        // Wait for transaction processing to start.
        let initial_count = self.data_t0.num;
        let mut interval_wait_txs_start = time::interval(Duration::from_millis(100));
        info!("Waiting for transaction processing to start");
        loop {
            interval_wait_txs_start.tick().await;
            let report = self.get_report().await.unwrap();
            let metric = get_metric(MetricName::SuccessfulTransactions, report.as_ref()).unwrap();
            // TODO refactor `Metric*` to avoid such matching.
            let num = match metric {
                MetricValue::SuccessfulTransactions { num } => num,
            };
            if num > initial_count {
                let start_time = Instant::now();
                self.data_t0 = SuccessfulTxsMetric::new(0, start_time);
                self.data_t1 = SuccessfulTxsMetric::new(0, start_time);
                info!("Observed successful transactions");
                break;
            }
        }

        // Measure TPS.
        // The first interval tick fires immediately, but we just started observing transactions.
        // So start measuring TPS only with the next tick, after some time passed.
        self.refresh_interval.tick().await;
        loop {
            self.refresh_interval.tick().await;
            let report = self.get_report().await.unwrap();
            let new_metric = SuccessfulTxsMetric::from_report(&report, Instant::now()).unwrap();

            if !(new_metric.num > self.data_t2.num) {
                // No progress since the last observation, so assuming the workload is finished.
                break;
            }

            // More transactions were processed, so update data points and print latest TPS.
            self.data_t1 = self.data_t2;
            self.data_t2 = new_metric;
            self.log_tps();
        }

        info!(
            r#"Tx statistics cut off a small time from the observation period.
            This is done to avoid statistic services slowing down the system.
            So statistics miss a few successful txs, but adjust the observation period.
            Hence for workloads running more than a few secs, TPS are representative.
        "#
        );
    }

    async fn get_report(&self) -> anyhow::Result<String> {
        self.http_client.get(&self.metrics_url).send().await?.text().await.map_err(Into::into)
    }

    /// Calculates and logs successfully processed transactions per second (TPS) observed during
    /// the lifetime of `self`. Reports TPS of zero if the observation period is less than 1 second.
    fn log_tps(&self) -> u64 {
        // Using `data_t1` as transaction processing was still ongoing at `t1`.
        // Don't use `t2` as processing might have stopped before `t2`, see the fields doc comments.
        let elapsed_secs = (self.data_t1.time - self.data_t0.time).as_secs();
        if !(elapsed_secs > 0) {
            return 0;
        }
        let num = self.data_t1.num - self.data_t0.num;
        let tps = num / elapsed_secs;
        info!("Observed {num} successful txs in {elapsed_secs} seconds => {tps} TPS");
        tps
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
