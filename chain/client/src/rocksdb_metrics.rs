use near_metrics::{try_create_gauge_vec, try_create_int_gauge};
use prometheus::{GaugeVec, IntGauge};
use std::collections::HashMap;

#[derive(Default)]
/// Creates prometheus metrics on-demand for exporting RocksDB statistics.
pub(crate) struct RocksDBMetrics {
    // Contains counters and sums, which are integer statistics in RocksDB.
    int_gauges: HashMap<String, IntGauge>,
    // Contains floating point statistics, such as quantiles of timings.
    gauges: HashMap<String, GaugeVec>,
}

impl RocksDBMetrics {
    pub fn export_stats_as_metrics(
        &mut self,
        stats: &[(&str, Vec<StatsValue>)],
    ) -> anyhow::Result<()> {
        for (stat_name, values) in stats {
            if values.len() == 1 {
                // A counter stats.
                // A statistic 'a.b.c' creates the following prometheus metric:
                // - near_a_b_c
                if let StatsValue::Count(value) = values[0] {
                    self.set_int_value(
                        |stat_name| stat_name.to_string(),
                        |stat_name| get_prometheus_metric_name(stat_name),
                        stat_name,
                        value,
                    )?;
                }
            } else {
                // A summary stats.
                // A statistic 'a.b.c' creates the following prometheus metrics:
                // - near_a_b_c_sum
                // - near_a_b_c_count
                // - near_a_b_c{quantile="0.95"}
                for stats_value in values {
                    match stats_value {
                        StatsValue::Count(value) => {
                            self.set_int_value(
                                |stat_name| get_stats_summary_count_key(stat_name),
                                |stat_name| get_metric_name_summary_count_gauge(stat_name),
                                stat_name,
                                *value,
                            )?;
                        }
                        StatsValue::Sum(value) => {
                            self.set_int_value(
                                |stat_name| get_stats_summary_sum_key(stat_name),
                                |stat_name| get_metric_name_summary_sum_gauge(stat_name),
                                stat_name,
                                *value,
                            )?;
                        }
                        StatsValue::Percentile(percentile, value) => {
                            let key: &str = stat_name;
                            if !self.gauges.contains_key(key) {
                                self.gauges.insert(
                                    key.to_string(),
                                    try_create_gauge_vec(
                                        &get_prometheus_metric_name(stat_name),
                                        stat_name,
                                        &["quantile"],
                                    )?,
                                );
                            }
                            self.gauges
                                .get(key)
                                .unwrap()
                                .with_label_values(&[&format!("{:.2}", *percentile as f64 * 0.01)])
                                .set(*value);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn set_int_value<F1, F2>(
        &mut self,
        key_fn: F1,
        metric_fn: F2,
        stat_name: &str,
        value: i64,
    ) -> anyhow::Result<()>
    where
        F1: Fn(&str) -> String,
        F2: Fn(&str) -> String,
    {
        let key: &str = &key_fn(stat_name);
        if !self.int_gauges.contains_key(key) {
            self.int_gauges
                .insert(key.to_string(), try_create_int_gauge(&metric_fn(stat_name), stat_name)?);
        }
        self.int_gauges.get(key).unwrap().set(value);
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum StatsValue {
    Count(i64),
    Sum(i64),
    Percentile(u32, f64),
}

/// Parses a string containing RocksDB statistics.
pub(crate) fn parse_statistics(statistics: &str) -> anyhow::Result<Vec<(&str, Vec<StatsValue>)>> {
    let mut result = vec![];
    // Statistics are given one per line.
    for line in statistics.split('\n') {
        // Each line follows one of two formats:
        // 1) <stat_name> COUNT : <value>
        // 2) <stat_name> P50 : <value> P90 : <value> COUNT : <value> SUM : <value>
        // Each line gets split into words and we parse statistics according to this format.
        if let Some((stat_name, words)) = line.split_once(' ') {
            let mut values = vec![];
            let mut words = words.split(" : ").flat_map(|v| v.split(" "));
            while let (Some(key), Some(val)) = (words.next(), words.next()) {
                match key {
                    "COUNT" => values.push(StatsValue::Count(val.parse::<i64>()?)),
                    "SUM" => values.push(StatsValue::Sum(val.parse::<i64>()?)),
                    p if p.starts_with("P") => values.push(StatsValue::Percentile(
                        key[1..].parse::<u32>()?,
                        val.parse::<f64>()?,
                    )),
                    _ => {
                        return Err(anyhow::anyhow!(
                            "Unsupported stats value: {} in {}",
                            key,
                            line
                        ));
                    }
                }
            }
            result.push((stat_name, values));
        }
    }
    Ok(result)
}

fn get_prometheus_metric_name(stat_name: &str) -> String {
    format!("near_{}", stat_name.replace(".", "_"))
}

fn get_metric_name_summary_count_gauge(stat_name: &str) -> String {
    format!("near_{}_count", stat_name.replace(".", "_"))
}

fn get_metric_name_summary_sum_gauge(stat_name: &str) -> String {
    format!("near_{}_sum", stat_name.replace(".", "_"))
}

fn get_stats_summary_count_key(stat_name: &str) -> String {
    format!("{}.count", stat_name)
}

fn get_stats_summary_sum_key(stat_name: &str) -> String {
    format!("{}.sum", stat_name)
}
