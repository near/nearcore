use crate::db::{Db, EstimationRow};
use anyhow::Context;
use std::time::Duration;

/// Additional information required for import
#[derive(Debug, clap::Parser)]
pub(crate) struct ImportConfig {
    /// Required for importing estimation results, which source code commit it
    /// should be associated with.
    #[clap(long)]
    pub commit_hash: Option<String>,
    /// Required for importing parameter values, which protocol version it
    /// should be associated with.
    #[clap(long)]
    pub protocol_version: Option<u32>,
}

/// Estimation result as produced by the params-estimator
#[derive(serde::Deserialize, Debug, PartialEq)]
struct EstimatorOutput {
    name: String,
    result: EstimationResult,
    computed_in: Duration,
}
#[derive(serde::Deserialize, Debug, PartialEq)]
struct EstimationResult {
    gas: f64,
    time_ns: Option<f64>,
    instructions: Option<f64>,
    io_r_bytes: Option<f64>,
    io_w_bytes: Option<f64>,
    uncertain_reason: Option<String>,
}

impl Db {
    pub(crate) fn import_json_lines(&self, info: &ImportConfig, input: &str) -> anyhow::Result<()> {
        for line in input.lines() {
            self.import(info, line)?;
        }
        Ok(())
    }

    fn import(&self, info: &ImportConfig, line: &str) -> anyhow::Result<()> {
        if let Ok(estimator_output) = serde_json::from_str::<EstimatorOutput>(line) {
            let commit_hash = info.commit_hash.as_ref().with_context(|| {
                "Missing --commit-hash argument while importing estimation data".to_owned()
            })?;
            let row = EstimationRow {
                name: estimator_output.name,
                gas: estimator_output.result.gas,
                parameter: None, // TODO
                wall_clock_time: estimator_output.result.time_ns,
                icount: estimator_output.result.instructions,
                io_read: estimator_output.result.io_r_bytes,
                io_write: estimator_output.result.io_w_bytes,
                uncertain_reason: estimator_output.result.uncertain_reason,
                commit_hash: commit_hash.clone(),
            };
            row.insert(self)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::db::{Db, EstimationRow};
    use crate::import::ImportConfig;
    use crate::Metric;

    #[test]
    fn test_import_time() {
        let input = r#"
            {"computed_in":{"nanos":826929296,"secs":0},"name":"LogBase","result":{"gas":441061948,"metric":"time","time_ns":441.061948,"uncertain_reason":null}}
            {"computed_in":{"nanos":983235753,"secs":0},"name":"LogByte","result":{"gas":2743748,"metric":"time","time_ns":2.7437486640625,"uncertain_reason":"HIGH-VARIANCE"}}
        "#;
        let expected = [
            EstimationRow {
                name: "LogBase".to_owned(),
                gas: 441061948.0,
                parameter: None,
                wall_clock_time: Some(441.061948),
                icount: None,
                io_read: None,
                io_write: None,
                uncertain_reason: None,
                commit_hash: "53a3ccf3ef07".to_owned(),
            },
            EstimationRow {
                name: "LogByte".to_owned(),
                gas: 2743748.0,
                parameter: None,
                wall_clock_time: Some(2.7437486640625),
                icount: None,
                io_read: None,
                io_write: None,
                uncertain_reason: Some("HIGH-VARIANCE".to_owned()),
                commit_hash: "53a3ccf3ef07".to_owned(),
            },
        ];
        let info = ImportConfig {
            commit_hash: Some("53a3ccf3ef07".to_owned()),
            protocol_version: Some(0),
        };
        assert_import(input, &info, &expected, Metric::Time);
    }
    #[test]
    fn test_import_icount() {
        let input = r#"
        {"computed_in":{"nanos":107762511,"secs":17},"name":"ActionReceiptCreation","result":{"gas":240650158750,"instructions":1860478.51,"io_r_bytes":0.0,"io_w_bytes":1377.08,"metric":"icount","uncertain_reason":null}}
        {"computed_in":{"nanos":50472,"secs":0},"name":"ApplyBlock","result":{"gas":9059500000,"instructions":71583.0,"io_r_bytes":0.0,"io_w_bytes":19.0,"metric":"icount","uncertain_reason":"HIGH-VARIANCE"}}
        "#;
        let expected = [
            EstimationRow {
                name: "ActionReceiptCreation".to_owned(),
                gas: 240650158750.0,
                parameter: None,
                wall_clock_time: None,
                icount: Some(1860478.51),
                io_read: Some(0.0),
                io_write: Some(1377.08),
                uncertain_reason: None,
                commit_hash: "53a3ccf3ef07".to_owned(),
            },
            EstimationRow {
                name: "ApplyBlock".to_owned(),
                gas: 9059500000.0,
                parameter: None,
                wall_clock_time: None,
                icount: Some(71583.0),
                io_read: Some(0.0),
                io_write: Some(19.0),
                uncertain_reason: Some("HIGH-VARIANCE".to_owned()),
                commit_hash: "53a3ccf3ef07".to_owned(),
            },
        ];
        let info = ImportConfig {
            commit_hash: Some("53a3ccf3ef07".to_owned()),
            protocol_version: Some(0),
        };
        assert_import(input, &info, &expected, Metric::ICount);
    }
    #[track_caller]
    fn assert_import(
        input: &str,
        info: &ImportConfig,
        expected_output: &[EstimationRow],
        metric: Metric,
    ) {
        let db = Db::test();
        db.import_json_lines(info, input).unwrap();
        let output = EstimationRow::select_by_commit_and_metric(
            &db,
            info.commit_hash.as_ref().unwrap(),
            metric,
        )
        .unwrap();
        assert_eq!(expected_output, output);
    }
}
