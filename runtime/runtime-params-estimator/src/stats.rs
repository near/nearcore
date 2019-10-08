use gnuplot::{AxesCommon, Caption, Color, DotDotDash, Figure, Graph, LineStyle, PointSymbol};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

/// Stores measurements per block.
#[derive(Default)]
pub struct Measurements {
    data: BTreeMap<usize, Vec<Duration>>,
    title: &'static str,
}

impl Measurements {
    pub fn new(title: &'static str) -> Self {
        Self { title, data: Default::default() }
    }
    /// Record measurement.
    pub fn record_measurement(&mut self, block_size: usize, value: Duration) {
        self.data.entry(block_size).or_insert_with(Vec::new).push(value)
    }

    /// Flatten all measurements into nanosec per operation.
    fn flat_nanosec(&self) -> Vec<u128> {
        self.data
            .iter()
            .map(|(block_size, times)| {
                let block_size = *block_size;
                times.iter().map(move |t| t.as_nanos() / (block_size as u128))
            })
            .flatten()
            .collect()
    }

    pub fn stats(&self) -> DataStats {
        DataStats::from_nanos(self.flat_nanosec())
    }

    pub fn plot(measurements: &[Self]) {
        const COLORS: &[&str] = &["red", "orange", "cyan", "blue", "violet"];
        const POINTS: &[char] = &['o', 'x', '*', 's', 't', 'd', 'r'];
        if measurements.len() > COLORS.len() {
            println!("Not enough hardcoded colors.");
        }

        let mut fg = Figure::new();
        let axes = fg
            .axes2d()
            .set_title("A plot", &[])
            .set_legend(Graph(0.5), Graph(0.9), &[], &[])
            .set_x_label("Block size", &[])
            .set_y_label("Duration micros", &[])
            .set_grid_options(true, &[LineStyle(DotDotDash), Color("black")])
            .set_x_log(Some(2.0))
            .set_x_grid(true)
            .set_y_log(Some(2.0))
            .set_y_grid(true);

        for (i, measurement) in measurements.iter().enumerate() {
            let mut xs = vec![];
            let mut ys = vec![];
            let mut mean_xs = vec![];
            let mut mean_ys = vec![];
            for (block_size, durations) in &measurement.data {
                for duration in durations {
                    xs.push(*block_size as u64);
                    ys.push(duration.as_micros() as u64 / *block_size as u64);
                }
                mean_xs.push(*block_size as u64);
                mean_ys.push(
                    durations.iter().map(|d| d.as_micros() as u64).sum::<u64>()
                        / durations.len() as u64
                        / *block_size as u64,
                );
            }
            axes.points(
                xs.as_slice(),
                ys.as_slice(),
                &[
                    Color(COLORS[i % COLORS.len()]),
                    PointSymbol(POINTS[i % POINTS.len()]),
                    Caption(measurement.title),
                ],
            )
            .lines_points(
                mean_xs.as_slice(),
                mean_ys.as_slice(),
                &[Color(COLORS[i % COLORS.len()]), PointSymbol('.')],
            );
        }
        fg.show().unwrap();
    }

    pub fn save_to_csv(measurements: &[Self], path: &Path) {
        let mut writer = csv::Writer::from_path(path).unwrap();
        writer.write_record(&["measurement", "mean", "stddev", "5-ile", "95-ile"]).unwrap();
        for measurement in measurements {
            let stats = measurement.stats();
            writer
                .write_record(&[
                    format!("{}", stats.mean.as_micros()),
                    format!("{}", stats.stddev.as_micros()),
                    format!("{}", stats.ile5.as_micros()),
                    format!("{}", stats.ile95.as_micros()),
                ])
                .unwrap();
        }
        writer.flush().unwrap();
    }
}

pub struct DataStats {
    pub mean: Duration,
    pub stddev: Duration,
    pub ile5: Duration,
    pub ile95: Duration,
}

impl DataStats {
    pub fn from_nanos(mut nanos: Vec<u128>) -> Self {
        nanos.sort();
        let mean = (nanos.iter().sum::<u128>() / (nanos.len() as u128)) as i128;
        let stddev2 = nanos.iter().map(|x| (*x as i128 - mean) * (*x as i128 - mean)).sum::<i128>()
            / (nanos.len() as i128 - 1);
        let stddev = (stddev2 as f64).sqrt() as u128;
        let ile5 = nanos[nanos.len() * 5 / 100];
        let ile95 = nanos[nanos.len() * 95 / 100];

        Self {
            mean: Duration::from_nanos(mean as u64),
            stddev: Duration::from_nanos(stddev as u64),
            ile5: Duration::from_nanos(ile5 as u64),
            ile95: Duration::from_nanos(ile95 as u64),
        }
    }
}

impl std::fmt::Display for DataStats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.mean.as_secs() > 100 {
            write!(
                f,
                "{}s±{}s ({}s, {}s)",
                self.mean.as_secs(),
                self.stddev.as_secs(),
                self.ile5.as_secs(),
                self.ile95.as_secs()
            )
        } else if self.mean.as_millis() > 100 {
            write!(
                f,
                "{}ms±{}ms ({}ms, {}ms)",
                self.mean.as_millis(),
                self.stddev.as_millis(),
                self.ile5.as_millis(),
                self.ile95.as_millis()
            )
        } else if self.mean.as_micros() > 100 {
            write!(
                f,
                "{}μ±{}μ ({}μ, {}μ)",
                self.mean.as_micros(),
                self.stddev.as_micros(),
                self.ile5.as_micros(),
                self.ile95.as_micros()
            )
        } else {
            write!(
                f,
                "{}n±{}n ({}n, {}n)",
                self.mean.as_nanos(),
                self.stddev.as_nanos(),
                self.ile5.as_nanos(),
                self.ile95.as_nanos()
            )
        }
    }
}
