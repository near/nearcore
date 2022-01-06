use nearcore::{get_default_home, get_store_path};
use serde::Serialize;
use std::collections::HashMap;
use std::process::Command;

#[derive(Serialize, Debug)]
struct Data {
    col: String,
    entries: u64,
    estimated_table_size: u64,
    // Total size of all keys in bytes.
    raw_key_size: u64,
    // Total size of all values in bytes.
    raw_value_size: u64,
}

// SST file dump keys we use to collect statistics.
const SST_FILE_DUMP_LINES: &[&str] = &[
    "column family name",
    "# entries",
    "(estimated) table size",
    "raw key size",
    "raw value size",
];

impl Data {
    pub fn from_sst_file_dump(lines: &[&str]) -> Self {
        // Mapping from SST file dump key to value.
        let mut values: HashMap<&str, &str> = Default::default();

        for line in lines {
            let split_result = line.split_once(':');
            let (line, value) = match split_result {
                None => continue,
                Some((prefix, suffix)) => (prefix, suffix),
            };

            for sst_file_line in SST_FILE_DUMP_LINES {
                if line == sst_file_line {
                    let prev = values.insert(line, value);
                    assert!(
                        prev.is_none(),
                        "Line {} was presented twice and contains values {} and {}",
                        line,
                        prev.unwrap(),
                        value
                    );
                }
            }
        }

        Data {
            col: String::from(values.get(SST_FILE_DUMP_LINES[0]).unwrap().clone()),
            entries: values.get(SST_FILE_DUMP_LINES[1]).unwrap().parse::<u64>().unwrap(),
            estimated_table_size: values
                .get(SST_FILE_DUMP_LINES[2])
                .unwrap()
                .parse::<u64>()
                .unwrap(),
            raw_key_size: values.get(SST_FILE_DUMP_LINES[3]).unwrap().parse::<u64>().unwrap(),
            raw_value_size: values.get(SST_FILE_DUMP_LINES[4]).unwrap().parse::<u64>().unwrap(),
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.entries += other.entries;
        self.estimated_table_size += other.estimated_table_size;
        self.raw_key_size += other.raw_key_size;
        self.raw_value_size += other.raw_value_size;
    }
}

fn main() -> anyhow::Result<()> {
    let home_dir = get_default_home();
    let store_dir = get_store_path(&home_dir);
    let mut cmd = Command::new("sst_dump");
    cmd.arg("--file").arg(&store_dir).arg("--show_properties").arg("--command=none"); // For some reason, adding this argument makes execution 20x faster
    tracing::info!("Running {:?} ...", cmd);
    let output = cmd.output()?;
    if !output.status.success() {
        anyhow::bail!("failed to run sst_dump, exit status {}", output.status);
    }

    tracing::info!("Parsing output ...");
    let out = std::str::from_utf8(&output.stdout).unwrap();
    let lines: Vec<&str> = out.lines().collect();
    let mut column_data: HashMap<String, Data> = HashMap::new();
    for sst_file_slice in lines.split(|line| line.contains("Process")) {
        let data = Data::from_sst_file_dump(sst_file_slice);
        if let Some(x) = column_data.get_mut(&data.col) {
            x.merge(&data);
        } else {
            column_data.insert(data.col.clone(), data);
        }
    }

    let mut column_data_list: Vec<&Data> = column_data.values().collect();
    column_data_list.sort_by_key(|data| std::cmp::Reverse(data.estimated_table_size));
    tracing::info!("Printing stats ...");
    println!("{}", serde_json::to_string_pretty(&column_data_list).unwrap());
    Ok(())
}
