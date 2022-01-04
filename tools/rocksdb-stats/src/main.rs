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
const SST_FILE_DUMP_LINES: [&str; 5] =
    ["column family name", "# entries", "(estimated) table size", "raw key size", "raw value size"];

impl Data {
    pub fn from_sst_file_dump(lines: &[&str]) -> Self {
        // Mapping from SST file dump key to value.
        let mut values: HashMap<&str, &str> = Default::default();

        for line in lines {
            let split_line: Vec<&str> = line.split(':').collect();
            if split_line.len() < 2 {
                continue;
            }
            let line = split_line[0].trim();
            let value = split_line[1].trim();

            for sst_file_line in SST_FILE_DUMP_LINES {
                if line == sst_file_line {
                    if values.contains_key(line) {
                        panic!(
                            "Line {} was presented twice and contains values {} and {}",
                            line,
                            values.get(line).unwrap(),
                            value
                        );
                    } else {
                        values.insert(line, value);
                    }
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

fn main() {
    let home_dir = get_default_home();
    let store_dir = get_store_path(&home_dir);
    let mut cmd = Command::new("sst_dump");
    cmd.arg(format!("--file={}", store_dir.to_str().unwrap()))
        .arg("--show_properties")
        .arg("--command=none"); // For some reason, adding this argument makes execution 20x faster
    eprintln!("Running {:?} ...", cmd);
    let output = cmd.output().expect("sst_dump command failed to start");

    eprintln!("Parsing output ...");
    let out = std::str::from_utf8(&output.stdout).unwrap();
    let mut sst_file_breaks: Vec<usize> = vec![];
    let lines: Vec<&str> = out.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        if line.contains("Process") {
            sst_file_breaks.push(i);
        }
    }
    sst_file_breaks.push(lines.len());

    let mut column_data: HashMap<String, Data> = HashMap::new();

    for i in 1..sst_file_breaks.len() {
        let data = Data::from_sst_file_dump(&lines[sst_file_breaks[i - 1]..sst_file_breaks[i]]);
        if let Some(x) = column_data.get_mut(&data.col) {
            x.merge(&data);
        } else {
            column_data.insert(data.col.clone(), data);
        }
    }

    let mut column_data_list: Vec<&Data> = column_data.values().collect();
    column_data_list.sort_by_key(|data| u64::MAX - data.estimated_table_size);
    eprintln!("Printing stats ...");
    println!("{}", serde_json::to_string(&column_data_list).unwrap());
}
