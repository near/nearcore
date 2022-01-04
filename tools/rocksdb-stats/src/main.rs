use nearcore::get_default_home;
use std::collections::HashMap;
use std::process::Command;

#[derive(Default, Debug)]
struct Data {
    col: String,
    entries: u64,
    estimated_table_size: u64,
    // Total size of all keys in bytes.
    raw_key_size: u64,
    // Total size of all values in bytes.
    raw_value_size: u64,
}

impl Data {
    pub fn merge(&mut self, other: &Data) {
        self.entries += other.entries;
        self.estimated_table_size += other.estimated_table_size;
        self.raw_key_size += other.raw_key_size;
        self.raw_value_size += other.raw_value_size;
    }
}

fn parse_sst_file_dump(lines: &[&str]) -> Data {
    let mut data = Data::default();
    for line in lines {
        let split_line: Vec<&str> = line.split(':').collect();
        if split_line.len() < 2 {
            continue;
        }
        let line = split_line[0].trim();
        let value = split_line[1].trim();

        if line.starts_with("column family name") {
            data.col = value.to_string();
        } else if line.starts_with("# entries") {
            data.entries = value.parse::<u64>().unwrap();
        } else if line.starts_with("(estimated) table size") {
            data.estimated_table_size = value.parse::<u64>().unwrap();
        } else if line.starts_with("raw key size") {
            data.raw_key_size = value.parse::<u64>().unwrap();
        } else if line.starts_with("raw value size") {
            data.raw_value_size = value.parse::<u64>().unwrap();
        }
    }
    data
}

fn main() {
    let home_dir = get_default_home();
    let output = Command::new("sst_dump")
        .arg(format!("--file={}", home_dir.to_str().unwrap()))
        .arg("--show_properties")
        .output()
        .expect("sst_dump command failed to start");

    let out = String::from_utf8(output.stdout).unwrap();
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
        let data = parse_sst_file_dump(&lines[sst_file_breaks[i - 1]..sst_file_breaks[i]]);
        if let Some(x) = column_data.get_mut(&data.col) {
            x.merge(&data);
        } else {
            column_data.insert(data.col.clone(), data);
        }
    }

    let mut column_data_list: Vec<&Data> = column_data.values().collect();
    column_data_list.sort_by_key(|data| -(data.estimated_table_size as i64));
    for column_data in column_data_list {
        println!("{:#?}", column_data);
    }
}
