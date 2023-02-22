use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(serde::Serialize, Debug)]
struct SSTFileData {
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

/// Statistics about a single SST file.
impl SSTFileData {
    fn for_sst_file(file_path: &Path) -> anyhow::Result<Self> {
        let mut file_arg = std::ffi::OsString::from("--file=");
        file_arg.push(file_path);

        let mut cmd = Command::new("sst_dump");
        // For some reason, adding --command=none makes execution 20x faster.
        cmd.arg(file_arg).arg("--show_properties").arg("--command=none");

        let output = cmd.output()?;
        anyhow::ensure!(
            output.status.success(),
            "failed to run sst_dump, {}, stderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );

        Self::from_sst_file_dump(std::str::from_utf8(&output.stdout).unwrap())
    }

    fn from_sst_file_dump(output: &str) -> anyhow::Result<Self> {
        // Mapping from SST file dump key to value.
        let mut values: HashMap<&str, Option<&str>> =
            SST_FILE_DUMP_LINES.iter().map(|key| (*key, None)).collect();

        for line in output.lines() {
            let (key, value) = match line.split_once(':') {
                None => continue,
                Some((key, value)) => (key.trim(), value.trim()),
            };

            if let Some(entry) = values.get_mut(key) {
                if let Some(prev) = entry {
                    anyhow::bail!(
                        "Key {key} was presented twice and \
                                   contains values {prev} and {value}"
                    );
                }
                let _ = entry.insert(value);
            }
        }

        let get_u64 =
            |idx| values.get(SST_FILE_DUMP_LINES[idx]).unwrap().unwrap().parse::<u64>().unwrap();

        Ok(SSTFileData {
            col: values.get(SST_FILE_DUMP_LINES[0]).unwrap().unwrap().to_owned(),
            entries: get_u64(1),
            estimated_table_size: get_u64(2),
            raw_key_size: get_u64(3),
            raw_value_size: get_u64(4),
        })
    }

    fn merge(&mut self, other: &Self) {
        self.entries += other.entries;
        self.estimated_table_size += other.estimated_table_size;
        self.raw_key_size += other.raw_key_size;
        self.raw_value_size += other.raw_value_size;
    }
}

/// Merged statistics for all columns.
struct ColumnsData(HashMap<String, SSTFileData>);

impl ColumnsData {
    fn new() -> Self {
        Self(Default::default())
    }

    fn add_sst_data(&mut self, data: SSTFileData) {
        self.0.entry(data.col.clone()).and_modify(|entry| entry.merge(&data)).or_insert(data);
    }

    fn into_vec(self) -> Vec<SSTFileData> {
        let mut values: Vec<_> = self.0.into_values().collect();
        values.sort_by_key(|data| std::cmp::Reverse(data.estimated_table_size));
        values
    }
}

pub fn get_rocksdb_stats(store_dir: &Path, file: Option<PathBuf>) -> anyhow::Result<()> {
    let mut data = ColumnsData::new();

    for entry in std::fs::read_dir(store_dir)? {
        let entry = entry?;
        let file_name = std::path::PathBuf::from(entry.file_name());
        if file_name.extension().map_or(false, |ext| ext == "sst") {
            let file_path = store_dir.join(file_name);
            eprintln!("Processing ‘{}’...", file_path.display());
            data.add_sst_data(SSTFileData::for_sst_file(&file_path)?);
        }
    }

    eprintln!("Dumping stats ...");
    let result = serde_json::to_string_pretty(&data.into_vec()).unwrap();
    match file {
        None => println!("{}", result),
        Some(file) => std::fs::write(file, result)?,
    }
    Ok(())
}
