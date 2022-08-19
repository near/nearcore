use anyhow::Context;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct CacheStats {
    num_get: u64,
    num_set: u64,
    total_size_get: u64,
    total_size_set: u64,

    num_read: u64,
    num_write: u64,
    total_size_read: u64,
    total_size_write: u64,

    num_tn_shard_cache: u64,
    num_tn_chunk_cache: u64,
    num_tn_db: u64,

    num_tn_shard_cache_miss: u64,
    num_tn_shard_cache_too_large: u64,
}

impl CacheStats {
    pub(super) fn eval_db_op(&mut self, op: &str, size: Option<u64>) {
        match op {
            "GET" => {
                self.num_get += 1;
                self.total_size_get += size.unwrap_or(0);
            }
            "SET" => {
                self.num_set += 1;
                self.total_size_set += size.unwrap_or(0);
            }
            _ => {
                // nop
            }
        }
    }

    pub(super) fn eval_storage_op(
        &mut self,
        storage_operation: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        let size = if storage_operation == "storage_has_key" {
            0
        } else {
            dict.get("size").unwrap_or(&"0").parse()?
        };
        let mut tn_db_reads: u64 = dict
            .get("tn_db_reads")
            .map(|s| s.parse().unwrap())
            .context("no tn_db_reads on storage op")?;
        let mut tn_mem_reads: u64 = dict
            .get("tn_mem_reads")
            .map(|s| s.parse().unwrap())
            .context("no tn_mem_reads on storage op")?;

        let tn_shard_cache_hits =
            dict.get("shard_cache_hit").map(|s| s.parse().unwrap()).unwrap_or(0);
        let tn_shard_cache_misses =
            dict.get("shard_cache_miss").map(|s| s.parse().unwrap()).unwrap_or(0);
        let tn_shard_cache_too_large =
            dict.get("shard_cache_too_large").map(|s| s.parse().unwrap()).unwrap_or(0);

        match storage_operation {
            "storage_read" | "storage_has_key" => {
                self.num_read += 1;
                self.total_size_read += size;
                // We are currently counting one node too little, see
                // https://github.com/near/nearcore/issues/6225. But we don't
                // know where, could be either tn_db_reads or tn_mem_reads. But
                // we know that tn_db_reads = shard_cache_hits +
                // shard_cache_misses. So we can correct for it here.
                if tn_db_reads < tn_shard_cache_misses + tn_shard_cache_hits {
                    tn_db_reads += 1;
                } else {
                    tn_mem_reads += 1;
                }
                debug_assert_eq!(tn_db_reads, tn_shard_cache_misses + tn_shard_cache_hits)
            }
            "storage_write" => {
                self.num_write += 1;
                self.total_size_write += size;
            }
            _ => {}
        }

        self.num_tn_chunk_cache += tn_mem_reads;
        self.num_tn_shard_cache += tn_shard_cache_hits;
        self.num_tn_db += tn_db_reads - tn_shard_cache_hits;
        self.num_tn_shard_cache_too_large += tn_shard_cache_too_large;
        self.num_tn_shard_cache_miss += tn_shard_cache_misses;

        Ok(())
    }

    pub(super) fn print(&self, indent: usize) {
        println!(
            "{:indent$}DB GET        {:>5} requests for a total of {:>8} B",
            "", self.num_get, self.total_size_get
        );
        println!(
            "{:indent$}DB SET        {:>5} requests for a total of {:>8} B",
            "", self.num_set, self.total_size_set
        );
        println!(
            "{:indent$}STORAGE READ  {:>5} requests for a total of {:>8} B",
            "", self.num_read, self.total_size_read
        );
        println!(
            "{:indent$}STORAGE WRITE {:>5} requests for a total of {:>8} B",
            "", self.num_write, self.total_size_write
        );
        println!(
            "{:indent$}TRIE NODES    {:>4} /{:>4} /{:>4}  (chunk-cache/shard-cache/DB)",
            "", self.num_tn_chunk_cache, self.num_tn_shard_cache, self.num_tn_db
        );
        Self::print_cache_rate(
            indent,
            "SHARD CACHE",
            self.num_tn_shard_cache,
            self.num_tn_shard_cache_miss,
            self.num_tn_shard_cache_too_large,
            "too large nodes",
        );
        Self::print_cache_rate(
            indent,
            "CHUNK CACHE",
            self.num_tn_chunk_cache,
            self.num_tn_shard_cache + self.num_tn_db,
            self.num_tn_shard_cache,
            "shard cache hits",
        );
    }

    fn print_cache_rate(
        indent: usize,
        cache_name: &str,
        hits: u64,
        misses: u64,
        special_misses: u64,
        special_misses_msg: &str,
    ) {
        let total = hits + misses;
        if special_misses > 0 {
            println!(
                "{:indent$}{cache_name:<16}   {:>6.2}% hit rate, {:>6.2}% if removing {} {special_misses_msg} from total",
                "",
                hits as f64 / total as f64 * 100.0,
                hits as f64 / (total - special_misses) as f64 * 100.0,
                special_misses,
            );
        } else if total > 0 {
            println!(
                "{:indent$}{cache_name:<16} {:>6.2}% hit rate",
                "",
                hits as f64 / total as f64 * 100.0,
            );
        } else {
            println!("{:indent$}{cache_name} not accessed", "");
        }
    }
}
