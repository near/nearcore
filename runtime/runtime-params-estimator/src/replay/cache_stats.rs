use anyhow::Context;
use std::collections::BTreeMap;
use std::io::Write;

/// Keeps track of cache statistics and prints them on demand.
#[derive(Default)]
pub(super) struct CacheStats {
    /// Count of all DB get requests, from guest or host.
    num_get: u64,
    /// Count of all DB set requests, from guest or host.
    num_set: u64,
    /// Sum of all DB get sizes, from guest or host.
    total_size_get: u64,
    /// Sum of all DB set sizes, from guest or host.
    total_size_set: u64,

    /// Count of all storage reads. (can only be from inside guest program)
    num_read: u64,
    /// Count of all storage writes. (can only be from inside guest program)
    num_write: u64,
    /// Sum of all storage reads sizes. (can only be from inside guest program)
    total_size_read: u64,
    /// Sum of all storage writes sizes. (can only be from inside guest program)
    total_size_write: u64,

    /// Hits in the chunk cache. (can only be from inside guest program)
    num_tn_chunk_cache_hit: u64,
    /// Hits in the shard cache, from inside guest program.
    num_tn_shard_cache_hit_guest: u64,
    /// Misses in the shard cache, from inside guest program.
    num_tn_shard_cache_miss_guest: u64,
    /// All trie node accesses that the user pays for as being fetched from DB.
    /// Includes shard cache misses and hits, but no chunk cache hits.
    num_tn_db_paid: u64,

    /// Hits in the shard cache, requested by host.
    num_tn_shard_cache_hit_host: u64,
    /// Misses in the shard cache, requested by host.
    num_tn_shard_cache_miss_host: u64,

    /// Misses in the shard cache anywhere, due to the size being too large.
    num_tn_shard_cache_too_large: u64,
}

impl CacheStats {
    /// Digest cache statistics written directly on a single DB operations.
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

    /// Digest cache statistics written on a storage operation, such cache hits.
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

        self.num_tn_chunk_cache_hit += tn_mem_reads;
        self.num_tn_shard_cache_hit_guest += tn_shard_cache_hits;
        self.num_tn_db_paid += tn_db_reads;
        self.num_tn_shard_cache_too_large += tn_shard_cache_too_large;
        self.num_tn_shard_cache_miss_guest += tn_shard_cache_misses;

        Ok(())
    }

    /// Digest cache statistics not part of storage operations but some other label.
    ///
    /// Labels to look out for here are `process_receipt`, `process_transaction`, or
    /// even just `apply`. They all access the trie also outside of guest-spawned
    /// storage operations.
    pub(super) fn eval_generic_label(&mut self, dict: &BTreeMap<&str, &str>) {
        let tn_shard_cache_hits =
            dict.get("shard_cache_hit").map(|s| s.parse().unwrap()).unwrap_or(0);
        let tn_shard_cache_misses =
            dict.get("shard_cache_miss").map(|s| s.parse().unwrap()).unwrap_or(0);
        let tn_shard_cache_too_large =
            dict.get("shard_cache_too_large").map(|s| s.parse().unwrap()).unwrap_or(0);

        // there is no chunk cache update here, as we are not in a smart contract execution
        self.num_tn_shard_cache_hit_host += tn_shard_cache_hits;
        self.num_tn_shard_cache_too_large += tn_shard_cache_too_large;
        self.num_tn_shard_cache_miss_host += tn_shard_cache_misses;
    }

    pub(super) fn print(&self, out: &mut dyn Write, indent: usize) -> anyhow::Result<()> {
        writeln!(
            out,
            "{:indent$}DB GET            {:>5} requests for a total of {:>8} B",
            "", self.num_get, self.total_size_get
        )?;
        writeln!(
            out,
            "{:indent$}DB SET            {:>5} requests for a total of {:>8} B",
            "", self.num_set, self.total_size_set
        )?;
        writeln!(
            out,
            "{:indent$}STORAGE READ      {:>5} requests for a total of {:>8} B",
            "", self.num_read, self.total_size_read
        )?;
        writeln!(
            out,
            "{:indent$}STORAGE WRITE     {:>5} requests for a total of {:>8} B",
            "", self.num_write, self.total_size_write
        )?;
        writeln!(
            out,
            "{:indent$}TRIE NODES (guest) {:>4} /{:>4} /{:>4}  (chunk-cache/shard-cache/DB)",
            "",
            self.num_tn_chunk_cache_hit,
            self.num_tn_shard_cache_hit_guest,
            self.num_tn_shard_cache_miss_guest
        )?;
        writeln!(
            out,
            "{:indent$}TRIE NODES (host)        {:>4} /{:>4}  (shard-cache/DB)",
            "", self.num_tn_shard_cache_hit_host, self.num_tn_shard_cache_miss_host
        )?;
        Self::print_cache_rate(
            out,
            indent,
            "SHARD CACHE",
            self.num_tn_shard_cache_hit_guest + self.num_tn_shard_cache_hit_host,
            self.num_tn_shard_cache_miss_guest + self.num_tn_shard_cache_miss_host,
            Some((self.num_tn_shard_cache_too_large, "too large nodes")),
        )?;
        Self::print_cache_rate(
            out,
            indent,
            "CHUNK CACHE",
            self.num_tn_chunk_cache_hit,
            self.num_tn_db_paid,
            None,
        )?;
        Ok(())
    }

    /// Given hit and miss counts, print hit rate for a cache.
    ///
    /// Provide name, hit count, and miss count for basic data.
    /// Additionally, a number of "special misses" is reported
    /// sep
    fn print_cache_rate(
        out: &mut dyn Write,
        indent: usize,
        cache_name: &str,
        hits: u64,
        misses: u64,
        special_misses: Option<(u64, &str)>,
    ) -> anyhow::Result<()> {
        let total = hits + misses;
        if total > 0 {
            write!(
                out,
                "{:indent$}{cache_name:<16}   {:>6.2}% hit rate",
                "",
                hits as f64 / total as f64 * 100.0,
            )?;
            if let Some((special_misses, msg)) = special_misses {
                if special_misses > 0 {
                    debug_assert!(special_misses <= misses, "failed: {special_misses} <= {misses}");
                    write!(
                        out,
                        ", {:>6.2}% if removing {} {msg} from total",
                        hits as f64 / (total - special_misses) as f64 * 100.0,
                        special_misses,
                    )?;
                }
            }
            writeln!(out)?;
        } else {
            writeln!(out, "{:indent$}{cache_name} not accessed", "")?;
        }
        Ok(())
    }
}
