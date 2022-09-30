use anyhow::Context;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::str::SplitWhitespace;
use tracing::log::error;

use self::fold_db_ops::FoldDbOps;
use self::gas_charges::ChargedVsFree;

mod cache_stats;
mod fold_db_ops;
mod gas_charges;

#[derive(clap::Parser)]
pub(crate) struct ReplayCmd {
    trace: PathBuf,
    #[clap(subcommand)]
    mode: ReplayMode,
    /// Only show data for a specific smart contract, specified by account id.
    #[clap(long)]
    account: Option<String>,
}

#[derive(Clone, Copy, clap::Subcommand, Debug)]
pub(crate) enum ReplayMode {
    /// Print DB accesses and cache statistics for the entire trace.
    CacheStats,
    /// Print DB accesses per receipt.
    ReceiptDbStats,
    /// Print DB accesses and cache statistics per receipt.
    ReceiptCacheStats,
    /// Print DB accesses per chunk.
    ChunkDbStats,
    /// Print DB accesses and cache statistics per chunk.
    ChunkCacheStats,
    /// Go over DB operations and print how much of it is paid for with gas.
    GasCharges,
}

impl ReplayCmd {
    pub(crate) fn run(&self, out: &mut dyn Write) -> anyhow::Result<()> {
        let file = File::open(&self.trace)?;

        let mut visitor = self.build_visitor();

        for line in io::BufReader::new(file).lines() {
            let line = line?;
            if let Err(e) = visitor.eval_line(out, &line) {
                error!("ERROR: {e} for input line: {line}");
            }
        }
        visitor.flush(out)?;

        Ok(())
    }

    fn build_visitor(&self) -> Box<dyn Visitor> {
        match &self.mode {
            ReplayMode::CacheStats => {
                Box::new(FoldDbOps::new().with_cache_stats().account_filter(self.account.clone()))
            }
            ReplayMode::ChunkDbStats => {
                if self.account.is_some() {
                    unimplemented!("account filter does not work with per-chunk statistics");
                }
                Box::new(FoldDbOps::new().chunks())
            }
            ReplayMode::ChunkCacheStats => {
                if self.account.is_some() {
                    unimplemented!("account filter does not work with per-chunk statistics");
                }
                Box::new(FoldDbOps::new().chunks().with_cache_stats())
            }
            ReplayMode::ReceiptDbStats => {
                Box::new(FoldDbOps::new().receipts().account_filter(self.account.clone()))
            }
            ReplayMode::ReceiptCacheStats => Box::new(
                FoldDbOps::new().receipts().with_cache_stats().account_filter(self.account.clone()),
            ),
            ReplayMode::GasCharges => {
                if self.account.is_some() {
                    unimplemented!("account filter does not work with gas charges");
                }
                Box::new(ChargedVsFree::default())
            }
        }
    }
}

trait Visitor {
    /// The root entry point of the visitors.
    ///
    /// This function takes a raw input line as input without any preprocessing.
    /// A visitor may choose to overwrite this function for full control but the
    /// intention is that the default implementation takes over the basic
    /// parsing and visitor implementations defined their behaviour using the
    /// other trait methods.
    fn eval_line(&mut self, out: &mut dyn Write, line: &str) -> anyhow::Result<()> {
        if let Some(indent) = line.chars().position(|c| !c.is_whitespace()) {
            let mut tokens = line.split_whitespace();
            if let Some(keyword) = tokens.next() {
                match keyword {
                    "GET" | "SET" | "UPDATE_RC" => {
                        let col = tokens.next().context("missing column field in DB operation")?;
                        let mut key_str = tokens.next().context("missing key in DB operation")?;
                        if key_str.starts_with('"') {
                            key_str = &key_str[1..key_str.len() - 1];
                        }
                        let key = bs58::decode(key_str).into_vec()?;
                        let dict = extract_key_values(tokens)?;
                        let size: Option<u64> = dict.get("size").map(|s| s.parse()).transpose()?;
                        self.eval_db_op(out, indent, keyword, size, &key, col)?;
                    }
                    "storage_read" | "storage_write" | "storage_remove" | "storage_has_key" => {
                        let op = tokens.next();
                        if op.is_none() {
                            return Ok(());
                        }

                        let dict = extract_key_values(tokens)?;
                        self.eval_storage_op(out, indent, keyword, &dict)?;
                    }
                    other_label => {
                        let dict = extract_key_values(tokens)?;
                        self.eval_label(out, indent, other_label, &dict)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Gets called for every trie storage operation.
    ///
    /// A storage operation is a layer above DB operations, one storage
    /// operation can cause many DB operations, or it could be resolved from
    /// cache without any DB operations.
    fn eval_storage_op(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        op: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        let (_, _, _, _) = (out, indent, op, dict);
        Ok(())
    }

    /// Gets called for every DB operation.
    fn eval_db_op(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        op: &str,
        size: Option<u64>,
        key: &[u8],
        col: &str,
    ) -> anyhow::Result<()> {
        if col == "State" {
            self.eval_state_db_op(out, indent, op, size, key)
        } else {
            Ok(())
        }
    }

    /// Gets called for every DB operation on the state column.
    fn eval_state_db_op(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        op: &str,
        size: Option<u64>,
        key: &[u8],
    ) -> anyhow::Result<()> {
        let (_, _, _, _, _) = (out, indent, op, size, key);
        Ok(())
    }

    /// Opening spans that are not storage or DB operations.
    fn eval_label(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        label: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        let (_, _, _, _) = (out, indent, label, dict);
        Ok(())
    }

    /// Print accumulated internal state.
    ///
    /// This function is called at the end of a replayed trace. Visitor
    /// implementation usually choose to call this more often. For example, once
    /// for each receipt.
    fn flush(&mut self, out: &mut dyn Write) -> anyhow::Result<()> {
        let _ = out;
        Ok(())
    }
}

fn extract_key_values<'a>(
    mut tokens: SplitWhitespace<'a>,
) -> anyhow::Result<BTreeMap<&'a str, &'a str>> {
    let mut dict = BTreeMap::new();
    while let Some(key_val) = tokens.next() {
        let (key, value) =
            key_val.split_once('=').context("key-value pair delimited by `=` expected")?;
        dict.insert(key, value);
    }
    Ok(dict)
}

#[cfg(test)]
mod tests {
    use super::{ReplayCmd, ReplayMode};

    // These inputs are real mainnet traffic for the given block heights.
    // Each trace contains two chunks if one shard.
    // In combination, they cover an interesting variety of content.
    //  Shard 0: Many receipts, but no function calls.
    //  Shard 1: Empty chunks without txs / receipts.
    //  Shard 2: Some receipts and txs, but no function calls.
    //  Shard 3: Many function calls with lots of storage accesses.
    const INPUT_TRACES: &[&str] = &[
        "75220100-75220101.s0.io_trace",
        "75220100-75220101.s1.io_trace",
        "75220100-75220101.s2.io_trace",
        "75220100-75220101.s3.io_trace",
    ];

    #[test]
    fn test_cache_stats() {
        check_replay_mode(ReplayMode::CacheStats);
    }

    #[test]
    fn test_chunk_cache_stats() {
        check_replay_mode(ReplayMode::ChunkCacheStats);
    }

    #[test]
    fn test_chunk_db_stats() {
        check_replay_mode(ReplayMode::ChunkDbStats);
    }

    #[test]
    fn test_gas_charges() {
        check_replay_mode(ReplayMode::GasCharges);
    }

    #[test]
    fn test_receipt_cache_stats() {
        check_replay_mode(ReplayMode::ReceiptCacheStats);
    }

    #[test]
    fn test_receipt_db_stats() {
        check_replay_mode(ReplayMode::ReceiptDbStats);
    }

    #[track_caller]
    fn check_replay_mode(mode: ReplayMode) {
        for trace_name in INPUT_TRACES {
            let dir = env!("CARGO_MANIFEST_DIR");
            let trace_path = std::path::Path::new(dir).join("res").join(trace_name);
            let cmd = ReplayCmd { trace: trace_path, mode, account: None };
            let mut buffer = Vec::new();
            cmd.run(&mut buffer).unwrap_or_else(|e| {
                panic!("command should not fail for input {trace_name}, failure was {e}")
            });
            let output = String::from_utf8(buffer).unwrap_or_else(|e| {
                panic!("invalid output for input {trace_name}, failure was {e}")
            });
            insta::assert_snapshot!(format!("{mode:?}-{trace_name}"), output);
        }
    }
}
