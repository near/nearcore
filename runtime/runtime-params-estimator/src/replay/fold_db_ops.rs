use super::cache_stats::CacheStats;
use super::Visitor;
use std::collections::BTreeMap;
use std::io::Write;

const EMPTY_STATE: &str = "states must never be empty";

/// A visitor that keeps track of DB operations and aggregates it by specific labels.
pub(super) struct FoldDbOps {
    /// Labels at which to break aggregation.
    fold_anchors: Vec<String>,
    /// Fields to display on each output, if they are available in the trace.
    printed_fields: Vec<String>,
    /// Print the DB operations that are at indentation 0, not associated with a span.
    print_top_level: bool,
    /// Only show data for a specific smart contract, specified by account id.
    /// Currently only applies within receipts.
    account_filter: Option<String>,
    /// Skip all input lines that have deeper indentation.
    skip_indent: Option<usize>,
    /// Optionally collect and print detailed statistics for cache hits and misses.
    track_caches: bool,
    /// Keeps track of current block.
    block_hash: Option<String>,
    /// Stack of states.
    states: Vec<State>,
}

#[derive(Default)]
struct State {
    /// The indent at which this state started
    indent: usize,
    /// Keeps track of operations per DB column.
    ops_cols: BTreeMap<String, BTreeMap<String, usize>>,
    /// Optionally collect and print detailed statistics for cache hits and misses.
    cache_stats: Option<CacheStats>,
}

impl FoldDbOps {
    pub(super) fn new() -> Self {
        Self {
            fold_anchors: vec![],
            printed_fields: vec![],
            print_top_level: true,
            account_filter: None,
            track_caches: false,
            states: vec![State::default()],
            block_hash: None,
            skip_indent: None,
        }
    }

    /// Pre-set that folds on chunks.
    pub(super) fn chunks(self) -> Self {
        self.fold("apply_transactions").print_field("receiver").print_field("shard_id")
    }

    /// Pre-set that folds on receipts.
    pub(super) fn receipts(self) -> Self {
        self.fold("process_receipt")
            .print_field("receiver")
            .print_field("receipt_id")
            .fold("process_transaction")
            .print_field("receipt_id")
            .print_top_level(false)
    }

    pub(super) fn fold(mut self, anchor: impl Into<String>) -> Self {
        self.fold_anchors.push(anchor.into());
        self
    }

    pub(super) fn print_field(mut self, field: impl Into<String>) -> Self {
        self.printed_fields.push(field.into());
        self
    }

    pub(super) fn print_top_level(mut self, yes: bool) -> Self {
        self.print_top_level = yes;
        self
    }

    pub(super) fn with_cache_stats(mut self) -> Self {
        self.track_caches = true;
        self.state().cache_stats = Some(CacheStats::default());
        self
    }

    pub(super) fn account_filter(mut self, account: Option<String>) -> Self {
        self.account_filter = account;
        self
    }

    fn state(&mut self) -> &mut State {
        self.states.last_mut().expect(EMPTY_STATE)
    }

    fn push_state(&mut self, indent: usize) {
        let cache_stats = if self.track_caches { Some(CacheStats::default()) } else { None };
        let new_state = State { indent, ops_cols: Default::default(), cache_stats };
        self.states.push(new_state);
    }

    fn pop_state(&mut self) -> State {
        let state = self.states.pop().expect(EMPTY_STATE);
        if self.states.is_empty() {
            self.push_state(0);
        }
        state
    }

    fn skip(&mut self, indent: usize) -> bool {
        if let Some(skip_indent) = self.skip_indent {
            if skip_indent >= indent {
                self.skip_indent = None;
            } else {
                return true;
            }
        }
        false
    }

    /// Check if indentation has gone back enough to pop current state.
    fn check_indent(&mut self, out: &mut dyn Write, indent: usize) -> anyhow::Result<()> {
        if self.states.len() > 1 && self.state().indent >= indent {
            self.pop_state().print(out)?;
        }
        Ok(())
    }
}

impl Visitor for FoldDbOps {
    fn eval_db_op(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        op: &str,
        size: Option<u64>,
        key: &[u8],
        col: &str,
    ) -> anyhow::Result<()> {
        // Block hash in traces is visibly by looking at DB lookups of
        // `BlockInfo` column. Keep track of it so that each time something is
        // printed, the block hash can be included if desired.
        match op {
            "GET" => {
                if col == "BlockInfo" {
                    self.block_hash = Some(bs58::encode(key).into_string());
                }
            }
            _ => {
                // nop
            }
        }

        if self.skip(indent) {
            return Ok(());
        }

        // Count DB operation for the corresponding column.
        *self
            .state()
            .ops_cols
            .entry(op.to_owned())
            .or_default()
            .entry(col.to_owned())
            .or_default() += 1;

        if let Some(cache_stats) = &mut self.state().cache_stats {
            cache_stats.eval_db_op(op, size);
        }

        self.check_indent(out, indent)?;
        Ok(())
    }

    fn eval_storage_op(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        op: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        if self.skip(indent) {
            return Ok(());
        }
        if let Some(cache_stats) = &mut self.state().cache_stats {
            cache_stats.eval_storage_op(op, dict)?;
        }
        self.check_indent(out, indent)?;
        Ok(())
    }

    fn eval_label(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        label: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        if self.skip(indent) {
            return Ok(());
        }
        self.check_indent(out, indent)?;
        if let (Some(receiver), Some(filtered_account)) =
            (dict.get("receiver"), &self.account_filter)
        {
            if receiver != filtered_account {
                self.skip_indent = Some(indent);
                return Ok(());
            }
        }
        if self.fold_anchors.iter().any(|anchor| *anchor == label) {
            // Section to fold on starts. Push a new state on the stack and
            // print the header for the new section.
            self.push_state(indent);
            write!(out, "{:indent$}{label}", "")?;
            for key in self.printed_fields.iter() {
                if let Some(value) = dict.get(key.as_str()) {
                    write!(out, " {key}={value}")?;
                }
            }
            if let Some(block) = &self.block_hash {
                write!(out, " block={block}")?;
            }
            writeln!(out)?;
        }

        if let Some(cache_stats) = &mut self.state().cache_stats {
            cache_stats.eval_generic_label(dict);
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut dyn Write) -> anyhow::Result<()> {
        if self.print_top_level {
            writeln!(out, "top-level:")?;
            self.pop_state().print(out)?;
        }
        Ok(())
    }
}

impl State {
    fn print(self, out: &mut dyn Write) -> anyhow::Result<()> {
        let indent = self.indent + 2;
        for (op, map) in self.ops_cols.into_iter() {
            if !map.is_empty() {
                write!(out, "{:indent$}{op}   ", "")?;
            }
            for (col, num) in map.into_iter() {
                write!(out, "{num:8>} {col}  ")?;
            }
            writeln!(out)?;
        }

        if let Some(stats) = self.cache_stats {
            stats.print(out, indent)?;
        }
        writeln!(out)?;
        Ok(())
    }
}
