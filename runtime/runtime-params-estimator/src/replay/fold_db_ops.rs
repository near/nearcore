use super::cache_stats::CacheStats;
use super::Visitor;
use std::collections::{BTreeMap, HashMap};
use std::io::Write;

const EMPTY_STATE_ERR: &str = "states must never be empty";

/// A visitor that keeps track of DB operations and aggregates it by specific labels.
pub(super) struct FoldDbOps {
    /// Labels at which to break aggregation, and the fields to print on each.
    ///
    /// If field is not available, it will be silently ignored.
    fold_anchors: HashMap<String, Vec<String>>,
    /// Print the DB operations that are at indentation 0, not associated with a span.
    ///
    /// This is a summary of the total. Instead it shows what operations have
    /// not been covered by already reported output. To get a summary of the
    /// total, run without folding.
    print_top_level: bool,
    /// Only show data for a specific smart contract, specified by account id.
    /// Currently only applies within receipts.
    account_filter: Option<String>,
    /// Only evaluate lines with at least this depth of indentation.
    ///
    /// Used to apply filters and skip some parts of the trace.
    /// Default is 0, which collects everything.
    min_indent: usize,
    /// Reset the filter when indentation goes back to this value.
    ///
    /// Used in combination with `min_indent` and `account_filter`.
    /// Resetting means that `min_indent` is set to usize::MAX and nothing will
    /// be evaluated until the `account_filter` is triggered again and sets it
    /// to a smaller value again.
    filter_reset_indent: Option<usize>,
    /// Optionally collect and print detailed statistics for cache hits and misses.
    track_caches: bool,
    /// Keeps track of current block.
    block_hash: Option<String>,
    /// Stack of states, each starting at a specific indent.
    states: Vec<State>,
}

/// Stores statistics for a range of the trace, starting at a fixed indent and
/// collecting data for everything indented further, until another state object
/// is pushed on top.
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
            fold_anchors: HashMap::new(),
            print_top_level: true,
            account_filter: None,
            track_caches: false,
            states: vec![State::default()],
            block_hash: None,
            min_indent: 0,
            filter_reset_indent: None,
        }
    }

    /// Pre-set that folds on chunks.
    pub(super) fn chunks(self) -> Self {
        self.fold("apply_transactions", &["receiver", "shard_id"])
    }

    /// Pre-set that folds on receipts.
    pub(super) fn receipts(self) -> Self {
        self.fold("process_receipt", &["receiver", "receipt_id"])
            .fold("process_transaction", &["tx_hash"])
            .print_top_level(false)
    }

    pub(super) fn fold(mut self, anchor: impl Into<String>, printed_fields: &[&str]) -> Self {
        let fields = printed_fields.into_iter().map(|s| (*s).into()).collect();
        self.fold_anchors.insert(anchor.into(), fields);
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
        if account.is_some() {
            // evaluate nothing if there is a filter, until the filter matches the first time
            self.min_indent = usize::MAX;
        }
        self.account_filter = account;
        self
    }

    fn state(&mut self) -> &mut State {
        self.states.last_mut().expect(EMPTY_STATE_ERR)
    }

    fn push_state(&mut self, indent: usize) {
        let cache_stats = if self.track_caches { Some(CacheStats::default()) } else { None };
        let new_state = State { indent, ops_cols: Default::default(), cache_stats };
        self.states.push(new_state);
    }

    fn pop_state(&mut self) -> State {
        let state = self.states.pop().expect(EMPTY_STATE_ERR);
        if self.states.is_empty() {
            self.push_state(0);
        }
        state
    }

    fn skip_eval(&mut self, trace_indent: usize) -> bool {
        trace_indent < self.min_indent
    }

    /// Check if indentation has gone back enough to pop current state or reset filter.
    ///
    /// Call this before `skip()` to ensure it uses the correct `min_indent`.
    fn update_state(&mut self, out: &mut dyn Write, indent: usize) -> anyhow::Result<()> {
        if self.states.len() > 1 && self.state().indent >= indent {
            self.pop_state().print(out)?;
        }
        if let Some(reset_indent) = self.filter_reset_indent {
            if indent <= reset_indent {
                self.filter_reset_indent = None;
                self.min_indent = usize::MAX;
            }
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
        self.update_state(out, indent)?;

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

        if self.skip_eval(indent) {
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

        Ok(())
    }

    fn eval_storage_op(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        op: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        self.update_state(out, indent)?;
        if self.skip_eval(indent) {
            return Ok(());
        }
        if let Some(cache_stats) = &mut self.state().cache_stats {
            cache_stats.eval_storage_op(op, dict)?;
        }
        Ok(())
    }

    fn eval_label(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        label: &str,
        dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        self.update_state(out, indent)?;
        if let (Some(receiver), Some(filtered_account)) =
            (dict.get("receiver"), &self.account_filter)
        {
            if receiver == filtered_account && self.filter_reset_indent.is_none() {
                self.min_indent = indent;
                self.filter_reset_indent = Some(indent);
            }
        }
        if self.skip_eval(indent) {
            return Ok(());
        }
        if self.fold_anchors.contains_key(label) {
            // Section to fold on starts. Push a new state on the stack and
            // print the header for the new section.
            self.push_state(indent);
            write!(out, "{:indent$}{label}", "")?;
            // Unnecessary perf optimization: Second lookup in fold anchors
            // could be avoided by reading the key directly but then we keep
            // a mutable reference to self and cannot naively call
            // self.push_state above.
            // Better to lookup twice and keep code simple.
            for key in self.fold_anchors.get(label).expect("just checked contains key").iter() {
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
