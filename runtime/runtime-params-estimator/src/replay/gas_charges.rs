use super::Visitor;
use std::collections::BTreeMap;
use std::io::Write;

/// Visitor that tracks which DB operations are charged with gas and which are
/// for free.
#[derive(Default)]
pub(super) struct ChargedVsFree {
    in_fn_call: bool,
    storage_op_indent: usize,

    free_gets: u64,
    free_gets_size: u64,
    charged_gets: u64,
    charged_gets_size: u64,
}

impl Visitor for ChargedVsFree {
    fn eval_db_op(
        &mut self,
        out: &mut dyn Write,
        indent: usize,
        op: &str,
        size: Option<u64>,
        _key: &[u8],
        _col: &str,
    ) -> anyhow::Result<()> {
        self.eval_label(out, indent, op, &BTreeMap::new())?;
        if op != "GET" {
            return Ok(());
        }
        if self.in_fn_call {
            self.charged_gets += 1;
            self.charged_gets_size += size.unwrap_or(0);
        } else {
            self.free_gets += 1;
            self.free_gets_size += size.unwrap_or(0);
        }
        Ok(())
    }

    fn eval_storage_op(
        &mut self,
        _out: &mut dyn Write,
        indent: usize,
        _op: &str,
        _dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        self.storage_op_indent = indent + 2;
        self.in_fn_call = true;
        Ok(())
    }

    fn eval_label(
        &mut self,
        _out: &mut dyn Write,
        indent: usize,
        _label: &str,
        _dict: &BTreeMap<&str, &str>,
    ) -> anyhow::Result<()> {
        if indent < self.storage_op_indent {
            self.in_fn_call = false;
        }
        Ok(())
    }

    fn flush(&mut self, out: &mut dyn Write) -> anyhow::Result<()> {
        writeln!(
            out,
            "{:>8} free gets with total size of    {:>8}",
            self.free_gets, self.free_gets_size
        )?;
        writeln!(
            out,
            "{:>8} charged gets with total size of {:>8}",
            self.charged_gets, self.charged_gets_size
        )?;
        writeln!(
            out,
            "{:>7.2}% of gets uncharged",
            100.0 * self.free_gets as f64 / (self.charged_gets + self.free_gets) as f64
        )?;
        writeln!(
            out,
            "{:>7.2}% of gets total size uncharged",
            100.0 * self.free_gets_size as f64
                / (self.charged_gets_size + self.free_gets_size) as f64
        )?;

        self.free_gets = 0;
        self.free_gets_size = 0;
        self.charged_gets = 0;
        self.charged_gets_size = 0;

        Ok(())
    }
}
