use std::fmt;
use std::sync::Arc;

use cargo_metadata::camino::Utf8PathBuf;

use super::style;

#[derive(Debug)]
pub struct Package {
    pub parsed: cargo_metadata::Package,
    pub raw: toml::Value,
}

#[derive(Debug)]
pub struct Workspace {
    pub root: Utf8PathBuf,
    pub members: Vec<Arc<Package>>,
}

#[derive(Debug)]
pub struct PackageOutcome {
    pub pkg: Arc<Package>,
    pub value: Option<String>,
}

#[derive(Debug)]
pub struct Expected {
    pub value: String,
    pub reason: Option<String>,
}

#[derive(Debug)]
pub struct ComplianceError {
    pub msg: String,
    pub expected: Option<Expected>,
    pub outliers: Vec<PackageOutcome>,
}

impl fmt::Display for ComplianceError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

impl std::error::Error for ComplianceError {}

impl ComplianceError {
    pub fn report(&self, workspace: &Workspace) {
        let mut report = format!(
            "{c_heading}(i) {}:{c_none}{}",
            self.msg,
            match &self.expected {
                None => "".to_string(),
                Some(Expected { value, reason }) => format!(
                    " [expected: {c_expected}{}{c_none}{}]",
                    value,
                    reason.as_ref().map_or("".to_string(), |reason| format!(", {}", reason)),
                    c_expected = style::fg(style::Color::Color256(35))
                        + &style::bg(style::Color::Gray { shade: 3 })
                        + style::bold(),
                    c_none = style::reset()
                ),
            },
            c_heading = style::fg(style::Color::Color256(172)) + style::bold(),
            c_none = style::reset()
        );

        for PackageOutcome { pkg, value } in &self.outliers {
            let Package { parsed: pkg, .. } = &**pkg;
            report.push_str(&format!(
                "\n \u{2022} {c_name}{}{c_none} v{} {c_path}({}){c_none}{}",
                pkg.name,
                pkg.version,
                pkg.manifest_path.strip_prefix(&workspace.root).unwrap(),
                match value {
                    None => "".to_string(),
                    Some(value) => format!(
                        " [found: {c_found}{}{c_none}]",
                        value,
                        c_found = style::fg(style::Color::White)
                            + &style::bg(style::Color::Red)
                            + style::bold(),
                        c_none = style::reset()
                    ),
                },
                c_name = style::fg(style::Color::Color256(39)) + style::bold(),
                c_path = style::fg(style::Color::Gray { shade: 12 }),
                c_none = style::reset(),
            ));
        }
        eprintln!("{}", report);
    }
}
