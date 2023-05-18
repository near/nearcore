use std::fmt;
use std::rc::Rc;

use cargo_metadata::camino::Utf8PathBuf;

use super::style;

#[derive(Debug)]
pub struct Package {
    pub parsed: cargo_metadata::Package,
    pub manifest: Manifest,
}

#[derive(Debug)]
pub struct Workspace {
    pub root: Utf8PathBuf,
    pub members: Vec<Package>,
    pub manifest: Rc<Manifest>,
}

#[derive(Debug)]
pub struct Manifest {
    raw: toml::Value,
    parent: Option<Rc<Manifest>>,
}

impl Manifest {
    pub fn new(this: toml::Value, parent: Option<Rc<Manifest>>) -> Self {
        Self { raw: this, parent }
    }

    pub fn read(&self, path: &[&str]) -> Option<&toml::Value> {
        read_toml_with_potential_inheritance(&self.raw, self.parent.as_ref().map(|p| &p.raw), path)
    }
}

#[derive(Debug)]
pub struct Outlier {
    pub path: Utf8PathBuf,
    pub found: Option<String>,
    pub extra: Option<String>,
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
    pub outliers: Vec<Outlier>,
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

        for Outlier { path, found, extra: reason } in &self.outliers {
            report.push_str(&format!(
                "\n {c_path}\u{21b3} {}{c_none}{}{}",
                path.strip_prefix(&workspace.root).unwrap(),
                match found {
                    None => "".to_string(),
                    Some(found) => format!(
                        " (found: {c_found}{}{c_none})",
                        found,
                        c_found = style::fg(style::Color::White)
                            + &style::bg(style::Color::Red)
                            + style::bold(),
                        c_none = style::reset()
                    ),
                },
                match reason {
                    None => "".to_string(),
                    Some(reason) => format!(" ({})", reason),
                },
                c_path = style::fg(style::Color::Gray { shade: 12 }),
                c_none = style::reset(),
            ));
        }
        eprintln!("{}", report);
    }
}

fn read_toml_with_potential_inheritance<'a>(
    this: &'a toml::Value,
    parent: Option<&'a toml::Value>,
    path: &[&str],
) -> Option<&'a toml::Value> {
    let mut raw = this;
    let mut cursor = 0;

    for (i, key) in path.iter().enumerate() {
        if let Some(_raw) = raw.get(key) {
            (raw, cursor) = (_raw, i);
        }
    }

    if let (toml::Value::Table(contents), Some(parent)) = (raw, parent) {
        if let Some(toml::Value::Boolean(true)) = contents.get("workspace") {
            return read_toml_with_potential_inheritance(&parent["workspace"], None, path);
        }
    }

    (cursor == path.len() - 1).then_some(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use toml::toml;

    #[test]
    fn test_read_pkg() {
        let pkg = toml! {
            [package]
            name = "pkg"
            version = "0.1.0"
            edition = "2021"
            authors = ["Alice <alice@p2p.io>"]

            [dependencies]
            syn = "2"
            quote = { version = "0.6", optional = true }

            [dependencies.serde]
            path = "../serde"
        };

        let workspace = toml! {
            [workspace]
            members = ["pkg"]
        };

        assert_eq!(
            Some(&toml::Value::String("2021".to_owned())),
            read_toml_with_potential_inheritance(&pkg, Some(&workspace), &["package", "edition"])
        );

        assert_eq!(
            Some(&toml::Value::String("2".to_owned())),
            read_toml_with_potential_inheritance(&pkg, Some(&workspace), &["dependencies", "syn"])
        );

        assert_eq!(
            Some(&toml::Value::Boolean(true)),
            read_toml_with_potential_inheritance(
                &pkg,
                Some(&workspace),
                &["dependencies", "quote", "optional"]
            )
        );

        assert_eq!(
            Some(&toml! {
                "path" = "../serde"
            }),
            read_toml_with_potential_inheritance(
                &pkg,
                Some(&workspace),
                &["dependencies", "serde"]
            )
        );
    }

    #[test]
    fn test_workspace_inheritance() {
        let pkg = toml! {
            [package]
            name = "pkg"
            version.workspace = true
            edition.workspace = true
            authors.workspace = true

            [dependencies]
            syn.workspace = true
            quote.workspace = true
            serde = { workspace = true }
        };

        let workspace = toml! {
            [workspace.package]
            version = "0.1.0"
            edition = "2021"
            authors = ["Alice <alice@p2p.io>"]

            [workspace.dependencies]
            syn = "2"
            quote = { version = "0.6", optional = true }

            [workspace.dependencies.serde]
            path = "../serde"
        };

        assert_eq!(
            Some(&toml::Value::String("2021".to_owned())),
            read_toml_with_potential_inheritance(&pkg, Some(&workspace), &["package", "edition"])
        );

        assert_eq!(
            Some(&toml::Value::String("2".to_owned())),
            read_toml_with_potential_inheritance(&pkg, Some(&workspace), &["dependencies", "syn"])
        );

        assert_eq!(
            Some(&toml::Value::Boolean(true)),
            read_toml_with_potential_inheritance(
                &pkg,
                Some(&workspace),
                &["dependencies", "quote", "optional"]
            )
        );

        assert_eq!(
            Some(&toml! {
                "path" = "../serde"
            }),
            read_toml_with_potential_inheritance(
                &pkg,
                Some(&workspace),
                &["dependencies", "serde"]
            )
        );
    }
}
