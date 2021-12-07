use cargo_metadata::{camino::Utf8PathBuf, Package};

mod rules;
mod style;
#[macro_use]
mod utils;

#[derive(Debug)]
pub struct Workspace {
    root: Utf8PathBuf,
    members: Vec<Package>,
}

#[derive(Debug)]
pub struct PackageOutcome<'a> {
    pkg: &'a Package,
    value: Option<String>,
}

#[derive(Debug)]
pub struct Expected {
    value: String,
    reason: Option<String>,
}

#[derive(Debug)]
pub enum Error<'a> {
    OutcomeError { msg: String, expected: Option<Expected>, outliers: Vec<PackageOutcome<'a>> },
    RuntimeError(anyhow::Error),
}

impl<'a, E: Into<anyhow::Error>> From<E> for Error<'a> {
    fn from(err: E) -> Self {
        Error::RuntimeError(err.into())
    }
}

fn main() -> anyhow::Result<()> {
    let workspace = utils::parse_workspace()?;

    if workspace.members.is_empty() {
        return Ok(());
    }

    let passed = chk! {
        [workspace]: {
            rules::is_unversioned,
            rules::has_publish_spec,
            rules::has_rust_version,
            rules::has_debuggable_rust_version,
            rules::has_unified_rust_edition,
            rules::author_is_near,
            // rules::publish_is_publishable,
        }
    };

    if !passed {
        std::process::exit(1);
    }

    Ok(())
}
