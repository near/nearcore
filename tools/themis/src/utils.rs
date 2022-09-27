use std::fs;

use cargo_metadata::{camino::Utf8PathBuf, CargoOpt, MetadataCommand};
use serde::de::DeserializeOwned;

use super::types::{Package, Workspace};

macro_rules! _warn {
    ($msg:literal $($arg:tt)*) => {
        use $crate::style::{fg, bold, reset, Color};
        eprintln!(concat!("{}warning:{} ", $msg), fg(Color::Yellow) + bold(), reset() $($arg)*)
    }
}

pub fn parse_toml<T: DeserializeOwned>(path: &Utf8PathBuf) -> anyhow::Result<T> {
    Ok(toml::from_slice(&fs::read(path)?)?)
}

pub fn parse_workspace() -> anyhow::Result<Workspace> {
    let mut cmd = MetadataCommand::new();

    cmd.features(CargoOpt::AllFeatures);
    cmd.no_deps();

    let metadata = cmd.exec()?;

    let members = metadata
        .packages
        .iter()
        .cloned()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .map(|package| {
            let raw = parse_toml(&package.manifest_path)?;
            Ok(Package { parsed: package, raw })
        })
        .collect::<anyhow::Result<_>>()?;

    Ok(Workspace { root: metadata.workspace_root, members })
}

/// Checks if the crate specified is explicitly publishable
pub fn is_publishable(pkg: &Package) -> bool {
    !matches!(pkg.raw["package"].get("publish"), Some(toml::Value::Boolean(false)))
}

/// Checks if the file specified exists relative to the crate folder
pub fn exists(pkg: &Package, file: &str) -> bool {
    pkg.parsed.manifest_path.parent().unwrap().join(file).exists()
}
