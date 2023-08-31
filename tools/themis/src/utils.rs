use std::{fs, io, rc::Rc};

use cargo_metadata::{camino::Utf8PathBuf, CargoOpt, MetadataCommand};

use super::types::{Manifest, Package, Workspace};

pub fn read_toml(path: &Utf8PathBuf) -> anyhow::Result<Option<toml::Value>> {
    match fs::read(path) {
        Ok(p) => Ok(Some(toml::from_slice(&p)?)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn parse_workspace() -> anyhow::Result<Workspace> {
    let mut cmd = MetadataCommand::new();

    cmd.features(CargoOpt::AllFeatures);
    cmd.no_deps();

    let metadata = cmd.exec()?;

    let workspace_manifest = Rc::new(Manifest::new(
        match read_toml(&metadata.workspace_root.join("Cargo.toml"))? {
            Some(raw) => raw,
            None => anyhow::bail!("workspace manifest not found"),
        },
        None,
    ));

    let members = metadata
        .packages
        .iter()
        .cloned()
        .filter(|package| metadata.workspace_members.contains(&package.id))
        .map(|package| {
            Ok(Package {
                manifest: Manifest::new(
                    match read_toml(&package.manifest_path)? {
                        Some(raw) => raw,
                        None => anyhow::bail!("package manifest `{}` not found", package.name),
                    },
                    Some(workspace_manifest.clone()),
                ),
                parsed: package,
            })
        })
        .collect::<anyhow::Result<_>>()?;

    Ok(Workspace { root: metadata.workspace_root, members, manifest: workspace_manifest })
}

/// Checks if the crate specified is explicitly publishable
pub fn is_publishable(pkg: &Package) -> bool {
    !matches!(pkg.manifest.read(&["package", "publish"]), Some(toml::Value::Boolean(false)))
}

/// Checks if the file specified exists relative to the crate folder
pub fn exists(pkg: &Package, file: &str) -> bool {
    pkg.parsed.manifest_path.parent().unwrap().join(file).exists()
}

/// Prints a string-ish iterator as a human-readable list
///
/// ```
/// assert_eq!(
///     print_list(&["a", "b", "c"]),
///     "a, b and c"
/// );
/// ```
pub fn human_list<I, T>(i: I) -> String
where
    I: Iterator<Item = T>,
    T: AsRef<str>,
{
    let mut items = i.peekable();
    let mut s = match items.next() {
        Some(s) => s.as_ref().to_owned(),
        None => return String::new(),
    };
    while let Some(i) = items.next() {
        s += if items.peek().is_some() { ", " } else { " and " };
        s += i.as_ref();
    }
    s
}
