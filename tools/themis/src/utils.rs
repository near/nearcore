use std::fs;

use cargo_metadata::{camino::Utf8PathBuf, CargoOpt, MetadataCommand};
use serde::de::DeserializeOwned;
use toml;

use super::{Error, PackageOutcome, Workspace};

pub fn parse_toml<T: DeserializeOwned>(path: Utf8PathBuf) -> anyhow::Result<T> {
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
        .collect();

    Ok(Workspace { root: metadata.workspace_root, members })
}

macro_rules! chk {
    ([$workspace:ident]: {$($outcome:expr),+$(,)?}) => {{
        let mut failed = false;
        $(failed |= $crate::utils::check_and_report($outcome(&$workspace), &$workspace)?;)+
        !failed
    }};
}

pub fn check_and_report<'a>(
    outcome: Result<(), Error<'a>>,
    workspace: &Workspace,
) -> anyhow::Result<bool> {
    match outcome {
        Err(Error::RuntimeError(err)) => Err(err),
        Err(Error::OutcomeError { msg, outliers }) => {
            let report = format!(
                "{}:{}",
                msg,
                outliers.iter().fold(String::new(), |mut acc, PackageOutcome { pkg, value }| {
                    acc.extend(
                        [
                            "\n - \x1b[33m",
                            pkg.name.as_str(),
                            "\x1b[0m v",
                            pkg.version.to_string().as_str(),
                            " (\x1b[36m",
                            pkg.manifest_path.strip_prefix(&workspace.root).unwrap().as_str(),
                            "\x1b[0m)",
                        ]
                        .into_iter(),
                    );
                    if let Some(value) = value {
                        acc.extend([" [", value.as_str(), "]"].into_iter());
                    }
                    acc
                })
            );
            eprintln!("{}", report);
            Ok(true)
        }
        _ => Ok(false),
    }
}
