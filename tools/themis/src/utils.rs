use std::fs;

use cargo_metadata::{camino::Utf8PathBuf, CargoOpt, MetadataCommand};
use serde::de::DeserializeOwned;
use toml;

use super::{style, Error, Expected, Package, PackageOutcome, Workspace};

macro_rules! _warn {
    ($msg:literal $($arg:tt)*) => {
        use $crate::style::{fg, bold, reset, Color};
        eprintln!(concat!("{}warning:{} ", $msg), fg(Color::Yellow) + bold(), reset() $($arg)*)
    }
}

pub(crate) use _warn as warn;

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
        .collect::<anyhow::Result<Vec<Package>>>()?;

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
        Err(Error::OutcomeError { msg, expected, outliers }) => {
            let header = format!(
                "{c_heading}(i) {}:{c_none}{}",
                msg,
                expected.map_or("".to_string(), |Expected { value, reason }| format!(
                    " [expected: {c_expected}{}{c_none}{}]",
                    value,
                    reason.map_or("".to_string(), |reason| format!(", {}", reason)),
                    c_expected = style::fg(style::Color::Color256(35))
                        + &style::bg(style::Color::Gray { shade: 3 })
                        + style::bold(),
                    c_none = style::reset()
                )),
                c_heading = style::fg(style::Color::Color256(172)) + style::bold(),
                c_none = style::reset()
            );
            let report = outliers.into_iter().fold(
                header,
                |mut acc, PackageOutcome { pkg: Package { parsed: pkg, .. }, value }| {
                    acc.push_str(&format!(
                        "\n \u{2022} {c_name}{}{c_none} v{} {c_path}({}){c_none}{}",
                        pkg.name,
                        pkg.version,
                        pkg.manifest_path.strip_prefix(&workspace.root).unwrap(),
                        value.map_or("".to_string(), |v| format!(
                            " [found: {c_found}{}{c_none}]",
                            v,
                            c_found = style::fg(style::Color::White)
                                + &style::bg(style::Color::Red)
                                + style::bold(),
                            c_none = style::reset()
                        )),
                        c_name = style::fg(style::Color::Color256(39)) + style::bold(),
                        c_path = style::fg(style::Color::Gray { shade: 12 }),
                        c_none = style::reset(),
                    ));
                    acc
                },
            );
            eprintln!("{}", report);
            Ok(true)
        }
        _ => Ok(false),
    }
}
