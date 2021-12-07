use std::env;
use std::fs;
use std::process;

use cargo_metadata::{camino::Utf8PathBuf, CargoOpt, MetadataCommand};
use serde::de::DeserializeOwned;
use toml;

use super::{style, Error, Expected, PackageOutcome, Workspace};

macro_rules! _warn {
    ($msg:literal $($arg:tt)*) => {
        use $crate::style::{fg, bold, reset, Color};
        eprintln!(concat!("{}warning:{} ", $msg), fg(Color::Yellow) + bold(), reset() $($arg)*)
    }
}

pub(crate) use _warn as warn;

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
            let report =
                outliers.into_iter().fold(header, |mut acc, PackageOutcome { pkg, value }| {
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
                });
            eprintln!("{}", report);
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[derive(Debug)]
pub struct CargoVersion {
    pub parsed: semver::Version,
    pub raw: String,
}

pub fn cargo_version() -> anyhow::Result<CargoVersion> {
    let cargo =
        env::var("CARGO").map(Utf8PathBuf::from).unwrap_or_else(|_| Utf8PathBuf::from("cargo"));
    let output = process::Command::new(cargo).arg("--version").output()?;
    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "failed to get cargo version: {}",
            String::from_utf8(output.stderr)?
        ));
    }
    let raw_version = String::from_utf8(output.stdout)?;
    let version = raw_version
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("failed to extract cargo version"))
        .and_then(|v| {
            semver::Version::parse(v).map_err(|_| anyhow::anyhow!("failed to parse cargo version"))
        })?;
    Ok(CargoVersion { parsed: version, raw: raw_version })
}
