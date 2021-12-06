use std::collections::HashMap;

use serde::Deserialize;

use super::{utils, Error, PackageOutcome, Workspace};

/// ensure all crates have the `publish = <true/false>` specification
pub fn has_publish_spec(workspace: &Workspace) -> Result<(), Error> {
    let outliers: Vec<_> = workspace
        .members
        .iter()
        .filter(|pkg| pkg.publish.is_none())
        .map(|pkg| PackageOutcome { pkg, value: None })
        .collect();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "The following packages lack the `publish = true/false` specification".to_string(),
            outliers,
        });
    }

    return Ok(());
}

/// ensure all crates specify a MSRV
pub fn has_rust_version(workspace: &Workspace) -> Result<(), Error> {
    let outliers: Vec<_> = workspace
        .members
        .iter()
        .filter(|pkg| pkg.rust_version.is_none())
        .map(|pkg| PackageOutcome { pkg, value: None })
        .collect();

    for outlier in &outliers {
        println!("{} {:?}", outlier.pkg.name, outlier.pkg.rust_version);
    }

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "The following packages do not specify a Minimum Supported Rust Version (MSRV)"
                .to_string(),
            outliers,
        });
    }

    return Ok(());
}

/// ensure all crates are versioned to v0.0.0
pub fn is_unversioned(workspace: &Workspace) -> Result<(), Error> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            matches!(
                pkg.version,
                semver::Version {
                    major: 0,
                    minor: 0,
                    patch: 0,
                    ref pre,
                    ref build,
                } if pre == &semver::Prerelease::EMPTY
                  && build == &semver::BuildMetadata::EMPTY
            )
        })
        .map(|pkg| PackageOutcome { pkg, value: Some(format!("v{}", pkg.version)) })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "The following packages are versioned".to_string(),
            outliers,
        });
    }

    return Ok(());
}

#[derive(Deserialize)]
struct ToolchainSection {
    channel: semver::Version,
}

#[derive(Deserialize)]
struct RustToolchainCfg {
    toolchain: ToolchainSection,
}

/// ensures all crates have a rust-version spec less than
/// or equal to the version defined in rust-toolchain.toml
pub fn has_debuggable_rust_version(workspace: &Workspace) -> Result<(), Error> {
    let RustToolchainCfg { toolchain: ToolchainSection { channel: rust_toolchain } } =
        utils::parse_toml(workspace.root.join("rust-toolchain.toml"))?;

    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            pkg.rust_version
                .as_ref()
                .map_or(false, |rust_version| rust_version.matches(&rust_toolchain))
        })
        .map(|pkg| PackageOutcome {
            pkg,
            value: Some(format!("v{}", pkg.rust_version.as_ref().unwrap())),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg:
                "These packages have a higher `rust-version` from the workspace's `rust-toolchain`"
                    .to_string(),
            outliers,
        });
    }

    return Ok(());
}

pub fn has_unified_rust_edition(workspace: &Workspace) -> Result<(), Error> {
    let mut edition_groups = HashMap::new();

    for pkg in &workspace.members {
        *edition_groups.entry(&pkg.edition).or_insert(0) += 1;
    }

    let (most_common_edition, _) =
        edition_groups.into_iter().reduce(|a, b| if a.1 > b.1 { a } else { b }).unwrap();

    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| pkg.edition != *most_common_edition)
        .map(|pkg| PackageOutcome { pkg, value: Some(pkg.edition.clone()) })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: format!(
                "These packages deviate from the most common rust edition [{}]",
                most_common_edition
            ),
            outliers,
        });
    }

    return Ok(());
}

// / ensures all crates have a unified rust edition
// pub fn has_unified_rust_edition(workspace: &Workspace) -> Result<(), Error> {
//     let mut version_groups = HashMap::new();
//     // let mut outliers = vec![];

//     for pkg in pkgs {
//         if let None = pkg.rust_version {
//             outliers.push(pkg);
//         }
//     }

//     if !outliers.is_empty() {
//         return Err((
//             "The following packages don't have a publish specification in their package manifest",
//             outliers,
//         ));
//     }

//     return Ok(());
// }
