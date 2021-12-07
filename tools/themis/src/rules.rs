use std::collections::HashMap;

use super::{style, utils, Error, Expected, PackageOutcome, Workspace};

/// ensure all crates have the `publish = <true/false>` specification
pub fn has_publish_spec(workspace: &Workspace) -> Result<(), Error> {
    let outliers: Vec<_> = workspace
        .members
        .iter()
        .filter(|pkg| pkg.raw["package"].get("publish").is_none())
        .map(|pkg| PackageOutcome { pkg, value: None })
        .collect();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These packages should have the `publish` specification".to_string(),
            expected: Some(Expected { value: "publish = <true/false>".to_string(), reason: None }),
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
        .filter(|pkg| pkg.raw["package"].get("rust-version").is_none())
        .map(|pkg| PackageOutcome { pkg, value: None })
        .collect();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These packages should specify a Minimum Supported Rust Version (MSRV)"
                .to_string(),
            expected: None,
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
            !matches!(
                pkg.parsed.version,
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
        .map(|pkg| PackageOutcome { pkg, value: Some(pkg.parsed.version.to_string()) })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These packages shouldn't be versioned".to_string(),
            expected: Some(Expected { value: "0.0.0".to_string(), reason: None }),
            outliers,
        });
    }

    return Ok(());
}

/// ensures all crates have a rust-version spec less than
/// or equal to the version defined in rust-toolchain.toml
pub fn has_debuggable_rust_version(workspace: &Workspace) -> Result<(), Error> {
    let rust_toolchain =
        utils::parse_toml::<toml::Value>(&workspace.root.join("rust-toolchain.toml"))?;
    let rust_toolchain = rust_toolchain["toolchain"]["channel"].as_str().unwrap().to_owned();

    let rust_toolchain = match semver::Version::parse(&rust_toolchain) {
        Ok(rust_toolchain) => rust_toolchain,
        Err(err) => {
            utils::warn!(
                "semver: unable to parse rustup channel from {}: {}",
                style::highlight("rust-toolchain.toml"),
                err
            );

            return Ok(());
        }
    };

    let mut outliers = vec![];
    for pkg in &workspace.members {
        let (rust_version, raw) = match pkg.raw["package"].get("rust-version") {
            Some(rust_version) => {
                let raw = rust_version.as_str().unwrap();
                (semver::VersionReq::parse(raw)?, raw)
            }
            // we can skip, since we have has_rust_version check
            None => continue,
        };

        if !rust_version.matches(&rust_toolchain) {
            outliers.push(PackageOutcome { pkg, value: Some(raw.to_owned()) });
        }
    }

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These packages have an incompatible `rust-version`".to_string(),
            expected: Some(Expected {
                value: format!("<={}", rust_toolchain),
                reason: Some("as defined in the `rust-toolchain`".to_string()),
            }),
            outliers,
        });
    }

    return Ok(());
}

/// ensure all crates share the same rust edition
pub fn has_unified_rust_edition(workspace: &Workspace) -> Result<(), Error> {
    let mut edition_groups = HashMap::new();

    for pkg in &workspace.members {
        *edition_groups.entry(&pkg.parsed.edition).or_insert(0) += 1;
    }

    let (most_common_edition, n_compliant) =
        edition_groups.into_iter().reduce(|a, b| if a.1 > b.1 { a } else { b }).unwrap();

    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| pkg.parsed.edition != *most_common_edition)
        .map(|pkg| PackageOutcome { pkg, value: Some(pkg.parsed.edition.clone()) })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These packages have an unexpected rust edition".to_string(),
            expected: Some(Expected {
                value: most_common_edition.to_string(),
                reason: Some(format!("used by {} other packages in the workspace", n_compliant)),
            }),
            outliers,
        });
    }

    return Ok(());
}

const NEAR_AUTHOR: &'static str = "Near Inc <hello@nearprotocol.com>";
/// ensure all crates have NEAR as an author, non-exclusively of course.
pub fn author_is_near(workspace: &Workspace) -> Result<(), Error> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| !pkg.parsed.authors.iter().any(|author| author == NEAR_AUTHOR))
        .map(|pkg| PackageOutcome { pkg, value: Some(format!("{:?}", pkg.parsed.authors)) })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These packages should be tagged as authored by NEAR".to_string(),
            expected: Some(Expected { value: NEAR_AUTHOR.to_string(), reason: None }),
            outliers,
        });
    }

    return Ok(());
}

/// ensure all publishable crates have a license
pub fn publishable_has_license(workspace: &Workspace) -> Result<(), Error> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::is_publishable(pkg)
                && !(pkg.parsed.license.is_some() || pkg.parsed.license_file.is_some())
        })
        .map(|pkg| PackageOutcome { pkg, value: None })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These non-private packages should have a `license` specification".to_string(),
            expected: None,
            outliers,
        });
    }

    return Ok(());
}

/// ensure all publishable crates have a license file
///
/// Checks for either one LICENSE file, or two LICENSE files, one of which
/// is the Apache License 2.0 and the other is the MIT license.
pub fn publishable_has_license_file(workspace: &Workspace) -> Result<(), Error> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::pub_missing!(
                pkg,
                "LICENSE" || ("LICENSE-APACHE" && "LICENSE-MIT") || (pkg.parsed.license_file)?
            )
        })
        .map(|pkg| PackageOutcome {
            pkg,
            value: pkg.parsed.license_file.as_ref().map(|l| l.to_string()),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These non-private packages should have a license file".to_string(),
            expected: None,
            outliers,
        });
    }

    return Ok(());
}
