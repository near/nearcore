use super::types::{ComplianceError, Expected, Outlier, Workspace};
use super::{style, utils};
use anyhow::{bail, Context};
use std::collections::HashMap;

/// Ensure all crates have the `publish = <true/false>` specification
pub fn has_publish_spec(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers: Vec<_> = workspace
        .members
        .iter()
        .filter(|pkg| pkg.raw["package"].get("publish").is_none())
        .map(|pkg| Outlier { path: pkg.parsed.manifest_path.clone(), found: None })
        .collect();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These packages should have the `publish` specification".to_string(),
            expected: Some(Expected { value: "publish = <true/false>".to_string(), reason: None }),
            outliers,
        });
    }

    Ok(())
}

/// Ensure all crates specify a MSRV and that it is <= to the version in the
/// rust-toolchain.toml file.
pub fn has_rust_version(workspace: &Workspace) -> anyhow::Result<()> {
    let rust_toolchain =
        utils::parse_toml::<toml::Value>(&workspace.root.join("rust-toolchain.toml"))
            .context("Failed to read rust-toolchain file")?;
    let rust_toolchain = rust_toolchain["toolchain"]["channel"].as_str().unwrap().to_owned();
    let rust_toolchain = match semver::Version::parse(&rust_toolchain) {
        Ok(rust_toolchain) => rust_toolchain,
        Err(err) => {
            bail!(
                "semver: unable to parse rustup channel from {}: {}",
                style::highlight("rust-toolchain.toml"),
                err
            );
        }
    };

    let mut no_rust_version = vec![];
    let mut wrong_rust_version = vec![];

    for pkg in &workspace.members {
        match &pkg.parsed.rust_version {
            Some(rust_version) => {
                if !rust_version.matches(&rust_toolchain) {
                    wrong_rust_version.push(Outlier {
                        path: pkg.parsed.manifest_path.clone(),
                        found: Some(format!("{}", rust_version)),
                    });
                }
            }
            None => no_rust_version
                .push(Outlier { path: pkg.parsed.manifest_path.clone(), found: None }),
        }
    }

    if !no_rust_version.is_empty() {
        bail!(ComplianceError {
            msg: "These packages should specify a Minimum Supported Rust Version (MSRV)"
                .to_string(),
            expected: None,
            outliers: no_rust_version,
        });
    }
    if !wrong_rust_version.is_empty() {
        bail!(ComplianceError {
            msg: "These packages have an incompatible `rust-version`".to_string(),
            expected: Some(Expected {
                value: format!("<={}", rust_toolchain),
                reason: Some("as defined in the `rust-toolchain`".to_string()),
            }),
            outliers: wrong_rust_version,
        });
    }

    Ok(())
}

/// Ensure all crates are versioned to v0.0.0
pub fn is_unversioned(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            !pkg.parsed.metadata["workspaces"]["independent"].as_bool().unwrap_or(false)
                && pkg.parsed.version != semver::Version::new(0, 0, 0)
        })
        .map(|pkg| Outlier {
            path: pkg.parsed.manifest_path.clone(),
            found: Some(pkg.parsed.version.to_string()),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These packages shouldn't be versioned".to_string(),
            expected: Some(Expected { value: "0.0.0".to_string(), reason: None }),
            outliers,
        });
    }

    Ok(())
}

/// Ensure all crates share the same rust edition
pub fn has_unified_rust_edition(workspace: &Workspace) -> anyhow::Result<()> {
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
        .map(|pkg| Outlier {
            path: pkg.parsed.manifest_path.clone(),
            found: Some(pkg.parsed.edition.clone()),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These packages have an unexpected rust edition".to_string(),
            expected: Some(Expected {
                value: most_common_edition.clone(),
                reason: Some(format!("used by {} other packages in the workspace", n_compliant)),
            }),
            outliers,
        });
    }

    Ok(())
}

const EXPECTED_AUTHOR: &str = "Near Inc <hello@nearprotocol.com>";

/// Ensure all crates have the appropriate author, non-exclusively of course.
pub fn author_is_near(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| !pkg.parsed.authors.iter().any(|author| author == EXPECTED_AUTHOR))
        .map(|pkg| Outlier {
            path: pkg.parsed.manifest_path.clone(),
            found: Some(format!("{:?}", pkg.parsed.authors)),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These packages need to be correctly authored".to_string(),
            expected: Some(Expected { value: EXPECTED_AUTHOR.to_string(), reason: None }),
            outliers,
        });
    }

    Ok(())
}

/// Ensure all non-private crates have a license
pub fn publishable_has_license(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::is_publishable(pkg)
                && !(pkg.parsed.license.is_some() || pkg.parsed.license_file.is_some())
        })
        .map(|pkg| Outlier { path: pkg.parsed.manifest_path.clone(), found: None })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These non-private packages should have a `license` specification".to_string(),
            expected: None,
            outliers,
        });
    }

    Ok(())
}

/// Ensure all non-private crates have a license file
///
/// Checks for either one LICENSE file, or two LICENSE files, one of which
/// is the Apache License 2.0 and the other is the MIT license.
pub fn publishable_has_license_file(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::is_publishable(pkg)
                && !(utils::exists(pkg, "LICENSE")
                    || (utils::exists(pkg, "LICENSE-APACHE") && utils::exists(pkg, "LICENSE-MIT"))
                    || matches!(pkg
                        .parsed
                        .license_file, Some(ref l) if utils::exists(pkg, l.as_str())))
        })
        .map(|pkg| Outlier {
            path: pkg.parsed.manifest_path.clone(),
            found: pkg.parsed.license_file.as_ref().map(ToString::to_string),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These non-private packages should have a license file".to_string(),
            expected: None,
            outliers,
        });
    }

    Ok(())
}

const EXPECTED_LICENSE: &str = "MIT OR Apache-2.0";

/// Ensure all non-private crates use the the same expected license
pub fn publishable_has_unified_license(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::is_publishable(pkg)
                && matches!(pkg.parsed.license, Some(ref l) if l != EXPECTED_LICENSE)
        })
        .map(|pkg| Outlier {
            path: pkg.parsed.manifest_path.clone(),
            found: pkg.parsed.license.as_ref().map(ToString::to_string),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These non-private packages have an unexpected license".to_string(),
            expected: Some(Expected { value: EXPECTED_LICENSE.to_string(), reason: None }),
            outliers,
        });
    }

    Ok(())
}

/// Ensure all non-private crates have a description
pub fn publishable_has_description(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| utils::is_publishable(pkg) && pkg.parsed.description.is_none())
        .map(|pkg| Outlier { path: pkg.parsed.manifest_path.clone(), found: None })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These non-private packages should have a `description`".to_string(),
            expected: None,
            outliers,
        });
    }

    Ok(())
}

/// Ensure all non-private crates have a README file
pub fn publishable_has_readme(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::is_publishable(pkg)
                && !(utils::exists(pkg, "README.md")
                    || matches!(pkg.parsed.readme, Some(ref r) if utils::exists(pkg, r.as_str())))
        })
        .map(|pkg| Outlier {
            path: pkg.parsed.manifest_path.clone(),
            found: pkg.parsed.readme.as_ref().map(ToString::to_string),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These non-private packages should have a readme file".to_string(),
            expected: None,
            outliers,
        });
    }

    Ok(())
}

const EXPECTED_LINK: &str = "https://github.com/near/nearcore";

/// Ensure all non-private crates have appropriate repository links
pub fn publishable_has_near_link(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::is_publishable(pkg)
                && !matches!(pkg.parsed.repository, Some(ref r) if r == EXPECTED_LINK)
        })
        .map(|pkg| Outlier {
            path: pkg.parsed.manifest_path.clone(),
            found: pkg.parsed.repository.as_ref().map(ToString::to_string),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These non-private packages need to have the appropriate `repository` link"
                .to_string(),
            expected: Some(Expected { value: EXPECTED_LINK.to_string(), reason: None }),
            outliers,
        });
    }

    Ok(())
}
