use super::types::{ComplianceError, Expected, Outlier, Workspace};
use super::{style, utils};
use crate::utils::read_toml;
use anyhow::bail;
use cargo_metadata::DependencyKind;
use std::collections::{BTreeMap, HashMap};

/// Ensure all crates have the `publish = <true/false>` specification
pub fn has_publish_spec(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers: Vec<_> = workspace
        .members
        .iter()
        .filter(|pkg| pkg.manifest.read(&["package", "publish"]).is_none())
        .map(|pkg| Outlier { path: pkg.parsed.manifest_path.clone(), found: None, extra: None })
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

/// Ensure all crates specify a MSRV if they are publishable
pub fn has_rust_version(workspace: &Workspace) -> anyhow::Result<()> {
    let outliers: Vec<_> = workspace
        .members
        .iter()
        .filter(|pkg| {
            pkg.parsed.rust_version.is_none()
                && pkg.manifest.read(&["package", "publish"]) != Some(&toml::Value::Boolean(false))
        })
        .map(|pkg| Outlier { path: pkg.parsed.manifest_path.clone(), found: None, extra: None })
        .collect();

    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These packages should specify a Minimum Supported Rust Version (MSRV)"
                .to_string(),
            expected: None,
            outliers,
        });
    }

    Ok(())
}

/// Ensure rust-version is the same in Cargo.toml and rust-toolchain.toml
pub fn rust_version_matches_toolchain(workspace: &Workspace) -> anyhow::Result<()> {
    fn get<'a>(mut val: &'a toml::Value, indexes: &[&str]) -> anyhow::Result<&'a toml::Value> {
        for &i in indexes {
            val = val.get(i).ok_or_else(|| anyhow::format_err!("no `{i}` in {val}"))?;
        }
        Ok(val)
    }

    let toolchain_file = match read_toml(&workspace.root.join("rust-toolchain.toml"))? {
        Some(toolchain_file) => toolchain_file,
        None => return Ok(()),
    };
    let toolchain_version = get(&toolchain_file, &["toolchain", "channel"])?;

    let workspace_version =
        workspace.manifest.read(&["workspace", "package", "rust-version"]).ok_or_else(|| {
            anyhow::format_err!("no `workspace.package.rust-version` in the root Cargo.toml")
        })?;

    if toolchain_version != workspace_version {
        bail!(ComplianceError {
            msg: "rust-version in rust-toolchain.toml and workspace Cargo.toml differ".to_string(),
            expected: None,
            outliers: Vec::new(),
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
            extra: None,
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
            extra: None,
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
            extra: None,
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
        .map(|pkg| Outlier { path: pkg.parsed.manifest_path.clone(), found: None, extra: None })
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
            extra: None,
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
                // near-vm is a wasmer fork, so we don’t control the license
                && !pkg.parsed.name.starts_with("near-vm")
        })
        .map(|pkg| Outlier {
            path: pkg.parsed.manifest_path.clone(),
            found: pkg.parsed.license.as_ref().map(ToString::to_string),
            extra: None,
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
        .map(|pkg| Outlier { path: pkg.parsed.manifest_path.clone(), found: None, extra: None })
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
            extra: None,
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
            extra: None,
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

/// Ensure all publishable crates do not depend on private crates
pub fn recursively_publishable(workspace: &Workspace) -> anyhow::Result<()> {
    let mut outliers = BTreeMap::new();
    for pkg in workspace.members.iter().filter(|pkg| utils::is_publishable(pkg)) {
        for dep in &pkg.parsed.dependencies {
            if matches!(dep.kind, DependencyKind::Normal | DependencyKind::Build) {
                if let Some(dep) = workspace.members.iter().find(|p| p.parsed.name == dep.name) {
                    if !utils::is_publishable(dep) {
                        outliers
                            .entry(dep.parsed.manifest_path.clone())
                            .or_insert_with(Vec::new)
                            .push(pkg.parsed.name.clone())
                    }
                }
            }
        }
    }
    if !outliers.is_empty() {
        bail!(ComplianceError {
            msg: "These private crates break publishable crates. Either make these private crates publishable or avoid using them in the publishable crates".to_string(),
            expected: None,
            outliers: outliers
                .into_iter()
                .map(|(path, found)| Outlier {
                    path,
                    found: None,
                    extra: Some(format!(
                        "depended on by {}",
                        utils::human_list(found.iter().map(|name| style::fg(style::Color::White)
                            + style::bold()
                            + name
                            + style::reset()))
                    )),
                })
                .collect(),
        });
    }
    Ok(())
}
