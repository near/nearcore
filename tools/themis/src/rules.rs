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

    Ok(())
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

    Ok(())
}

/// ensure all crates are versioned to v0.0.0
pub fn is_unversioned(workspace: &Workspace) -> Result<(), Error> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| pkg.parsed.version != semver::Version::new(0, 0, 0))
        .map(|pkg| PackageOutcome { pkg, value: Some(pkg.parsed.version.to_string()) })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These packages shouldn't be versioned".to_string(),
            expected: Some(Expected { value: "0.0.0".to_string(), reason: None }),
            outliers,
        });
    }

    Ok(())
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

    Ok(())
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
                value: most_common_edition.clone(),
                reason: Some(format!("used by {} other packages in the workspace", n_compliant)),
            }),
            outliers,
        });
    }

    Ok(())
}

/// ensure all crates have the appropriate author, non-exclusively of course.
pub fn author_is<'a>(expected: &'a str) -> impl Fn(&'a Workspace) -> Result<(), Error> {
    move |workspace| {
        let outliers = workspace
            .members
            .iter()
            .filter(|pkg| !pkg.parsed.authors.iter().any(|author| author == expected))
            .map(|pkg| PackageOutcome { pkg, value: Some(format!("{:?}", pkg.parsed.authors)) })
            .collect::<Vec<_>>();

        if !outliers.is_empty() {
            return Err(Error::OutcomeError {
                msg: "These packages need to be correctly authored".to_string(),
                expected: Some(Expected { value: expected.to_string(), reason: None }),
                outliers,
            });
        }

        Ok(())
    }
}

/// ensure all non-private crates have a license
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

    Ok(())
}

/// ensure all non-private crates have a license file
///
/// Checks for either one LICENSE file, or two LICENSE files, one of which
/// is the Apache License 2.0 and the other is the MIT license.
pub fn publishable_has_license_file(workspace: &Workspace) -> Result<(), Error> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::is_publishable(pkg)
                && !utils::exists!(
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

    Ok(())
}

/// ensure all non-private crates use the the same license
pub fn publishable_has_unified_license(workspace: &Workspace) -> Result<(), Error> {
    let mut license_groups = HashMap::new();

    for pkg in &workspace.members {
        if let Some(license) = &pkg.parsed.license {
            *license_groups.entry(license).or_insert(0) += 1;
        }
    }

    let (most_common_license, n_compliant) =
        license_groups.into_iter().reduce(|a, b| if a.1 > b.1 { a } else { b }).unwrap();

    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::is_publishable(pkg)
                && pkg.parsed.license.as_ref().map_or(false, |l| l != most_common_license)
        })
        .map(|pkg| PackageOutcome {
            pkg,
            value: pkg.parsed.license.as_ref().map(|l| l.to_string()),
        })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These non-private packages have an unexpected license".to_string(),
            expected: Some(Expected {
                value: most_common_license.clone(),
                reason: Some(format!("used by {} other packages in the workspace", n_compliant)),
            }),
            outliers,
        });
    }

    Ok(())
}

/// ensure all non-private crates have a description
pub fn publishable_has_description(workspace: &Workspace) -> Result<(), Error> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| utils::is_publishable(pkg) && pkg.parsed.description.is_none())
        .map(|pkg| PackageOutcome { pkg, value: None })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These non-private packages should have a `description`".to_string(),
            expected: None,
            outliers,
        });
    }

    Ok(())
}

/// ensure all non-private crates have a README file
pub fn publishable_has_readme(workspace: &Workspace) -> Result<(), Error> {
    let outliers = workspace
        .members
        .iter()
        .filter(|pkg| {
            utils::is_publishable(pkg) && !utils::exists!(pkg, "README.md" || (pkg.parsed.readme)?)
        })
        .map(|pkg| PackageOutcome { pkg, value: pkg.parsed.readme.as_ref().map(|r| r.to_string()) })
        .collect::<Vec<_>>();

    if !outliers.is_empty() {
        return Err(Error::OutcomeError {
            msg: "These non-private packages should have a readme file".to_string(),
            expected: None,
            outliers,
        });
    }

    Ok(())
}

/// ensure all non-private crates have repository and homepage links
pub fn publishable_has_links<'a>(expected: &'a str) -> impl Fn(&'a Workspace) -> Result<(), Error> {
    move |workspace| {
        let outliers = workspace
            .members
            .iter()
            .filter(|pkg| {
                utils::is_publishable(pkg)
                    && !(matches!(pkg.parsed.repository, Some(ref r) if r.contains(expected))
                        && pkg.parsed.homepage.is_some())
            })
            .map(|pkg| PackageOutcome {
                pkg,
                value: Some(format!(
                    "{c_none}{{repository = {c_found}{:?}{c_none}, homepage = {c_found}{:?}{c_none}}}",
                    pkg.parsed.repository.as_ref().map_or("null", |r| r.as_str()),
                    pkg.parsed.homepage.as_ref().map_or("null", |h| h.as_str()),
                    c_found = style::fg(style::Color::White)
                        + &style::bg(style::Color::Red)
                        + style::bold(),
                    c_none = style::reset()
                )),
            })
            .collect::<Vec<_>>();

        if !outliers.is_empty() {
            return Err(Error::OutcomeError {
                msg: "These non-private packages need to have appropriate `repository` and `homepage` links".to_string(),
                expected: Some(Expected { value: expected.to_string(), reason: None }),
                outliers,
            });
        }

        Ok(())
    }
}
