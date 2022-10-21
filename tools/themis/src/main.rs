mod rules;
mod style;
mod types;
mod utils;

fn main() -> anyhow::Result<()> {
    let workspace = utils::parse_workspace()?;

    assert!(!workspace.members.is_empty(), "unexpected empty workspace");

    let rules = [
        rules::is_unversioned,
        rules::has_publish_spec,
        rules::has_rust_version,
        rules::rust_version_matches_toolchain,
        rules::has_unified_rust_edition,
        rules::author_is_near,
        rules::publishable_has_license,
        rules::publishable_has_unified_license,
        rules::publishable_has_license_file,
        rules::publishable_has_description,
        rules::publishable_has_near_link,
        rules::recursively_publishable,
    ];

    let _unused_rules = [
        // TODO: https://github.com/near/nearcore/issues/5849
        // TODO: activate this rule when all non-private crates are sufficiently documented
        rules::publishable_has_readme,
    ];

    let mut failed = false;

    for rule in rules {
        if let Err(err) = rule(&workspace) {
            failed |= true;
            err.downcast::<types::ComplianceError>()?.report(&workspace);
        }
    }

    if failed {
        std::process::exit(1);
    }

    Ok(())
}
