# Dependencies

## Minimum publish age

Malicious or broken crate releases are usually discovered and yanked within a
few days of being published. To avoid picking them up in that window, the
repository sets a minimum publish age in `.cargo/config.toml`:

```toml
[registry]
global-min-publish-age = "7 days"
```

With this setting, cargo ignores crates.io versions published less than 7 days
ago whenever it resolves dependencies, e.g. in `cargo update` or `cargo add`.
The setting also applies to the standalone packages in this repository that
have their own `Cargo.lock`, such as `tracing` or the test contracts.

Versions that are already in `Cargo.lock` are not affected, so ordinary builds
and anything run with `--locked` behave as before.

### What to expect

* `cargo update -p <crate>` picks the newest version that is at least 7 days
  old. Cargo reports this as `Locking N packages to highest compatible versions
  as of 7 days ago`.
* If a version requirement in `Cargo.toml` can only be satisfied by a version
  that is too new, resolution fails with `version X is too new (published N
  days ago, minimum age 7 days)`. Either wait, or require an older version.

### Overriding

An urgent update, such as a security fix published in the last few days, can
bypass the check for a single command:

```console
$ CARGO_RESOLVER_INCOMPATIBLE_PUBLISH_AGE=allow cargo update -p <crate> --precise <version>
```

Please mention in the PR description that the check was bypassed and why, so
that the reviewer can take a closer look at the new version.

Note that this is a guard against accidentally picking up fresh releases, not
an enforced policy: CI builds with `--locked` and accepts whatever is in
`Cargo.lock`.
