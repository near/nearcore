Thank you for considering contributing to the NEAR reference client!

We welcome all external contributions. This document outlines the process of contributing to nearcore.
For contributing to other repositories, see `CONTRIBUTING.md` in the corresponding repository.
For non-technical contributions, such as e.g. content or events, see [this document](https://docs.nearprotocol.com/docs/contribution/contribution-overview).

For an overview of the NEAR core architecture, see [this playlist](https://www.youtube.com/playlist?list=PL9tzQn_TEuFV4qlts0tVgndnytFs4QSYo).

# Pull Requests and Issues

All the contributions to `nearcore` happen via Pull Requests. To create a Pull Request, fork `nearcore`, create a new branch, do the work there, and then send the PR via the Github interface.

The PRs should always be against the `master` branch.

The exact process depends on the particular contribution you are making.

## Typos or small fixes

If you see an obvious typo, or an obvious bug that can be fixed with a small change, in the code or documentation, feel free to submit the pull request that fixes it without opening an issue.

## Working on current tasks

If you have never contributed to nearcore before, take a look at the work items in the issue tracker labeled with `C-good-first-issue` [here](https://github.com/near/nearcore/labels/C-good-first-issue). If you see one that looks interesting, and is not claimed, please comment on the issue that you would like to start working on it, and someone from the team will assign it to you.

Keep in mind the following:

1. The changes need to be thoroughly tested. Refer to [this document](https://github.com/nearprotocol/nearcore/wiki/Writing-tests-for-nearcore) for our testing guidelines and overview of the testing infrastructure.
2. If you get an issue assigned to you, please post updates at least once a week. It is also preferred for you to send a draft PR as early as you have something working, before it is ready.

### Submitting the PR

Once your change is ready, prepare the PR. The PR can contain any number of commits, but when it is merged, they will all get squashed. The commit names and descriptions can be arbitrary, but the name and the description of the PR must follow the following template:

```
<type>: <name>

<description>

Test plan
---------
<test plan>
```

Where `type` is `fix` for fixes, `feat` for features, `refactor` for changes that primarily reorganize code, `doc` for changes that primarily change documentation or comments, and `test` for changes that primarily introduce new tests. The type is case sensitive.

The `test plan` should describe in detail what tests are presented, and what cases they cover.

If your PR introduces a new protocol feature, please document it in [CHANGELOG.md](CHANGELOG.md) under `unreleased`.


### After the PR is submitted

1. We have a CI process configured to run all the sanity tests on each PR. If the CI fails on your PR, you need to fix it before it will be reviewed.
2. Once the CI passes, you should expect the first feedback to appear within one business day.
   One code owner (chosen in a round robin order for the codeowner list) will first review your pull request.
   They may re-assign the pull request to another person if they feel that they don't have sufficient expertise to review the pull request.
   The reviewers will review your tests, and make sure that they can convince themselves the test coverage is adequate before they even look into the change, so make sure you tested all the corner cases.
   If you would like to request review from a specific review, feel free to do so [through the github UI](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/requesting-a-pull-request-review).
3. When all the comments have been addressed and all the reviewers are satisfied, they will approve your PR.  You can now add the `S-automerge` label on it to instruct Github to merge the PR.

## Proposing new ideas and features

If you want to propose an idea or a feature and work on it, create a new issue in the `nearcore` repository. We presently do not have an issue template.

You should expect someone to comment on the issue within 48 hours after it is created. If the proposal in the issue is accepted, you should then follow the process for `Working on current tasks` above.

# Build Process

Nearcore is a reasonably standard Rust project, so `cargo test` most likely will just work.
There are couple of specifics though:

* `nearcore` assumes UNIX-like environment.
  You can compile and run it on Linux or Mac, but windows is not supported yet.
* `nearcore` build process includes generating bindings to RocksDB via `bindgen`, which requires `libclang`.
  See [`bindgen` documentation](https://rust-lang.github.io/rust-bindgen/requirements.html#clang) for installation instructions.
* At the moment, `nearcore` only supports compiling for x86_64 architecture
  (for technical reasons, we have to care about CPU architecture, and supporting one is easier).
  To compile to x86_64 on Apple silicon, use `rustup set default-host x86_64-apple-darwin` command.
  That is, cross-compilation sadly doesn't work at the time of writing, although the fix is in the pipeline.
* You can optionally use the system installations of `librocksdb`, `libsnappy` and `lz4` in order
  to speed up the compilation and reduce the build memory requirements by setting the
  `ROCKSDB_LIB_DIR`, `SNAPPY_LIB_DIR` and `LZ4_LIB_DIR` environment variables. These environment
  variables should point at the directory containing the dynamic shared objects (`.so`s or
  `.dylib`s.)

If your setup does not involve `rustup`, the required version of the rust toolchain can be found in
the `rust-toolchain` file.

## Editors and IDEs

Majority of NEAR developers use CLion with Rust plugin as their primary IDE.

We also had success with VSCode with rust-analyzer, see the steps for installation [here](https://commonwealth.im/near/proposal/discussion/338-remote-development-with-vscode-and-rustanalyzer).

Some of us use VIM with [rust.vim](https://github.com/rust-lang/rust.vim) and [rusty-tags](https://github.com/dan-t/rusty-tags). It has fewer features than CLion or VSCode, but overall provides a usable setting.

Refer to [this document](https://docs.nearprotocol.com/docs/contribution/nearcore) for details on setting up your environment.

# Release Schedule

Once your change ends up in master, it will be released with the rest of the changes by other contributors on the regular release schedules.

On [betanet](https://docs.near.org/docs/concepts/networks#betanet) we run nightly build from master with all the nightly protocol feature enabled.
Every five weeks, we stabilize some protocol features and make a release candidate for testnet.
The process for feature stabilization can be found in [this document](docs/protocol_upgrade.md).
After the release candidate has been running on testnet for four weeks and no issue is observed,
we stabilize and publish the release for mainnet.

# Crate Versioning and Publishing

While all the crates in the workspace are directly unversioned (`v0.0.0`), they all share a unified variable version in the [workspace manifest](Cargo.toml). This keeps versions consistent across the workspace and informs their versions at the moment of publishing.

We also have CI infrastructure set up to automate the publishing process to crates.io. So, on every merge to master, if there's a version change, it's automatically applied to all the crates in the workspace and it attempts to publish the new versions of all non-private crates. All crates that should be exempt from this process should be marked `private`. That is, they should have the `publish = false` specification in their package manifest.

This process is managed by [cargo-workspaces](https://github.com/pksunkara/cargo-workspaces), with a [bit of magic](https://github.com/pksunkara/cargo-workspaces/compare/master...miraclx:grouping-and-exclusion#files_bucket) sprinkled on top.

## Issue Labels

Issue labels are of the following format `<type>-<content>` where `<type>` is a capital letter indicating the type of the label and `<content>` is a hyphened phrase indicating what is label is about.
For example, in the label `C-bug`, `C` means category and `bug` means that the label is about bugs.
Common types include `C`, which means category, `A`, which means area, `T`, which means team.

An issue can have multiple labels including which area it touches, which team should be responsible for the issue, and so on.
Each issue should have at least one label attached to it after it is triaged and the label could be a general one, such as `C-enhancement` or `C-bug`.
