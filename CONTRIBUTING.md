Thank you for your interest in contributing to the NEAR reference client!  We
welcome contributions from everyone.  Below are various bits of information to
help you get started.  If you require additional help, please reach out to us on
our [zulip channel](https://near.zulipchat.com/).

# Getting started

- For non-technical contributions, such as e.g. content or events, see [this
document](https://docs.nearprotocol.com/docs/contribution/contribution-overview).

- For an overview of the NEAR core architecture, see [this
playlist](https://www.youtube.com/playlist?list=PL9tzQn_TEuFV4qlts0tVgndnytFs4QSYo).

- If you have never contributed to nearcore before and are looking for
relatively simple issues to get started with, please checkout issues labeled
with `C-good-first-issue`
[here](https://github.com/near/nearcore/labels/C-good-first-issue).  If you see
one that looks interesting and is not claimed, please comment on the issue that
you would like to start working on it and someone from the team should assign it
to you.

- If you have an idea for an enhancement to the protocol, please make the
proposal by following the [NEAR Enhancement
Proposal](https://github.com/near/NEPs/blob/master/neps/nep-0001.md).

- Otherwise, if you have an idea for an enhancement that does not require a
change in the proposal, please start by creaeting a new issue in the repository.
Someone should respond within 48 hours.  If the proposal is accepted, then you
can follow the process below to begin working on it.

- Finally, if you have noticed an obvious typo or bug that can be fixed with a
small code change, please feel free to submit a PR directly addressing the issue
without opening an issue.

If you have an issue assigned to you, then we kindly ask that you provide an
update at least once a week so we know if you are still actively working on the
issue.

# Setting up editors and IDEs

Majority of NEAR developers use CLion with Rust plugin as their primary IDE.

We also had success with VSCode with rust-analyzer, see the steps for
installation
[here](https://commonwealth.im/near/proposal/discussion/338-remote-development-with-vscode-and-rustanalyzer).

Some of us use VIM with [rust.vim](https://github.com/rust-lang/rust.vim) and
[rusty-tags](https://github.com/dan-t/rusty-tags). It has fewer features than
CLion or VSCode, but overall provides a usable setting.

Refer to [this
document](https://docs.nearprotocol.com/docs/contribution/nearcore) for details
on setting up your environment.

# Build Process

Nearcore is a reasonably standard Rust project, so `cargo test` most likely will
just work.  There are couple of specifics though:

* `nearcore` assumes a UNIX-like environment.  Linux is actively supported; Mac
is generally supported (the newer M1 chipset may not be well supported); and
Windows is not supported.
* `nearcore` build process includes generating bindings to RocksDB via
`bindgen`, which requires `libclang`.  See [`bindgen`
documentation](https://rust-lang.github.io/rust-bindgen/requirements.html#clang)
for installation instructions.
* At the moment, `nearcore` only supports compiling for x86_64 architecture (for
technical reasons, we have to care about CPU architecture and supporting one is
easier).  To compile to x86_64 on Apple silicon, use `rustup set default-host
x86_64-apple-darwin` command.  That is, cross-compilation sadly doesn't work at
the time of writing.
* You can optionally use the system installations of `librocksdb`, `libsnappy`
and `lz4` in order to speed up the compilation and reduce the build memory
requirements by setting the `ROCKSDB_LIB_DIR`, `SNAPPY_LIB_DIR` and
`LZ4_LIB_DIR` environment variables.  These environment variables should point at
the directory containing the dynamic shared objects (`.so`s or `.dylib`s.)

If your setup does not involve `rustup`, the required version of the rust
toolchain can be found in the `rust-toolchain` file.

# Pull Requests

All the contributions to `nearcore` happen via Pull Requests.  To create a Pull
Request, fork `nearcore`, create a new branch, do the work there, and then send
a PR against the `master` branch.

When working on a PR, please keep the following in mind:

1. The changes should be thoroughly tested.  Please refer to [this
document](https://github.com/nearprotocol/nearcore/wiki/Writing-tests-for-nearcore)
for our testing guidelines and overview of the testing infrastructure.
2. If you get an issue assigned to you, please post updates at least once a
week.  You are encouraged to submit work-in-progress draft PRs to get early
feedback.
3. A PR can contain any number of commits and when merged, the commits will be
squashed into a single commit.
4. The PR name should follow the template: `<type>: <name>`.  
Where `type` is `fix` for fixes; `feat` for new features; `refactor` for changes
that primarily reorganize code; `doc` for changes that primarily change
documentation or comments; `test` for changes that primarily introduce new
tests; and `chore` for updating grunt tasks. Please note that `type` is case
sensitive.
5. The PR description should follow the following template:

```
<description>

Test plan
---------
<test plan>
```
6. If your PR introduces a new protocol feature, please document it in
[CHANGELOG.md](CHANGELOG.md) under `unreleased`.

# After the PR is submitted

1. We have a CI process configured to run various tests on each PR.  If
the CI fails on your PR, you need to fix it before it will be reviewed.
2. Once the CI passes, you should expect the first feedback to appear within one
business day.
3. When all the comments have been addressed and all the reviewers are
satisfied, they will approve your PR.  You can now add the `S-automerge` label
on it to instruct Github to merge the PR.

# Code review process

We have two groups of code reviewers:  Super owners and normal owners.  When a
PR is created:

- a super owner will be automatically assigned to review.
- they may choose to review the PR themself or they may delegate to someone else
who belongs either to the super owners or the normal owners group.
- the delegate will perform the review and as needed engage other reviewers as
well.  They will review your tests, and make sure that they can convince
themselves the test coverage is adequate before they even look into the
change, so make sure you tested all the corner cases.

The author is also free to directly request reviews from specific persons
[through the github
ui](https://docs.github.com/en/github/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/requesting-a-pull-request-review).
In this case, the automatically selected super owner will ensure that the
selected reviewer is sufficient or additional reviewers are needed.

If you are interested in becoming a code reviewer, please get in touch with us
on zulip.

# Release Schedule

Once your change ends up in master, it will be released with the rest of the
changes by other contributors on the regular release schedules.

On [betanet](https://docs.near.org/docs/concepts/networks#betanet) we run
nightly build from master with all the nightly protocol feature enabled. Every
five weeks, we stabilize some protocol features and make a release candidate for
testnet.  The process for feature stabilization can be found in [this
document](docs/protocol_upgrade.md).  After the release candidate has been
running on testnet for four weeks and no issues are observed, we stabilize and
publish the release for mainnet.
