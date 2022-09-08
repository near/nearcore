Thank you for your interest in contributing to the NEAR reference client!  We
welcome contributions from everyone.  Below are various bits of information to
help you get started.  If you require additional help, please reach out to us on
our [zulip channel](https://near.zulipchat.com/).

## Getting started

- For non-technical contributions, such as e.g. content or events, see [this
document](https://docs.nearprotocol.com/docs/contribution/contribution-overview).

- For an overview of the NEAR core architecture, see [this
playlist](https://www.youtube.com/playlist?list=PL9tzQn_TEuFV4qlts0tVgndnytFs4QSYo).

- If you are looking for relatively simple tasks to familiarise yourself with
`nearcore`, please check out issues labeled with the `C-good-first-issue` label
[here](https://github.com/near/nearcore/labels/C-good-first-issue).  If you see
one that looks interesting and is unassigned or has not been actively worked on
in some time, please ask to have the issue be assigned to you and someone from
the team should help you get started.  We do not always keep the issue tracker
up-to-date, so if you do not find an interesting task to work on, please ask for
help on our zulip channel.

- If you have an idea for an enhancement to the protocol, please make the
proposal by following the [NEAR Enhancement
Proposal](https://github.com/near/NEPs/blob/master/neps/nep-0001.md).

- Otherwise, if you have an idea for an enhancement that does not require a
change in the proposal, please start by creating a new issue in the tracker.
Someone should respond within 48 hours.  If the proposal is accepted, then you
can follow the process below to begin work on it.

- Finally, if you have noticed an obvious typo or bug that can be fixed with a
small code change, please feel free to submit a PR directly addressing the issue
without opening an issue.

## Build Process

`nearcore` is a reasonably standard Rust project, so `cargo test` most likely will
just work.  There are couple of specifics though:

* `nearcore` assumes a UNIX-like environment.  Linux is actively supported; Mac
is generally supported (the newer M1 chipset may not be well supported); and
Windows is not supported.
* `nearcore` build process includes generating bindings to RocksDB via
`bindgen`, which requires `libclang`.  See [`bindgen`
documentation](https://rust-lang.github.io/rust-bindgen/requirements.html#clang)
for installation instructions.
* You can optionally use the system installations of `librocksdb`, `libsnappy`
and `lz4` in order to speed up the compilation and reduce the build memory
requirements by setting the `ROCKSDB_LIB_DIR`, `SNAPPY_LIB_DIR` and
`LZ4_LIB_DIR` environment variables.  These environment variables should point
at the directory containing the dynamic shared objects (`.so`s or `.dylib`s).
Please see the
[wiki](https://wiki.near.org/contribute/contribute-nearcore#use-system-provided-rocksdb)
for additional instructions on how to use a system-provided rocksdb
installation.

If your setup does not involve `rustup`, the required version of the rust
toolchain can be found in the `rust-toolchain` file.

## Pull Requests

All the contributions to `nearcore` happen via Pull Requests.  Please follow the
following steps when creating a PR:

1. Fork the `nearcore` repository and create a new branch there to do you work.
2. The branch can contain any number of commits.  When merged, all commits will
   be squashed into a single commit.
3. The changes should be thoroughly tested.  Please refer to [this
   document](https://github.com/nearprotocol/nearcore/wiki/Writing-tests-for-nearcore)
   for our testing guidelines and an overview of the testing infrastructure.
4. When ready, send a pull request against the `master` branch of the `nearcore`
   repository.
5. Feel free to submit draft PRs to get early feedback and to make sure you are
   on the right track.
6. The PR name should follow the template: `<type>: <name>`.  Where `type` is:
   - `fix` for bug fixes;
   - `feat` for new features;
   - `refactor` for changes that reorganize code without adding new content;
   - `doc` for changes that change documentation or comments;
   - `test` for changes that introduce new tests;
   - `chore` for grunt tasks like updating dependencies.
7. The PR should also contain a description when appropriate to provide
   additional information to help the reviewer inspect the proposed change.
8. If your PR introduces a user-observable change (e.g. a new protocol feature,
   new configuration option, new Prometheus metric etc.) please document it in
   [CHANGELOG.md](CHANGELOG.md) in the `[unreleased]` section.
9. It is important to select the `Allow edits from maintainers` checkbox on the
   PR.  Without this option, the merge bot will not have sufficient rights to be
   able to merge the PR when it is approved.  It also allows the maintainers to
   make trivial changes to the PR as necessary.  Please see
   [these](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork)
   [links](https://stackoverflow.com/questions/63341296/github-pull-request-allow-edits-by-maintainers)
   for the implications of selecting the checkbox.

## After the PR is submitted

1. We have a CI process configured to run various tests on each PR.  All tests
need to pass before a PR can be merged.
2. When all the comments from the reviewer(s) have been addressed, they should
approve the PR allowing a PR to be merged.
3. An approved PR can be merged by adding the `S-automerge` label to it.  The
label can be added by the author if they have the appropriate access or by a
reviewer otherwise.  PR authors can also apply label immediately after filing a
PR: removing an additional round-trip after PR is approved.

## Code review process

We have two groups of code reviewers:  Super owners and normal owners.  When a
PR is created:

- a super owner will be automatically assigned to review.
- they may choose to review the PR themselves or they may delegate to someone else
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

## Release Schedule

Once your change ends up in master, it will be released with the rest of the
changes by other contributors on the regular release schedules.

On [betanet](https://docs.near.org/docs/concepts/networks#betanet) we run
nightly build from master with all the nightly protocol feature enabled. Every
five weeks, we stabilize some protocol features and make a release candidate for
testnet.  The process for feature stabilization can be found in [this
document](docs/protocol_upgrade.md).  After the release candidate has been
running on testnet for four weeks and no issues are observed, we stabilize and
publish the release for mainnet.
