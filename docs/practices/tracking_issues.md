# Tracking issues

`nearcore` uses so-called "tracking issues" to coordinate larger pieces of work
(e.g. implementation of new NEPs).  Such issues are tagged with the
[`C-tracking-issue`
label](https://github.com/near/nearcore/issues?q=is%3Aopen+is%3Aissue+label%3AC-tracking-issue).

The goal of tracking issues is to serve as a coordination point. They can help
new contributors and other interested parties come up to speed with the current
state of projects. As such, they should link to things like design docs,
todo-lists of sub-issues, existing implementation PRs, etc.

One can further use tracking issues to:

- get a feeling for what's happening in `nearcore` by looking at the set of
  open tracking issues.
- find larger efforts to contribute to as tracking issues usually contain
  up-for-grab to-do lists.
- follow the progress of specific features by subscribing to the issue on GitHub.

If you are leading or participating in a larger effort, please create a tracking
issue for your work.

## Guidelines

- Tracking issues should be maintained in the `nearcore` repository. If the
  projects are security sensitive, then they should be maintained in the
  `nearcore-private` repository.
- The issues should be kept up-to-date. At a minimum, all new context
  should be added as comments, but preferably the original description should be
  edited to reflect the current status.
- The issues should contain links to all the relevant design documents
  which should also be kept up-to-date.
- The issues should link to any relevant NEP if applicable.
- The issues should contain a list of to-do tasks that should be kept
  up-to-date as new work items are discovered and other items are done. This
  helps others gauge progress and helps lower the barrier of entry for others to
  participate.
- The issues should contain links to relevant Zulip discussions. Prefer
  open forums like Zulip for discussions. When necessary, closed forums like
  video calls can also be used but care should be taken to document a summary of
  the discussions.
- For security-sensitive discussions, use the appropriate private Zulip streams.

[This issue](https://github.com/near/nearcore/issues/7670) is a good example of
how tracking issues should be maintained.

## Background

The idea of tracking issues is also used to track project work in the Rust
language. See [this
post](https://internals.rust-lang.org/t/how-the-rust-issue-tracker-works/3951)
for a rough description and
[these](https://github.com/rust-lang/rust/issues/101840)
[issues](https://github.com/rust-lang/rust/issues/100717) for how they are used
in Rust.
