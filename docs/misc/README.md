# Overview

This chapter holds various assorted bits of docs. If you want to document
something, but don't know where to put it, put it here!

## Crate Versioning and Publishing

While all the crates in the workspace are directly unversioned (`v0.0.0`), they
all share a unified variable version in the [workspace manifest](Cargo.toml).
This keeps versions consistent across the workspace and informs their versions
at the moment of publishing.

We also have CI infrastructure set up to automate the publishing process to
crates.io. So, on every merge to master, if there's a version change, it is
automatically applied to all the crates in the workspace and it attempts to
publish the new versions of all non-private crates. All crates that should be
exempt from this process should be marked `private`. That is, they should have
the `publish = false` specification in their package manifest.

This process is managed by
[cargo-workspaces](https://github.com/pksunkara/cargo-workspaces), with a
[bit of magic](https://github.com/pksunkara/cargo-workspaces/compare/master...miraclx:grouping-and-exclusion#files_bucket)
sprinkled on top.

## Issue Labels

Issue labels are of the following format `<type>-<content>` where `<type>` is a
capital letter indicating the type of the label and `<content>` is a hyphened
phrase indicating what this label is about. For example, in the label `C-bug`,
`C` means category and `bug` means that the label is about bugs. Common types
include `C`, which means category, `A`, which means area and `T`, which means team.

An issue can have multiple labels including which area it touches, which team
should be responsible for the issue, and so on. Each issue should have at least
one label attached to it after it is triaged and the label could be a general
one, such as `C-enhancement` or `C-bug`.
