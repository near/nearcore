# Tracking issues

`nearcore` uses so called "tracking issues" to coordinate larger pieces of work
(e.g. implementation of new NEPs).  Such issues are tagged with the
[`C-tracking-issue`
label](https://github.com/near/nearcore/issues?q=is%3Aopen+is%3Aissue+label%3AC-tracking-issue).

One can use tracking issues to:
- get a feeling for what's happening in `nearcore` by looking at the set of
  open tracking issues.
- find larger efforts to contribute to as tracking issues usually contain
  up-for-grabs todo lists.
- follow progress of a specific features by subscribing to the issue on Github.

If you are leading or participating in a larger effort, please create a tracking
issue for your work.

## Guidelines

- There should be one tracking issue per project.  Discretion can be used to
  decide what kind of project necessitates a tracking issue.
- Tracking issues should be maintained on the `nearcore` github repository.  If
  the projects are security sensitive, they should be tracked on the
  `nearcore-private` github repository.
- The tracking issues should contain links to all the relevant design documents.
  Care should be taken that when the design changes during the development
  lifecycle (which should be expected as norm), that the summary is kept
  up-to-date.
- If the feature development is associated with a NEP, then the above issue
  should have a link to the said NEP and the NEP should be kept up-to-date in
  accordance with the guidelines.
- The above issue should maintain a list of tasks done and list of tasks left to
  do.  This allows one to estimate the progress on the feature and when it might
  be delivered.  It can also lower the barrier to entry for other engineers to
  participate in feature development.
- Links to Zulip discussions pertaining to the feature development.  In general,
  engineers are encouraged to have discussions on Zulip as it is an open forum
  and affords everyone the opportunity to participate.  However, sometimes it is
  necessary to discuss design in other forums that are closed (e.g. video
  calls).  Such discussions are also perfectly fine.  Care should be taken to
  update the relevant Zulip discussions and the summary of the design.
- Please avoid using slack and DM messages for engineering discussions.  We
  encourage all discussions to happen on open Zulip streams.  For security
  sensitive discussions, please use the appropriate private Zulip streams.

## Background

The idea of tracking issues is also used to track project work in the Rust
language.  See [this
post](https://internals.rust-lang.org/t/how-the-rust-issue-tracker-works/3951)
for a rough description and
[these](https://github.com/rust-lang/rust/issues/101840)
[issues](https://github.com/rust-lang/rust/issues/100717) for how they are used
in Rust.
