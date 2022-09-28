# Tracking issues

`nearcore` uses tracking issues on github to make it easier to follow progress
on individual projects.

## Purpose

Different types of audiences want to track progress on feature development on `nearcore`:
- Other engineers contributing to the feature development but who may have
  missed some progress while they were unavailable.
- Pagoda management who would like to have an overview of which teams are
  working on what features and how much work is left to be done.
- The general NEAR community who are interested in development of new features
  in the protocol.

Using tracking issues can help the above audience come up to speed with projects
quickly and makes it easy for engineers working on projects to share updates.

## Guidelines

- There should be one tracking issue per project.  Discretion can be used to
  decide what kind of project necessitates a tracking issue.
- Tracking issues should be maintained on the `nearcore` github repository.  If
  the projects are security sensitive, they should be tracked on the
  `nearcore-private-1` github repository.
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
