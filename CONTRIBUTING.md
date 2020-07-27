Thank you for considering contributing to the NEAR reference client!

We welcome all external contributions. This document outlines the process of contributing to nearcore. 
For contributing to other repositories, see `CONTRIBUTING.md` in the corresponding repository. 
For non-technical contributions, such as e.g. content or events, see [this document](https://docs.nearprotocol.com/docs/contribution/contribution-overview).

# Pull Requests and Issues

All the contributions to `nearcore` happen via Pull Requests. To create a Pull Request, fork `nearcore`, create a new branch, do the work there, and then send the PR via Github interface.

The PRs should always be against the `master` branch.

The exact process depends on the particular contribution you are making.

## Typos or small fixes

If you see an obvious typo, or an obvious bug that can be fixed with a small change, in the code or documentation, feel free to submit the pull request that fixes it without opening an issue.

## Working on current tasks

If you have never contributed to nearcore before, take a look at the work items in the issue tracker labeled with `good first issue` [here](https://github.com/nearprotocol/nearcore/labels/good%20first%20issue) and `good first test` [here](https://github.com/nearprotocol/nearcore/labels/good%20first%20test). If you see one that looks interesting, and is not claimed, please comment on the issue that you would like to start working on it, and someone from the team will assign it to you.

Keep in mind the following:

1. The changes need to be thoroughly tested. Refer to [this document](https://github.com/nearprotocol/nearcore/wiki/Writing-tests-for-nearcore) for our testing guidelines and overview of the testing infrastructure.
2. Because of (1), starting with a `good first test` task is a good idea, since it helps you familiarize yourself with the testing infrastructure.
3. If you get an issue assigned to you, please post updates at least once a week. It is also preferred for you to send a draft PR as early as you have something working, before it is ready.

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

### After the PR is submitted

1. We have a CI process configured to run all the sanity tests on each PR. If the CI fails on your PR, you need to fix it before it will be reviewed.
2. Once the CI passes, you should expect the first feedback to appear within 48 hours. The reviewers will first review your tests, and make sure that they can convince themselves the test coverage is adequate before they even look into the change, so make sure you tested all the corner cases.
3. Once you address all the comments, and your PR is accepted, we will take care of merging it.

## Proposing new ideas and features

If you want to propose an idea or a feature and work on it, create a new issue in the `nearcore` repository. We presently do not have an issue template.

You should expect someone to comment on the issue within 48 hours after it is created. If the proposal in the issue is accepted, you should then follow the process for `Working on current tasks` above.

# Setting up the environment

We use nightly Rust features, so you will need nightly rust installed. See [this document](https://doc.rust-lang.org/1.2.0/book/nightly-rust.html) for details.

Majority of NEAR developers use CLion with Rust plugin as their primary IDE.

We also had success with VSCode with rust-analyzer, see the steps for installation [here](https://commonwealth.im/near/proposal/discussion/338-remote-development-with-vscode-and-rustanalyzer).

Some of us use VIM with [rust.vim](https://github.com/rust-lang/rust.vim) and [rusty-tags](https://github.com/dan-t/rusty-tags). It has fewer features than CLion or VSCode, but overall provides a usable setting.

Refer to [this document](https://docs.nearprotocol.com/docs/contribution/nearcore) for details on setting up your environment.

# Release Schedule

Once your change ends up in master, it will be released with the rest of the changes by other contributors on the regular release schedules.

You should expect the changes from `master` to get merged into `beta` branch the next time `nightly` test run completes, assuming it passes. 
Releases to the `stable` branch are manual, but generally contain a contiguous prefix of commits from `beta` branch.
Note, that the goal is to maintain `beta` as stable as possible and `stable` completely stable. Hence if your change is breaking something that gets detected down the line - it will rolled back and requested to address the issue with additional test coverage.
