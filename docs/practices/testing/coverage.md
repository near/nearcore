# Test Coverage

In order to focus the testing effort where it is most needed, we have a few
ways we track test coverage.

## Codecov

The main one is Codecov. Coverage is visible [on this
webpage](https://app.codecov.io/gh/near/nearcore), and displays the total
coverage, including unit and integration tests. Codecov is especially
interesting for [its PR
comments](https://github.com/near/nearcore/pull/10731#issuecomment-1985356880).
The PR comments, in particular, can easily show which diff lines are being
tested and which are not.

However, sometimes Codecov gives too rough estimates, and this is where
artifact results come in.

## Artifact Results

We also push artifacts, as a result of each CI run. You can access them here:
1. Click "Details" on one of the CI actions run on your PR (literally any one
   of the actions is fine, you can also access CI actions runs on any CI)
2. Click "Summary" on the top left of the opening page
3. Scroll to the bottom of the page
4. In the "Artifacts" section, just above the "Summary" section, there is a
   `coverage-html` link (there is also `coverage-lcov` for people who use eg.
   the coverage gutters vscode integration)
5. Downloading it will give you a zip file with the interesting files.

In there, you can find:
- Two `-diff` files, that contain code coverage for the diff of your PR, to
  easily see exactly which lines are covered and which are not
- Two `-full` folders, that contain code coverage for the whole repository
- Each of these exists in one `unit-` variant, that only contains the unit
  tests, and one `integration-` variant, that contains all the tests we
  currently have


**To check that your PR is properly tested**, if you want better quality
coverage than what codecov "requires," you can have a look at `unit-diff`,
because we agreed that we want unit tests to be able to detect most bugs
due to the troubles of debugging failing integration tests.

**To find a place that would deserve adding more tests**, look at one of the
`-full` directories on master, pick one not-well-tested file, and add (ideally
unit) tests for the lines that are missing.

The presentation is unfortunately less easy to access than codecov, and less
eye-catchy. On the other hand, it should be more precise. In particular, the
`-full` variants show region-based coverage. It can tell you that eg. the `?`
branch is not covered properly by highlighting it red.

One caveat to be aware of: the `-full` variants do not highlight covered lines
in green, they just highlight non-covered lines in red.
