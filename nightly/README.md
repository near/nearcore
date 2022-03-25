# Nightly tests lists

The directory contains test list files which can be sent to NayDuck to
request a run of the tests.  Most notably, `nightly.txt` file contains
all the tests that NayDuck runs once a day on the head of the master
branch of the repository.

Nightly build results are available on [NayDuck](https://nayduck.near.org/).

## List file format

Each list file is read line-by line.  Empty lines and lines starting
with a hash are ignored.  The rest either specifies a test to run or
a list file to include.  The general syntax of a line defining a test
is:

    <category> [--skip-build] [--timeout=<timeout>] [--release] [--remote] <args>... [--features <features>]

`<category>` specifies the category of the test and can be `pytest` or
`expensive`.  The meaning of `<args>` depends on the test category.

### pytest tests

The `pytest` tests are run by executing a file within `pytest/tests`
directory.  The file is executed via `python` interpreter (and
confusingly not via `pytest` command) so it must actually run the
tests rather than only defining them as test functions.

In the test specification path to the file needs to be given
(excluding the `pytest/tests` prefix) and anything that follows is
passed as arguments to the script.  For example:

    pytest sanity/lightclnt.py
    pytest sanity/state_sync_routed.py manytx 115

Note: NayDuck also handles `mocknet` test category.  It is now
deprecated and is treated like `pytest` with `--skip-build` flag
implicitly set.

### expensive tests

The `expensive` tests run a test binary and execute specific test in
it.  (Test binaries are those built via `cargo test --no-run`).  While
this can be used to run any Rust test, the intention is to run
expensive tests only.  Those are the tests which are ignored unless
`expensive_tests` crate feature is enabled.  Such tests should be
marked with a `cfg_attr` macro, e.g.:

    #[test]
    #[cfg_attr(not(feature = "expensive_tests"), ignore)]
    fn test_gc_boundaries_large() {
        /* ... */
    }

The arguments of an expensive test specify package in which the test
is defined, test binary name and the full path to the test function.
For example:

    expensive nearcore test_tps_regression test::test_highload

(Currently the package name is ignored but it may change in the future
so make sure it’s set correctly).  The path to the test function must
match exactly an the test binary is called with `--exact` argument.

### Other arguments

As mentioned, there are additional arguments that can go between the
test category and the test specification arguments.  Those are
`--skip-build`, `--timeout`, `--release` and `--remote`.

`--skip-build` causes build step to be skipped for the test.  This
means that the test doesn’t have access to build artefacts (located in
`target/debug` or `target/release`) but also doesn’t need to wait for
the build to finish and thus can start faster.

`--timeout=<timeout>` specifies the time after which the test will be
stopped and considered failed.  `<timeout>` is an integer with an
optional `s`, `m` or `h` suffix.  If no suffix is given, `s` is
assumed.  The default timeout is three minutes.  For example, the
following increases timeout for a test to four minutes:

    pytest --timeout=4m sanity/validator_switch.py

`--release` makes the build use a release profile rather than a dev
profile.  In other words, all `cargo` invocations are passed
additional `--release` argument.  This is supported but currently not
used by any of the nightly tests.

`--remote` configures pytest tests to use Near test nodes started on
spot GCP machines rather than executing a small cluster on host where
the test is running.  No nightly test uses this feature and to be
honest I can’t vouch whether it even works.

Lastly, at the end of the test specification line additional features
can be given in the form of `--features <features>` arguments.
Similarly to `--release`, this results in given features being enabled
in builds.  Note that the `test_features` Cargo feature is always
enabled so there's no need to specify it explicitly.

Note that with `--skip-build` switch the `--release` and `--features`
flags are essentially ignored since they only affect the build and are
not passed to the test.

### Include directive

To help organise tests, the file format also supports `./<path>`
syntax for including contents of other files in the list.  The
includes are handled recursively though at the moment there’s a limit
of three levels before the parser starts ignoring the includes.

For example, `nightly.txt` file may just look as follows:

    ./sandbox.txt
    ./pytest.txt
    ./expensive.txt

with individual tests listed in each of the referenced files.  This
makes the files more tidy.

Note that any includes accessible from `nightly.txt` file must live
within the `nightly` directory and use `.txt` file extension.  Using
arbitrary paths and extensions will work locally but it will break
NayDuck’s nightly runs.


## Scheduling a run

Every 24 hours NayDuck checks if master branch has changed and if it
has schedules a new run including all tests listed in the
`nightly.txt` file.  It’s also possible to request a run manually in
which case arbitrary set of tests can be run on an arbitrary commit
(so long as it exists in the near/nearcore repository).

This can be done with `nayduck.py` script which takes the list file as
an argument.  For example, to run spec tests one might invoke:

    ./scripts/nayduck.py -t nightly/pytest-spec.txt

With no other arguments the tests will be run against checked out
commit in the current branch.  It is possible to specify which branch
and commit to run against with `--branch` and `--sha` arguments.  For
full usage refer to `./scripts/nayduck.py --help` output.

NayDuck cannot run tests against local working directory or even
commits in a private fork of the nearcore repository.  To schedule
a NayDuck run, the commit must first be pushed to nearcore.  The
commit does not need to be on master branch; testing a feature branch
is supported.

On success the script outputs link to the page which can be used to
see status of the run.  Depending on which tests were scheduled the
run can take over an hour to finish.


## Creating new tests

New tests can be created either as a Rust test or a pytest.

If a Rust test is long-running (or otherwise requires a lot of
resources) and intended to be run as a nightly test on NayDuck it
should be marked with a `#[cfg(feature = "expensive_tests")]`
directive (either on the test function or module containing it).  With
that, the tests will not be part of a `cargo test` run performed on
every commit, but NayDuck will be able to execute them.  Apart from
that, expensive Rust tests work exactly the same as any other Rust
tests.

pytests are defined as scripts in the `pytest/tests` directory.  As
previously mentioned, even though the directory is called pytest, when
run on NayDuck they scripts are run directly via `python`.  This means
that they need to execute the tests when run as the main module rather
than just defining the tests function.  To make that happen it’s best
to define `test_<foo>` functions with test bodies and than execute all
those functions in a code fragment guarded by `if __name__ ==
'__main__'` condition.

### Check scripts

Unless tests are included (potentially transitively) in `nightly.txt`
file, NayDuck won’t run them.  As part of pull request checks,
verification is performed to make sure that no test is forgotten and
all new tests are included in the nightly list.  That’s done by
`scripts/check_nightly.txt` and `scripts/check_pytest.txt` scripts.
The list all the expensive and pytest tests defined in the repository
and then check whether they are all mentioned in the nightly list.

The scripts recognise commented out tests so if a test is broken it
can be removed from the list by commenting it out.  However, such
a test must be proceeded by a TODO comment mentioning an issue which
tracks the breakage.  For example:

    # TODO(#2411): Enable them when we fix the issue with proxy shutdown
    #pytest --timeout=900 sanity/sync_ban.py true
    #pytest --timeout=900 sanity/sync_ban.py false

The include directive can be commented out like that as well though
crucially there must be no space between the hash sign and dot.  For
example:

    # TODO(#2411): Working on a fix.
    #./bridge.txt
