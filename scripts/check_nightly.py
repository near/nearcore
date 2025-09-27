#!/usr/bin/env python3
"""Checks whether all expensive tests are mentioned in NayDuck tests list

Scans all Rust source files looking for expensive tests and than makes sure that
they are all referenced in NayDuck test list files (the nightly/*.txt files).
Returns with success if that's the case; with failure otherwise.

An expensive test is one which beings with the `ultra_slow_test_` prefix:

    #[test]
    fn ultra_slow_test_gc_random_large() {
        test_gc_random_common(25);
    }

Expensive tests are not executed when running `cargo test` nor are they run in
CI and it's the purpose of this script to make sure that they are listed for
NayDuck to run.
"""

import os
import pathlib
import re
import sys
import typing

import nayduck

IGNORED_SUBDIRS = ("target", "target_expensive", "sandbox")

EXPENSIVE_DIRECTIVES = ["fn ultra_slow_test_", "async fn ultra_slow_test_"]
TEST_DIRECTIVES = ["#[test]", "#[tokio::test]"]


def expensive_tests_in_file(path: pathlib.Path) -> typing.Iterable[str]:
    """Yields names of expensive tests found in given Rust file.
    Args:
        path: Path to the Rust source file.
    Yields:
        Names of functions defining expensive tests (e.g. `ultra_slow_test_gc_random_large`
        in example above).
    """
    with open(path) as rd:
        is_test = False
        for line in rd:
            line = line.strip()
            if not line:
                pass
            elif line.startswith("#"):
                is_test = is_test or line in TEST_DIRECTIVES
            elif any(
                    line.startswith(directive)
                    for directive in EXPENSIVE_DIRECTIVES):
                if is_test:
                    match = re.search(r"\bfn\s+([A-Za-z_][A-Za-z_0-9]*)\b",
                                      line)
                    if match:
                        yield match.group(1)
                is_test = False


def nightly_tests(repo_dir: pathlib.Path) -> typing.Iterable[str]:
    """Yields expensive tests mentioned in the nightly configuration file."""
    for test in nayduck.read_tests_from_file(repo_dir /
                                             nayduck.DEFAULT_TEST_FILE,
                                             include_comments=True):
        t = test.split()
        try:
            # It's okay to comment out a test intentionally.
            if t[t[0] == "#"] in ("expensive", "#expensive"):
                yield t[-1].split("::")[-1]
        except IndexError:
            pass


def main() -> typing.Optional[str]:
    repo_dir = pathlib.Path(__file__).parent.parent
    # Once we see the tests in the repo, we mark the test as found.
    nightly_txt_tests = {i: False for i in nightly_tests(repo_dir)}
    for root, dirs, files in os.walk(repo_dir):
        dirs[:] = [
            dirname for dirname in dirs if dirname not in IGNORED_SUBDIRS
        ]
        path = pathlib.Path(root)
        for filename in files:
            if filename.endswith(".rs"):
                filepath = path / filename
                print(f"checking file {filepath}")
                for test in expensive_tests_in_file(filepath):
                    print(f"  expensive test {test}")
                    if test not in nightly_txt_tests:
                        return f"error: file {filepath} test {test} not in ci.txt"
                    # Marking nightly test as found.
                    nightly_txt_tests[test] = True
    for test, found in nightly_txt_tests.items():
        # Not sure why are we yielding `nightly`
        if not found and test != "nightly":
            return f"error: test {test} not found in repo"
    print("all tests in nightly")
    return None


if __name__ == "__main__":
    sys.exit(main())
