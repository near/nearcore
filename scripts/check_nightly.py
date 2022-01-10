#!/usr/bin/env python3

import os
import pathlib
import re
import sys
import typing

import nayduck

IGNORED_SUBDIRS = ('target', 'target_expensive', 'sandbox')

EXPENSIVE_DIRECTIVE = '#[cfg_attr(not(feature = "expensive_tests"), ignore)]'
TEST_DIRECTIVE = '#[test]'


def expensive_tests_in_file(path: pathlib.Path) -> typing.Iterable[str]:
    """Yields names of expensive tests found in given Rust file.

    An expensive test is a function annotated with `test` and a conditional
    `ignore` attributes, specifically:

        #[test]
        #[cfg_attr(not(feature = "expensive_tests"), ignore)]
        fn test_slow() {
            // ...
        }

    Note that anything more complex in the `cfg_attr` will cause the function
    not to recognise the test.

    Args:
        path: Path to the Rust source file.
    Yields:
        Names of functions defining expensive tests (e.g. `test_slow` in example
        above).
    """
    with open(path) as rd:
        is_expensive = False
        is_test = False
        for line in rd:
            line = line.strip()
            if not line:
                pass
            elif line.startswith('#'):
                is_expensive = is_expensive or line == EXPENSIVE_DIRECTIVE
                is_test = is_test or line == TEST_DIRECTIVE
            elif is_expensive or is_test:
                if is_expensive and is_test:
                    match = re.search(r'\bfn\s+([A-Za-z_][A-Za-z_0-9]*)\b',
                                      line)
                    if match:
                        yield match.group(1)
                is_expensive = False
                is_test = False


def nightly_tests(repo_dir: pathlib.Path) -> typing.Iterable[str]:
    """Yields expensive tests mentioned in the nightly configuration file."""
    for test in nayduck.read_tests_from_file(repo_dir /
                                             nayduck.DEFAULT_TEST_FILE,
                                             include_comments=True):
        t = test.split()
        try:
            # It's okay to comment out a test intentionally.
            if t[t[0] == '#'] in ('expensive', '#expensive'):
                yield t[-1].split('::')[-1]
        except IndexError:
            pass


def main() -> typing.Optional[str]:
    repo_dir = pathlib.Path(__file__).parent.parent
    nightly_txt_tests = set(nightly_tests(repo_dir))
    for root, dirs, files in os.walk(repo_dir):
        dirs[:] = [
            dirname for dirname in dirs if dirname not in IGNORED_SUBDIRS
        ]
        path = pathlib.Path(root)
        for filename in files:
            if filename.endswith('.rs'):
                filepath = path / filename
                print(f'checking file {filepath}')
                for test in expensive_tests_in_file(filepath):
                    print(f'  expensive test {test}')
                    if test not in nightly_txt_tests:
                        return f'error: file {filepath} test {test} not in nightly.txt'
    print('all tests in nightly')
    return None


if __name__ == '__main__':
    sys.exit(main())
