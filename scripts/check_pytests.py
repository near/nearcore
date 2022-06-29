#!/usr/bin/env python3
"""Checks whether all pytest tests are mentioned in NayDuck or Buildkite

Lists all Python scripts inside of pytest/tests directory and checks whether
they are referenced in NayDuck test list files (the nightly/*.txt files) or
Buildkite pipeline configuration (the .buildkite/pipeline.yml file).  Returns
with success if that's the case; with failure otherwise.
"""
import fnmatch
import os
import pathlib
import random
import re
import sys
import typing

import yaml

import nayduck

# List of globs of Python scripts in the pytest/tests directory which are not
# test but rather helper scripts and libraries.  The entire mocknet/ directory
# is covered here as well since those tests are not run on NayDuck any more.
HELPER_SCRIPTS = [
    'delete_remote_nodes.py',
    'loadtest/*',
    'mocknet/*',
    'shardnet/*',
    'stress/hundred_nodes/*',
    'loadtest/*',
    'replay/*',
]

PYTEST_TESTS_DIRECTORY = pathlib.Path('pytest/tests')
NIGHTLY_TESTS_FILE = pathlib.Path(nayduck.DEFAULT_TEST_FILE)
BUILDKITE_PIPELINE_FILE = pathlib.Path('.buildkite/pipeline.yml')

StrGenerator = typing.Generator[str, None, None]


def list_test_files(topdir: pathlib.Path) -> StrGenerator:
    """Yields all *.py files in a given directory traversing it recursively.

    Args:
        topdir: Path to the directory to traverse.  Directory is traversed
            recursively.
    Yields:
        Paths (as str objects) to all the Python source files in the directory
        relative to the top directory.  __init__.py files (and in fact any files
        starting with __) are ignored.
    """
    for dirname, _, filenames in os.walk(topdir):
        dirpath = pathlib.Path(dirname).relative_to(topdir)
        for filename in filenames:
            if not filename.startswith('__') and filename.endswith('.py'):
                yield str(dirpath / filename)


def read_nayduck_tests(path: pathlib.Path) -> StrGenerator:
    """Reads NayDuck test file and yields all tests mentioned there.

    The NayDuck test list files are ones with .txt extension.  Only pytest and
    mocknet tests are taken into account and returned.  Enabled tests as well as
    those commented out with a corresponding TODO comment are considered.

    Args:
        path: Path to the test set.
    Yields:
        pytest and mocknet tests mentioned in the file.  May include duplicates.
    """

    def extract_name(line: str) -> StrGenerator:
        tokens = line.split()
        idx = 1 + (tokens[0] == '#')
        while idx < len(tokens) and tokens[idx].startswith('--'):
            idx += 1
        if idx < len(tokens):
            yield tokens[idx]

    found_todo = False
    for line in nayduck.read_tests_from_file(path, include_comments=True):
        line = re.sub(r'\s+', ' ', line.strip())
        if re.search(r'^(?:pytest|mocknet) ', line):
            found_todo = False
            yield from extract_name(line)
        elif found_todo and re.search(r'^# ?(?:pytest|mocknet) ', line):
            yield from extract_name(line)
        elif re.search('^# ?TODO.*#[0-9]{4,}', line):
            found_todo = True
        elif not line.strip().startswith('#'):
            found_todo = False


def read_pipeline_tests(filename: pathlib.Path) -> StrGenerator:
    """Reads pytest tests mentioned in Buildkite pipeline file.

    The parsing of the pipeline configuration is quite naive.  All this function
    is looking for is a "cd pytest" line in a step's command followed by
    "python3 tests/<name>" lines.  The <name> is yielded for each such line.

    Args:
        filename: Path to the Buildkite pipeline configuration file.
    Yields:
        pytest tests mentioned in the commands in the configuration file.
    """
    with open(filename) as rd:
        data = yaml.load(rd, Loader=yaml.SafeLoader)
    for step in data.get('steps', ()):
        in_pytest = False
        for line in step.get('command', '').splitlines():
            line = line.strip()
            line = re.sub(r'\s+', ' ', line)
            if line == 'cd pytest':
                in_pytest = True
            elif in_pytest and line.startswith('python3 tests/'):
                yield line.split()[1][6:]


def print_error(missing: typing.Collection[str]) -> None:
    """Formats and outputs an error message listing missing tests."""
    this_file = os.path.relpath(__file__)
    example = random.sample(tuple(missing), 1)[0]
    if example.startswith('mocknet/'):
        example = 'mocknet ' + example
    else:
        example = 'pytest ' + example
    msg = '''\
Found {count} pytest file{s} (i.e. Python file{s} in pytest/tests directory)
which are not included in any of the nightly/*.txt files:

{missing}

Add the test{s} to one of the lists in nightly/*.txt files.  For example as:

    {example}

If the test is temporarily disabled, add it to one of the files with an
appropriate {todo} comment.  For example:

    # {todo}(#1234): Enable the test again once <some condition>
    # {example}

Note that the {todo} comment must reference a GitHub issue (i.e. must
contain a #<number> string).

If the file is not a test but a helper library, consider moving it out
of the pytest/tests directory to pytest/lib or add it to HELPER_SCRIPTS
list at the top of {this_file} file.'''.format(
        count=len(missing),
        s='' if len(missing) == 1 else 's',
        missing='\n'.join(
            ' -  pytest/tests/' + name for name in sorted(missing)),
        example=example,
        this_file=this_file,
        todo='TO'
        'DO',
    )
    print(msg, file=sys.stderr)


def main() -> int:
    """Main function of the script; returns integer exit code."""
    missing = set(list_test_files(PYTEST_TESTS_DIRECTORY))
    count = len(missing)
    missing.difference_update(
        read_nayduck_tests(pathlib.Path(nayduck.DEFAULT_TEST_FILE)))
    missing.difference_update(read_pipeline_tests(BUILDKITE_PIPELINE_FILE))
    missing = set(filename for filename in missing if not any(
        fnmatch.fnmatch(filename, pattern) for pattern in HELPER_SCRIPTS))
    if missing:
        print_error(missing)
        return 1
    print(f'All {count} tests included')
    return 0


if __name__ == '__main__':
    sys.exit(main())
