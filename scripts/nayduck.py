#!/usr/bin/env python

# This script runs integration tests in the cloud.  You can see the runs here:
#
#     http://nayduck.eastus.cloudapp.azure.com:3000/
#
# To request a run, use the following command:
#
#    python3 scripts/nayduck.py      \
#        --branch    <your_branch>   \
#        --test_file <test_file>.txt
#
# See the `.txt` files in this directory for examples of test suites. Note that
# you must be a *public* memeber of the near org on GitHub to authenticate:
#
#    https://github.com/orgs/near/people
#
# The source code for nayduck itself is here:
#
#    https://github.com/utka/nayduck

import json
import pathlib
import subprocess


DEFAULT_TEST_FILE = 'tests_for_nayduck.txt'
DEFAULT_TEST_PATH = (pathlib.Path(__file__).parent.parent /
                     'nightly' / DEFAULT_TEST_FILE)


def _parse_args():
    import argparse
    parser = argparse.ArgumentParser(description='Run tests.')
    parser.add_argument('--branch', '-b',
                        help='Branch to test. By default gets current one.' )
    parser.add_argument('--sha', '-s',
                        help='Sha to test. By default gets current one.')
    parser.add_argument('--test-file', '-t', default=DEFAULT_TEST_PATH,
                        help=('Test file with list of tests. '
                              f'By default {DEFAULT_TEST_FILE}'))
    return parser.parse_args()


def get_curent_sha():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True)

def get_current_branch():
    return subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                                   text=True)

def get_current_user():
    return subprocess.check_output(['git', 'config', 'user.name'], text=True)


def read_tests_from_file(path: pathlib.Path, *,
                         include_comments: bool=False,
                         reader=lambda path: path.read_text()):
    """Reads lines from file in given directory handling `./<path>` includes.

    Returns an iterable over lines in given file but also handles `./<path>`
    syntax for including other files in the output and optionally filters
    commented out lines.

    A `./<path>` syntax acts like C's #include directive, Rust's `include!`
    macro or shell's `source` command.  All lines are read from file at <path>
    as if they were directly in the source file.  The `./<path>` directives are
    handled recursively up to three levels deep.

    Args:
        path: Path to the file to read.
        include_comments: By default empty lines and lines whose first non-space
            character is hash are ignored and not included in the output.  With
            this set to `True` such lines are included as well.
        reader: A callback which reads text content of a given file.  This is
            used by NayDuck.
    Returns:
        An iterable over lines in the given file.  All lines are stripped of
        leading and trailing white space.
    """
    def impl(path: pathlib.Path, depth: int):
        for lineno, line in enumerate(reader(path).splitlines()):
            line = line.strip()
            if line.startswith('./'):
                if depth == 3:
                    print(f'{path}:{lineno+1}: ignoring {line}; '
                          f'would exceed depth limit of {depth}')
                else:
                    yield from impl(path.parent / line, depth + 1)
            elif include_comments or (line and line[0] != '#'):
                yield line

    return impl(path, 1)


def github_auth():
    print("Go to the following link in your browser:")
    print()
    print("http://nayduck.eastus.cloudapp.azure.com:3000/local_auth")
    print()
    code = input("Enter verification code: ")
    (pathlib.Path.home() / '.nayduck', 'w').write_text(code)
    return code


def main():
    try:
        import colorama
        styles = (colorama.Fore.RED, colorama.Fore.GREEN,
                  colorama.Style.RESET_ALL)
    except ImportError:
        styles = ('', '', '')
    import requests

    args = _parse_args()

    path = pathlib.Path.home() / '.nayduck'
    if path.is_file():
        token = path.read_text().strip()
    else:
        token = github_auth()

    post = {
        'branch': args.branch or get_current_branch().strip(),
        'sha': args.sha or get_curent_sha().strip(),
        'requester': get_current_user().strip(),
        'tests': list(read_tests_from_file(pathlib.Path(args.test_file))),
        'token': token
    }

    print('Sending request ...')
    res = requests.post(
        'http://nayduck.eastus.cloudapp.azure.com:5005/request_a_run',
        json=post)
    json_res = json.loads(res.text)
    print(styles[json_res['code'] == 0] + json_res['response'] + styles[2])


if __name__ == "__main__":
    main()
