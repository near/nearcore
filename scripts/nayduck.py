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
import os
import pathlib
import subprocess


DEFAULT_TEST_FILE = 'nightly/nightly.txt'
NAYDUCK_BASE_HREF = 'http://nayduck.eastus.cloudapp.azure.com:5005'


def _parse_args():
    import argparse

    default_test_path = (pathlib.Path(__file__).parent.parent /
                         DEFAULT_TEST_FILE)

    parser = argparse.ArgumentParser(description='Run tests.')
    parser.add_argument('--branch', '-b',
                        help='Branch to test. By default gets current one.' )
    parser.add_argument('--sha', '-s',
                        help='Sha to test. By default gets current one.')
    parser.add_argument('--test-file', '-t', default=default_test_path,
                        help=('Test file with list of tests. '
                              f'By default {DEFAULT_TEST_FILE}'))
    return parser.parse_args()


def get_curent_sha():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True)

def get_current_branch():
    return subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                                   text=True)

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

    If include_comments is True, `#./<path>` lines are handled as well with all
    included line commented out.  This is useful to comment out a include a with
    TODO comment included and have check_nightly.py and check_pytest.py scripts
    still recognise those includes.  Note that the line must start with `#./`,
    i.e. there must be no space between hash and the dot.

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

    def impl(path: pathlib.Path, depth: int, comment: bool = False):
        for lineno, line in enumerate(reader(path).splitlines()):
            line = line.rstrip()
            if line.startswith('./') or (include_comments and
                                         line.startswith('#./')):
                if depth == 3:
                    print(f'{path}:{lineno+1}: ignoring {line}; '
                          f'would exceed depth limit of {depth}')
                else:
                    incpath = line[1:] if line.startswith('#') else line
                    yield from impl(path.parent / incpath,
                                    depth + 1,
                                    comment or line.startswith('#'))
            elif include_comments or (line and line[0] != '#'):
                if comment and not line.startswith('#'):
                    line = '#' + line
                yield line

    return impl(path, 1)


def github_auth(code_path: pathlib.Path):
    print('Go to the following link in your browser:\n\n{}/login/cli\n'.format(
        NAYDUCK_BASE_HREF))
    code = input('Enter authorisation code: ')
    code_path.parent.mkdir(parents=True, exist_ok=True)
    code_path.write_text(code)
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

    code_path = pathlib.Path(
        os.environ.get('XDG_CONFIG_HOME') or
        pathlib.Path.home() / '.config') / 'nayduck-code'
    if code_path.is_file():
        code = code_path.read_text().strip()
    else:
        code = github_auth(code_path)

    post = {
        'branch': args.branch or get_current_branch().strip(),
        'sha': args.sha or get_curent_sha().strip(),
        'tests': list(read_tests_from_file(pathlib.Path(args.test_file))),
    }

    while True:
        print('Sending request ...')
        res = requests.post(
            'http://nayduck.eastus.cloudapp.azure.com:5005/api/run/new',
            json=post, cookies={'nay-code': code})
        if res.status_code != 401:
            break
        print(f'{styles[0]}Unauthorised.{styles[2]}\n')
        code = github_auth(code_path)

    if res.status_code == 200:
        json_res = json.loads(res.text)
        print(styles[json_res['code'] == 0] + json_res['response'] + styles[2])
    else:
        print(f'{styles[0]}Got status code {res.status_code}:{styles[2]}\n')
        print(res.text)

    code = res.cookies.get('nay-code')
    if code:
        code_path.write_text(code)


if __name__ == "__main__":
    main()
