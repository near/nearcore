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
                         include_comments: bool=False, depth: int=0):
    if depth >= 3:
        print(f'Ignoring {path}; reached depth limit of {depth}')
        return
    with open(path) as rd:
        for line in rd:
            line = line.strip()
            if line.startswith('./'):
                yield from read_tests_from_file(
                    path, include_comments=include_comments, depth=depth + 1)
            elif include_comments or (line and line[0] != '#'):
                yield line


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
