#!/usr/bin/env python

# This script runs integration tests in the cloud.  You can see the runs here:
#
#     http://nayduck.eastus.cloudapp.azure.com:3000/
#
# To request a run, use the following command:
#
#    python3 nightly/nayduck.py      \
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

import argparse
import subprocess
import requests

import json
import os

from colorama import Fore


parser = argparse.ArgumentParser(description='Run tests.')
parser.add_argument('--branch', '-b', dest='branch',
                    help='Branch to test. By default gets current one.' )
parser.add_argument('--sha', '-s', dest='sha',
                    help='Sha to test. By default gets current one.')
parser.add_argument('--test_file', '-t', dest='test_file', default='tests_for_nayduck.txt',
                    help='Test file with list of tests. By default nayduck.txt')
parser.add_argument('--run_type', '-r', dest='run_type', default='custom',
                    help='The type of the run. When triggered by user, the type is custom.')

args = parser.parse_args()

def get_curent_sha():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'], universal_newlines=True)

def get_current_branch():
    return subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD'], universal_newlines=True)

def get_current_user():
    return subprocess.check_output(['git', 'config', 'user.name'], universal_newlines=True)

def get_tests(fl):
    tests = []
    with open(fl) as f:
        for x in f.readlines():
            x = x.strip()
            if len(x) and x[0] != '#':
                if len(x) > 2:
                    if x[0] == '.' and x[1] == '/':
                        tests.extend(get_tests(x))
                    else:
                        tests.append(x)
                else:
                    tests.append(x)
    return tests

def github_auth():
    print("Go to the following link in your browser:")
    print()
    print("http://nayduck.eastus.cloudapp.azure.com:3000/local_auth")
    print()
    code = input("Enter verification code: ")
    with open(os.path.expanduser('~/.nayduck'), 'w') as f:
        f.write(code)
    return code


if __name__ == "__main__":
    if os.path.isfile(os.path.expanduser('~/.nayduck')):
        with open(os.path.expanduser('~/.nayduck'), 'r') as f:
            token = f.read()
    else:
        token = github_auth()
    if not args.branch:
        branch = get_current_branch().strip()
    else:
        branch = args.branch
    if not args.sha:
        sha = get_curent_sha().strip()
    else:
        sha = args.sha
    tests = get_tests(args.test_file)
    user = get_current_user().strip()
    post = {'branch': branch, 'sha': sha, 'tests': tests, 'requester': user, 'run_type': args.run_type, 'token': token.strip()}
    print('Sending request ...')
    res = requests.post('http://nayduck.eastus.cloudapp.azure.com:5000/request_a_run', json=post)
    json_res = json.loads(res.text)
    if json_res['code'] == 0:
        print(Fore.GREEN + json_res['response'])
    else:
        print(Fore.RED + json_res['response'])
