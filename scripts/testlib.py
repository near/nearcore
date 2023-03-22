#!/usr/bin/env python3

import glob
import os
import subprocess
import fcntl
import re
import filecmp

fcntl.fcntl(1, fcntl.F_SETFL, 0)

current_path = os.path.dirname(os.path.abspath(__file__))
target_debug = os.path.abspath(os.path.join(current_path, '../target/debug'))


def clean_binary_tests():
    if os.environ.get('RFCI_COMMIT'):
        return
    for f in glob.glob(f'{target_debug}/*'):
        if os.path.isfile(f):
            os.remove(f)


def build_tests(nightly=False):
    command = ['cargo', 'test', '--workspace', '--no-run']
    if nightly:
        command += ['--features', 'nightly']
    print("Building tests using command: ", ' '.join(command))
    p = subprocess.run(command)
    if p.returncode != 0:
        os._exit(p.returncode)


def run_doc_tests(nightly=False):
    command = ['cargo', 'test', '--workspace', '--doc']
    if nightly:
        command += ['--features', 'nightly']
    print("Building doc tests using command: ", ' '.join(command))
    p = subprocess.run(command)
    if p.returncode != 0:
        os._exit(p.returncode)


def test_binaries(exclude=None):
    binaries = []
    for f in glob.glob(f'{target_debug}/deps/*'):
        fname = os.path.basename(f)
        ext = os.path.splitext(fname)[1]
        is_near_binary = filecmp.cmp(f, f'{target_debug}/near') or filecmp.cmp(
            f, f'{target_debug}/neard')
        if os.path.isfile(f) and not is_near_binary and ext == '':
            if not exclude:
                binaries.append(f)
            elif not any(map(lambda e: re.match(e, fname), exclude)):
                binaries.append(f)
            else:
                print(f'========= ignore {f}')
    return binaries


def run_test(test_binary, isolate=True):
    """ Run a single test, save exitcode, stdout and stderr """
    if isolate:
        cmd = [
            'docker', 'run', '--rm', '-u', f'{os.getuid()}:{os.getgid()}', '-v',
            f'{test_binary}:{test_binary}', 'nearprotocol/near-test-runtime',
            'bash', '-c', f'RUST_BACKTRACE=1 {test_binary}'
        ]
    else:
        cmd = [test_binary]
    print(f'========= run test {test_binary}')
    if os.path.isfile(test_binary):
        p = subprocess.Popen(cmd,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE,
                             universal_newlines=True)
        stdout, stderr = p.communicate()
        return (p.returncode, stdout, stderr)
    return -1, '', f'{test_binary} does not exist'
