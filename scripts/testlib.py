#!/usr/bin/env python3

import glob
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
import fcntl
import re


fcntl.fcntl(1, fcntl.F_SETFL, 0)


current_path = os.path.dirname(os.path.abspath(__file__))
target_debug = os.path.abspath(os.path.join(current_path, '../target/debug'))


def clean_binary_tests():
    if os.environ.get('RFCI_COMMIT'):
        return
    for f in glob.glob(f'{target_debug}/*'):
        if os.path.isfile(f):
            os.remove(f)


def build_tests():
    p = subprocess.run(['cargo', 'test', '--workspace', '--no-run'])
    if p.returncode != 0:
        os._exit(p.returncode)


def workers():
    if os.environ.get('RFCI_COMMIT'):
        workers = cpu_count() - 2
    else:
        workers = cpu_count() // 2
    print(f'========= run in {workers} workers')
    return workers


def test_binaries(exclude=None):
    binaries = []
    for f in glob.glob(f'{target_debug}/*'):
        fname = os.path.basename(f)
        ext = os.path.splitext(fname)[1]
        if os.path.isfile(f) and fname != 'near' and ext == '':
            if not exclude:
                binaries.append(f)
            elif not any(map(lambda e: re.match(e, fname), exclude)):
                binaries.append(f)
            else:
                print(f'========= ignore {f}')
    return binaries
