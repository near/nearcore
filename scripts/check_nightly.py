#!/usr/bin/env python3

import glob
import pathlib
import re
import sys

import nayduck

expensive_pattern = '#[cfg(feature = "expensive_tests")]'
test_pattern = '#[test]'


def find_first(data, tokens, start):
    found = [(data.find(token, start), token) for token in tokens]
    try:
        return min((pos, token) for pos, token in found if pos >= 0)
    except ValueError:
        return -1, None


def find_fn(str1, start):
    fn_start = str1.find('fn ', start)
    match = re.search(r'[a-zA-Z0-9_]+', str1[fn_start + 3:])
    if match:
        return match.group(0)
    return None


def expensive_tests_in_file(file):
    with (open(file)) as f:
        content = f.read()
    start = 0
    while True:
        start = content.find(expensive_pattern, start)
        if start == -1:
            return
        start += len(expensive_pattern)
        level = 0
        while True:
            start, tok = find_first(content, ['{', '}', test_pattern], start)
            if start == -1:
                break
            start += len(tok)
            if tok == '{':
                level += 1
            elif tok == '}':
                level -= 1
                if level == 0:
                    break
            elif tok == test_pattern:
                fn = find_fn(content, start)
                if fn:
                    yield fn


def nightly_tests(repo_dir):
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


def main():
    repo_dir = pathlib.Path(__file__).parent.parent
    nightly_txt_tests = set(nightly_tests(repo_dir))
    for rs in glob.glob(str(repo_dir / '**/*.rs'), recursive=True):
        if '/target/' in rs:
            continue
        print(f'checking file {rs}')
        for t in expensive_tests_in_file(rs):
            print(f'  expensive test {t}')
            if t not in nightly_txt_tests:
                return f'error: file {rs} test {t} not in nightly.txt'
    print('all tests in nightly')
    return None


if __name__ == '__main__':
    sys.exit(main())
