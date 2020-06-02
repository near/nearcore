#!/usr/bin/env python3

import glob
import re
import sys
import os

expensive_pattern = '#[cfg(feature = "expensive_tests")]'
test_pattern = '#[test]'


def find_first(str1, candidates, start):
    found = list(filter(lambda x: x[0]>=0, 
                          map(lambda str2: (str1.find(str2, start), str2),
                          candidates)))
    if found:
        return min(found)
    else:
        return -1, None
    

def find_fn(str1, start):
    fn_start = str1.find('fn ', start)
    match = re.search(r'[a-zA-Z0-9_]+', str1[fn_start+3:])
    if match:
        return match.group(0)
    return None


def expensive_tests_in_file(file):
    with(open(file)) as f:
        content = f.read()
    start = 0
    ret = []
    while True:
        start = content.find(expensive_pattern, start)
        if start == -1:
            return ret
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
                    ret.append(fn)
    return ret


def nightly_tests():
    with open(os.path.join(os.path.dirname(__file__), '../nightly/nightly.txt')) as f:
        tests = f.readlines()
    ret = set()
    for test in tests:
        t = test.strip().split(' ')
        if t[0] == 'expensive' or (t[0] == '#' and t[1] == 'expensive'):
            # It's okay to comment out a very expensive test intentionally
            ret.add(t[-1].split('::')[-1])
    return ret

if __name__ == '__main__':
    nightly_txt_tests = nightly_tests()
    rust_src = glob.glob(os.path.join(os.path.dirname(__file__), '../') + '**/*.rs', recursive=True)
    rust_src = list(filter(lambda f: f.find('/target/') == -1, rust_src))
    for rs in rust_src:
        rs = os.path.abspath(rs)
        print(f'checking file {rs}')
        expensive_tests = expensive_tests_in_file(rs)
        for t in expensive_tests:
            print(f'  expensive test {t}')
            if t not in nightly_txt_tests:
                print(f'error: file {rs} test {t} not in nightly.txt')
                exit(1)
    print('all tests in nightly')