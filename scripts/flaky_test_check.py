#!/usr/bin/env python3

import os
from testlib import clean_binary_tests, build_tests, test_binaries, run_test

TIMES = 10

if __name__ == "__main__":
    clean_binary_tests()
    build_tests()
    binaries = test_binaries(exclude=[r'test_regression-.*'])
    print(f'========= collected {len(binaries)} test binaries:')
    print('\n'.join(binaries))

    print(f"Run all tests sequentially for {TIMES} times: ")
    test_with_fails = {}
    for binary in binaries:
        fails = []
        for i in range(TIMES):
            exitcode, stdout, stderr = run_test(binary, isolate=False)
            print(f'Run {binary} {i+1} of {TIMES}, exit code {exitcode}')
            if exitcode != 0:
                fails.append(exitcode)
        if fails:
            test_with_fails[binary] = fails

    if test_with_fails:
        print("Some tests failed: ")
        for t, f in test_with_fails.items():
            print(f'{t} failed {len(f)} times')
