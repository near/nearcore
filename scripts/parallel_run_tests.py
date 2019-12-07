#!/usr/bin/env python3
import os
from testlib import clean_binary_tests, build_tests, test_binaries, workers
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess


def run_test(test_binary):
    """ Run a single test by copying to docker, save exitcode, stdout and stderr """
    p = subprocess.Popen(['docker', 'run',
    '-v', f'{test_binary}:{test_binary}', 
    'ailisp/near-test-runtime',
    'bash', '-c', f'chmod +x {test_binary} && RUST_BACKTRACE=1 {test_binary}'], 
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = p.communicate()
    return (p.returncode, stdout, stderr)


if __name__ == "__main__":
    clean_binary_tests()
    build_tests()
    binaries = test_binaries(exclude=[r'test_regression-*'])

    with ThreadPoolExecutor(max_workers=workers()) as executor:
        future_to_binary = {executor.submit(run_test, binary): binary for binary in binaries}
        for future in as_completed(future_to_binary):
            binary = future_to_binary[future]
            result = future.result()
            print(f'========= test {binary}')
            print('========= stdout:')
            print(result[1])
            print('========= stderr:')
            print(result[2])
            if result[0] != 0:
                print(f'========= test {binary} exit code {result[0]} cause build fail')
                os._exit(result[0])
