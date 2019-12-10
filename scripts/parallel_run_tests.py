#!/usr/bin/env python3
import os
from testlib import clean_binary_tests, build_tests, test_binaries, workers
from concurrent.futures import as_completed, ThreadPoolExecutor
import subprocess


def run_test(test_binary):
    """ Run a single test by copying to docker, save exitcode, stdout and stderr """
    cmd = ['docker', 'run',
    '-v', f'{test_binary}:{test_binary}', 
    'ailisp/near-test-runtime',
    'bash', '-c', f'chmod +x {test_binary} && RUST_BACKTRACE=1 {test_binary}']
    p = subprocess.Popen(cmd, 
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = p.communicate()
    return (p.returncode, stdout, stderr)


if __name__ == "__main__":
    clean_binary_tests()
    build_tests()
    binaries = test_binaries(exclude=[r'test_regression-*'])
    print(f'========= collected {len(binaries)} test binaries:')
    print('\n'.join(binaries))

    completed = 0
    fails = []
    with ThreadPoolExecutor(max_workers=workers()) as executor:
        future_to_binary = {executor.submit(run_test, binary): binary for binary in binaries}
        for future in as_completed(future_to_binary):
            completed += 1
            binary = os.path.basename(future_to_binary[future])
            result = future.result()
            print(f'========= test binary {binary}')
            print(f'========= stdout of {binary}:')
            print(result[1])
            print(f'========= stderr of {binary}:')
            print(result[2])
            if result[0] != 0:
                fails.append(f'========= test binary {binary} failed, exit code {result[0]}')

    print(f"========= finished run {completed} test binaries")
    if fails:
        for f in fails:
            print(f)
        exit(1)
    else:
        print("========= all tests passed")
