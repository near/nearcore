#!/usr/bin/env python3
import os
from testlib import clean_binary_tests, build_tests, test_binaries, workers
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess

def coverage(test_binary):
    """ Run a single test coverage by copying to docker, save exitcode, stdout and stderr """
    src_dir = os.path.abspath('.')
    coverage_output = f'target/cov/{test_binary}'
    subprocess.check_output(f'mkdir -p {coverage_output}', shell=True)
    coverage_output = os.path.abspath(coverage_output)

    p = subprocess.Popen(['docker', 'run',
    '-u', f'{os.getuid()}:{os.getgid()}',
    '-v', f'{test_binary}:{test_binary}',
    '-v', f'{src_dir}:{src_dir}',
    '-v', f'{coverage_output}:{coverage_output}',
    'ailisp/near-coverage-runtime',
    'bash', '-c', f'chmod +x {test_binary} && /usr/local/bin/kcov --include-pattern=nearcore --exclude-pattern=.so -verify {coverage_output} {test_binary}'], 
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = p.communicate()
    return (p.returncode, stdout, stderr)


def clean_coverage():
    subprocess.check_output('rm -rf target/cov', shell=True)


if __name__ == "__main__":
    clean_coverage()
    clean_binary_tests()
    build_tests()
    binaries = test_binaries(exclude=[
        r'test_regression-*',
        r'near-*',
        r'test_cases_runtime-*',
        r'test_cases_testnet_rpc-*',
        r'test_catchup-*',
        r'test_errors-*',
        r'test_rejoin-*',
        r'test_simple-*',
        r'test_tps_regression-*',
    ])

    with ThreadPoolExecutor(max_workers=workers()) as executor:
        future_to_binary = {executor.submit(coverage, binary): binary for binary in binaries}
        for future in as_completed(future_to_binary):
            binary = future_to_binary[future]
            result = future.result()
            if result[0] != 0:
                print(result[2])
                print(f'========= kcov {binary} exit code {result[0]} cause coverage fail')
                os._exit(result[0])
            print(f'========= kcov {binary} done')
