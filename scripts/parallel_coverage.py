#!/usr/bin/env python3
import os
from testlib import clean_binary_tests, build_tests, test_binaries, current_path
from concurrent.futures import ThreadPoolExecutor, as_completed
import subprocess
import glob
from itertools import zip_longest
from multiprocessing import cpu_count


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)


def coverage(test_binary):
    """ Run a single test coverage by copying to docker, save exitcode, stdout and stderr """
    src_dir = os.path.abspath('.')
    test_binary_basename = os.path.basename(test_binary)
    coverage_output = f'target/cov0/{test_binary_basename}'
    subprocess.check_output(f'mkdir -p {coverage_output}', shell=True)
    coverage_output = os.path.abspath(coverage_output)

    if not os.path.isfile(test_binary):
        return -1, '', f'{test_binary} does not exist'

    p = subprocess.Popen([
        'docker', 'run', '--rm', '--security-opt', 'seccomp=unconfined', '-u',
        f'{os.getuid()}:{os.getgid()}', '-v', f'{test_binary}:{test_binary}',
        '-v', f'{src_dir}:{src_dir}', '-v',
        f'{coverage_output}:{coverage_output}',
        'nearprotocol/near-coverage-runtime', 'bash', '-c',
        f'/usr/local/bin/kcov --include-pattern=nearcore --exclude-pattern=.so --verify {coverage_output} {test_binary}'
    ],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE,
                         universal_newlines=True)
    stdout, stderr = p.communicate()
    return (p.returncode, stdout, stderr)


def clean_coverage():
    subprocess.check_output(f'rm -rf {current_path}/../target/cov*', shell=True)
    subprocess.check_output(f'rm -rf {current_path}/../target/merged_coverage',
                            shell=True)


def coverage_dir(i):
    return f'{current_path}/../target/cov{i}'


def merge_coverage(i, to_merge, j):
    p = subprocess.Popen([
        'kcov', '--merge',
        os.path.join(coverage_dir(i + 1), str(j)), *to_merge
    ],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return (p.returncode, stdout, stderr)


if __name__ == "__main__":
    clean_coverage()
    clean_binary_tests()
    build_tests()
    binaries = test_binaries(exclude=[
        r'test_regression-.*',
        r'near-.*',
        r'test_cases_runtime-.*',
        r'test_cases_testnet_rpc-.*',
        r'test_catchup-.*',
        r'test_errors-.*',
        r'test_rejoin-.*',
        r'test_simple-.*',
        r'test_tps_regression-.*',
    ])
    errors = False

    # Run coverage
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        future_to_binary = {
            executor.submit(coverage, binary): binary for binary in binaries
        }
        for future in as_completed(future_to_binary):
            binary = future_to_binary[future]
            result = future.result()
            if result[0] != 0:
                print(result[2])
                errors = True
                print(
                    f'========= error: kcov {binary} fail, exit code {result[0]} cause coverage fail'
                )
            else:
                print(f'========= kcov {binary} done')

    # Merge coverage
    i = 0
    j = 0
    with ThreadPoolExecutor(max_workers=cpu_count()) as executor:
        while True:
            covs = glob.glob(f'{coverage_dir(i)}/*')
            if len(covs) == 1:
                break
            subprocess.check_output(f'mkdir -p {coverage_dir(i+1)}', shell=True)

            cov_to_merge = list(grouper(covs, 2))
            if cov_to_merge[-1][-1] is None:
                # ensure the last to merge is not only one cov
                cov_to_merge[-2] += (cov_to_merge[-1][0],)
                del cov_to_merge[-1]

            futures = []
            for cov in cov_to_merge:
                j += 1
                futures.append(executor.submit(merge_coverage, i, cov, j))

            for f in as_completed(futures):
                pass

            i += 1

    merged_coverage = os.path.join(coverage_dir(i), str(j))
    print(f'========= coverage merged to {merged_coverage}')
    subprocess.check_output(
        ['mv', merged_coverage, f'{current_path}/../merged_coverage'])
    subprocess.check_output(f'rm -rf {current_path}/../target/cov*', shell=True)

    if errors:
        print(
            f'========= some errors in running kcov, coverage maybe inaccurate')
