#!/usr/bin/env python3
import argparse
import os

from testlib import clean_binary_tests, build_tests, test_binaries, workers, run_test, run_doc_tests
from concurrent.futures import as_completed, ThreadPoolExecutor

RERUN_THRESHOLD = 5


def show_test_result(binary, result):
    print(f'========= test binary {binary}')
    print(f'========= stdout of {binary}:')
    print(result[1])
    print(f'========= stderr of {binary}:')
    print(result[2])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--nightly', action='store_const', const=True)
    args = parser.parse_args()

    print("Running the tests with nightly build flags " +
          ("enabled!" if args.nightly else "disabled!"))

    clean_binary_tests()
    run_doc_tests(args.nightly)
    build_tests(args.nightly)
    binaries = test_binaries(
        exclude=[r'test_regression-.*', r'near_rpc_error_macro-.*'])
    print(f'========= collected {len(binaries)} test binaries:')
    print('\n'.join(binaries))

    completed = 0
    fails = []
    with ThreadPoolExecutor(max_workers=workers()) as executor:
        future_to_binary = {
            executor.submit(run_test, binary): binary for binary in binaries
        }
        for future in as_completed(future_to_binary):
            completed += 1
            binary_full_name = future_to_binary[future]
            binary = os.path.basename(binary_full_name)
            result = future.result()
            if result[0] != 0:
                fails.append((binary_full_name, result))
            else:
                show_test_result(binary, result)

    print(f"========= finished run {completed} test binaries")
    if fails:
        if len(fails) <= RERUN_THRESHOLD:
            # if not fail a lot, retry run test sequentially to avoid potential timeout
            new_fails = []
            for f in fails:
                binary_full_name = f[0]
                result = f[1]
                binary = os.path.basename(binary_full_name)
                print(
                    f'========= test binary {binary} run in parallel failed, exit code {result[0]}, retry run equentially ...'
                )
                result = run_test(binary_full_name, isolate=False)
                if result[0] != 0:
                    new_fails.append((binary_full_name, result))
                else:
                    show_test_result(binary, result)
            if new_fails:
                new_fail_summary = []
                for f in new_fails:
                    binary_full_name = f[0]
                    result = f[1]
                    binary = os.path.basename(binary_full_name)
                    show_test_result(binary, result)
                    new_fail_summary.append(
                        f'========= test binary {binary} run sequentially failed, exit code {result[0]}'
                    )
                for s in new_fail_summary:
                    print(s)
                exit(1)
            else:
                print("========= all tests passed")
        else:
            # if fail more than threshold
            for f in fails:
                binary_full_name = f[0]
                result = f[1]
                binary = os.path.basename(binary_full_name)
                show_test_result(binary, result)
                print(
                    f'========= test binary {binary} failed, exit code {result[0]}'
                )
            exit(1)
    else:
        print("========= all tests passed")
