#!/usr/bin/env python3

import os
import sys
from pathlib import Path

from testlib import clean_binary_tests, build_tests, test_binaries, run_test
from concurrent.futures import as_completed, ThreadPoolExecutor

sys.path.append(str(Path(os.path.abspath(__file__)).parent.parent / 'pytest/lib'))
from configured_logger import logger


TIMES = 10

if __name__ == "__main__":
    clean_binary_tests()
    build_tests()
    binaries = test_binaries(exclude=[r'test_regression-.*'])
    logger.info(f'========= collected {len(binaries)} test binaries:')
    logger.info('\n'.join(binaries))

    logger.info(f"Run all tests sequentially for {TIMES} times: ")
    test_with_fails = {}
    for binary in binaries:
        fails = []
        for i in range(TIMES):
            exitcode, stdout, stderr = run_test(binary, isolate=False)
            logger.info(f'Run {binary} {i+1} of {TIMES}, exit code {exitcode}')
            if exitcode != 0:
                fails.append(exitcode)
        if fails:
            test_with_fails[binary] = fails

    if test_with_fails:
        logger.info("Some tests failed: ")
        for t, f in test_with_fails.items():
            logger.info(f'{t} failed {len(f)} times')
