#!/usr/bin/env python3
"""NayDuck entry point for the offline mirror test.

The test lives in pytest/tools/mirror/offline_test.py, which is outside
pytest/tests and therefore not directly runnable by NayDuck. This wrapper
builds the addkey contract the test deploys, runs the test as a subprocess,
and exits with the test's exit code.
"""
import os
import pathlib
import subprocess
import sys
import time

CONTRACT_BUILD_TIMEOUT = 8 * 60
PYTEST_DIR = pathlib.Path(__file__).resolve().parents[2]
TEST_SCRIPT = PYTEST_DIR / 'tools' / 'mirror' / 'offline_test.py'
CONTRACT_DIR = PYTEST_DIR / 'tools' / 'mirror' / 'contract'


def kill_stale_neard():
    """Kills neard processes left over from a previous test on this worker.

    The test framework starts nodes in their own sessions
    (pytest/lib/cluster.py), so they survive the process-group kill of a
    timed-out run; the orphans keep the test's network ports bound and every
    later run on the worker fails with AddrInUse.
    """
    result = subprocess.run(['pgrep', '-x', 'neard'],
                            capture_output=True,
                            text=True)
    pids = result.stdout.split()
    if not pids:
        return
    print(f'killing stale neard processes: {pids}')
    subprocess.run(['pkill', '-9', '-x', 'neard'])
    time.sleep(1)


def build_contract():
    """Builds the addkey contract wasm the test deploys.

    The contract is a standalone crate that the neard build does not produce.
    Clear the inherited rustflags: CI sets `-fuse-ld=lld` for the native
    build, which rust-lld rejects when linking the wasm target.
    """
    env = dict(os.environ)
    env['RUSTFLAGS'] = ''
    env.pop('CARGO_ENCODED_RUSTFLAGS', None)
    subprocess.run(
        ['cargo', 'build', '--target', 'wasm32-unknown-unknown', '--release'],
        cwd=CONTRACT_DIR,
        env=env,
        check=True,
        timeout=CONTRACT_BUILD_TIMEOUT)


def main():
    kill_stale_neard()
    build_contract()
    result = subprocess.run([sys.executable, '-u', str(TEST_SCRIPT)])
    sys.exit(result.returncode)


if __name__ == '__main__':
    main()
