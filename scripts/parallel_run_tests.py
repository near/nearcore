#!/usr/bin/env python3

import glob
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
import fcntl

fcntl.fcntl(1, fcntl.F_SETFL, 0)

current_path = os.path.dirname(os.path.abspath(__file__))
target_debug = os.path.abspath(os.path.join(current_path, '../target/debug'))
# test_wasm = os.path.abspath(os.path.join(current_path, '../runtime/near-vm-runner/tests/res/test_contract_rs.wasm'))

# Clean binary tests
for f in glob.glob(f'{target_debug}/*'):
    if os.path.isfile(f):
        os.remove(f)

# Build tests
p = subprocess.run(['cargo', 'test', '--workspace', '--no-run'])
if p.returncode != 0:
    os._exit(p.returncode)
binaries = []
for f in glob.glob(f'{target_debug}/*'):
    fname = os.path.basename(f)
    if os.path.isfile(f) and fname != 'near' and not f.endswith('.d') and not fname.startswith('test_regression'):
        binaries.append(f)


# Run a single test by copying to docker, save exitcode, stdout and stderr
def run_test(test_binary):
    p = subprocess.Popen(['docker', 'run',
    '-v', f'{test_binary}:{test_binary}', 
    # '-v', f'{test_wasm}:{test_wasm}',
    'ailisp/near-test-runtime',
    'bash', '-c', f'chmod +x {test_binary} && RUST_BACKTRACE=1 {test_binary}'], 
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    stdout, stderr = p.communicate()
    return (p.returncode, stdout, stderr)


# Run tests in runners
cpu = cpu_count()
if os.environ.get('GITLAB_CI'):
    cpu -= 1

with ThreadPoolExecutor(max_workers=cpu) as executor:
    future_to_binary = {executor.submit(run_test, binary): binary for binary in binaries}
    for future in as_completed(future_to_binary):
        binary = future_to_binary[future]
        result = future.result()
        print(f'========= test {binary} exit code {result[0]}')
        print('========= stdout:')
        print(result[1])
        print('========= stderr:')
        print(result[2])
        if result[0] != 0:
            os._exit(result[0])
