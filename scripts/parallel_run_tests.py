#!/usr/bin/env python3

import glob
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count

current_path = os.path.dirname(os.path.abspath(__file__))
target_debug = os.path.abspath(os.path.join(current_path, '../target/debug'))

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
    fname = f.split('/')[-1]
    if os.path.isfile(f) and fname != 'near' and not f.endswith('.d') and not fname.startswith('test_regression'):
        binaries.append(f)


# Run a single test by copying to docker, save exitcode, stdout and stderr
def run_test(test_binary):
    binary_file = f'/tmp/{test_binary.split("/")[-1]}'
    p = subprocess.Popen(['docker', 'run', '-v', f'{test_binary}:{binary_file}', 'ailisp/near-test-runtime',
     'bash', '-c', f'chmod +x {binary_file} && RUST_BACKTRACE=1 {binary_file}'], 
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    stdout, stderr = p.communicate()
    return (p.returncode, stdout, stderr)


# Run tests in runners
cpu = cpu_count()
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
