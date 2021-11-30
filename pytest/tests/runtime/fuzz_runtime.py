import os
import subprocess
import sys

sys.path.append('lib')

import cargo_fuzz

if __name__ == '__main__':
    sys.exit(cargo_fuzz.run('test-utils/runtime-tester/fuzz', 'runtime-fuzzer'))
