import os
import shutil
import sys


def main():
    cpus = len(os.sched_getaffinity(0))
    args = ('cargo', 'fuzz', 'run', '--jobs', str(cpus), 'runtime-fuzzer', '--',
           '-len_control=0' '-prefer_small=0', '-max_len=4000000')
    os.chdir('../test-utils/runtime-tester/fuzz')
    os.environ['RUSTC_BOOTSTRAP'] = '1'
    os.execvp('cargo', args)


if __name__ == "__main__":
    main()
