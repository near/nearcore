import subprocess
import os
import re
import sys

def run_fuzz():
    env = os.environ.copy()
    env["RUSTC_BOOTSTRAP"] = "1"
    fuzzing = subprocess.Popen(('cargo', 'fuzz', 'run', 'runtime-fuzzer',
                                '--', '-len_control=0' '-prefer_small=0', '-max_len=4000000'),
                               env=env, cwd='../test-utils/runtime-tester/fuzz',
                               stderr=subprocess.PIPE)
    out, err = fuzzing.communicate()
    print(err, file=sys.stderr)

    re_expr = re.compile(r'Output of `std::fmt::Debug`:((.|\n)*)Reproduce', re.MULTILINE)
    res = re_expr.search(err.decode('utf-8'))

    if res:
        print(res.group(1))

    if fuzzing.returncode != 0:
        raise Exception( f'Invalid result: { fuzzing.returncode }' )


def main():
    run_fuzz()

if __name__ == "__main__":
    main()
