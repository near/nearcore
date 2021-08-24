import subprocess
import os

def run_fuzz():
    env = os.environ.copy()
    env["RUSTC_BOOTSTRAP"] = "1"
    subprocess.check_call(['cargo', 'fuzz', 'run', 'runtime-fuzzer',
                           '--', '-len_control=0' '-prefer_small=0'],
                           env=env, cwd='../test-utils/runtime-tester/fuzz')

def main():
    run_fuzz()

if __name__ == "__main__":
    main()