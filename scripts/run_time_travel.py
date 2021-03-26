#!/usr/bin/python3
import os, sys
from subprocess import PIPE, Popen

success = 0
failure = 0
for i in range(int(sys.argv[1])):
    print(i, file=sys.stderr)
    cmd = f"python3 tests/adversarial/time_travel.py {' '.join(sys.argv[2:])}"
    pytest_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "pytest")
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, cwd=pytest_dir)
    stdout, stderr = p.communicate()
    try:
        out = stdout.decode("utf-8")
        err = stderr.decode("utf-8")
        if out.find("done in ") > -1:
            success += 1
        if err.find("timed out") > -1:
            failure += 1
        print(out, file=sys.stderr)
        print(err, file=sys.stderr)
    except UnicodeDecodeError:
        print("utf error!", file=sys.stderr)

print(success, failure)
