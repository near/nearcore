#!/usr/bin/python3

import os, sys, re, random
from subprocess import PIPE, Popen

utc_code     = re.escape('UtcProxy::now(file!(), line!())')
instant_code = re.escape('InstantProxy::now(file!(), line!())')
cmd = f"grep -RnHE '{utc_code}|{instant_code}' chain neard core"
top_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")
p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, cwd=top_dir)
stdout, stderr = p.communicate()
proxify_args = []
try:
    out = stdout.decode("utf-8")
    def filelocation(s):
        file, line = s.split(":")[:2]
        return ":".join([file, line, "unproxify"])

    proxify_args = list(map(filelocation, out.split("\n")[:-1]))
    print(proxify_args)
except:
    print("utf error!")
sys.exit()

num = 10
if len(sys.argv) > 1:
    num = int(sys.argv[1])
delay = 3.0
if len(sys.argv) > 2:
    delay = float(sys.argv[2])

def exclude(args):
    args2 = []
    incl = []
    i = 0
    for arg in args:
        if random.random() > 0.5:
            args2.append(arg)
            incl.append(i)
        i += 1
    return [incl, args2]

success_results = []

for i in range(1000):
    # print(f"SEARCH: exclude {i}")
    # args = [*proxify_args[:i], *proxify_args[i + 1:]]
    incl, args = exclude(proxify_args)
    args = " ".join([str(num), str(delay), *args]) #f"{num} {delay} {args}"
    print(args)

    cmd = f"python3 run_time_travel.py {args}"
    scripts_dir = os.path.dirname(os.path.realpath(__file__))
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, cwd=scripts_dir)
    stdout, stderr = p.communicate()
    try:
        out = stdout.decode("utf-8")
        err = stderr.decode("utf-8")
        # print(out)
        print(err, file=sys.stderr)
        success, failure = out.split("\n")[0].split(" ")
        # print(success, failure)
        success = int(success)
        failure = int(failure)
        if success == 5 and failure == 0:
            success_results.append(incl)
            occurences = {}
            for a in success_results:
                for b in a:
                    try:
                        occurences[b] += 1
                    except KeyError:
                        occurences[b] = 1
            occurences_ary = []
            try:
                occurences_ary = [0] * (max([x for x in occurences]) + 1)
            except ValueError:
                pass
            for key in occurences:
                occurences_ary[key] = occurences[key]
            print(["success", occurences_ary, incl])
        if success == 0 and failure == 5:
            print(["failure", incl])
        if success != 0 and success != 5:
            print("inconclusive")
    except UnicodeDecodeError:
        print("utf error!")
