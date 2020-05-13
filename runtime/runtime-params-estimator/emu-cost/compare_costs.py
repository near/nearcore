#!/usr/bin/env python3
import re
import sys

def read_costs(path):
    result = {}
    with open(path) as f:
        pattern = re.compile("\\s*\"*([\\w]+)\"*: ([\\d]+).*")
        for line in f:
            m = pattern.search(line)
            if m != None:
                result[m.group(1)] = m.group(2)
    return result

def rate(c2, c1):
    if c1 == 0:
        return "n/a"
    return '{:.2f}'.format(float(c2) / float(c1))

if __name__ == "__main__":
    costs1 = read_costs(sys.argv[1])
    costs2 = read_costs(sys.argv[2])

    for key in costs1:
        c1 = int(costs1[key])
        c2 = int(costs2[key])
        print("{}: time={} insn={} insn/time={}".format(key, c1, c2, rate(c2, c1)))
