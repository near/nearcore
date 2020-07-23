#!/usr/bin/env python3
import argparse
import json
import re
import sys

def read_costs(path):
    result = {}
    with open(path) as f:
        pattern1 = re.compile("\\s*\"*([\\w]+)\"*: ([\\d]+).*")
        pattern2 = re.compile("\\s*\"*([\\w]+)\"*:.*")
        prefix = ""
        for line in f:
            m = pattern1.search(line)
            if m != None:
                result[prefix + m.group(1)] = m.group(2)
            else:
                m = pattern2.search(line)
                if m != None:
                    prefix = m.group(1) + ": "
    return result

def rate(c2, c1):
    if c1 == 0:
        return "n/a"
    return '{:.2f}'.format(float(c2) / float(c1))

def process_props(file1, file2, safety1, safety2):
    costs1 = read_costs(file1)
    costs2 = read_costs(file2)

    for key in costs1:
        c1 = int(costs1[key]) * safety1
        c2 = int(costs2.get(key, "0")) * safety2
        print("{}: first={} second={} second/first={}".format(key, c1, c2, rate(c2, c1)))


def process_json(file1, file2):
    data1 = None
    with open(file1) as json_file:
        data1 = json.load(json_file)
    data2 = None
    with open(file2) as json_file:
        data2 = json.load(json_file)
    print(data1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compare two cost sets')
    parser.add_argument('files', nargs=2, help='Input files')
    parser.add_argument('--safety_first',
                        default=1,
                        help='Safety multiplier applied to first')
    parser.add_argument('--safety_second',
                        default=1,
                        help='Safety multiplier applied to second')
    args = parser.parse_args()

    process_props(args.files[0], args.files[1], int(args.safety_first), int(args.safety_second))
