#!/usr/bin/env python3
import argparse
import json
import re
import sys
import json
from collections import OrderedDict


def is_json_int(v):
    try:
        int(v)
        return True
    except:
        return False


def flatten_dict(d, result, prefix=''):
    for k, v in d.items():
        full_key = prefix + '.' + k if prefix else k
        if is_json_int(v):
            result[full_key] = v
        elif isinstance(v, dict):
            flatten_dict(v, result, full_key)

    return result

def parse_debug_print(path):
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

def read_costs(path):
    result = OrderedDict()
    try:
        with open(path) as f:
            genesis_or_runtime_config = json.load(f, object_pairs_hook=OrderedDict)
    except (json.decoder.JSONDecodeError):
        # try to load Rust debug pretty print.
        genesis_or_runtime_config = parse_debug_print(path)
    if 'runtime_config' in genesis_or_runtime_config:
        runtime_config = genesis_or_runtime_config['runtime_config']
    else:
        runtime_config = genesis_or_runtime_config
    return flatten_dict(runtime_config, result)


def rate(c2, c1):
    if c1 == 0:
        return "n/a"
    return '{:.2f}'.format(float(c2) / float(c1))

EPSILON=0.2
def significant(c1, c2):
    if c1 == 0 or c2 == 0:
        return c1 != c2
    return abs((c1 / c2) - 1.0) > EPSILON or abs((c2 / c1) - 1.0) > EPSILON

def process_props(file1, file2, safety1, safety2, diff):
    costs1 = read_costs(file1)
    costs2 = read_costs(file2)

    for key in costs1:
        c1 = int(costs1[key]) * safety1
        c2 = int(costs2.get(key, "0")) * safety2
        if not diff or significant(c1, c2):
            print("{}: first={} second={} second/first={}".format(
                key, c1, c2, rate(c2, c1)))


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
    parser.add_argument('--diff', dest='diff', action='store_true')
    parser.set_defaults(diff=False)
    args = parser.parse_args()

    process_props(args.files[0], args.files[1], int(args.safety_first),
                  int(args.safety_second), args.diff)
