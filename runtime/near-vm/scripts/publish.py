#! /usr/bin/env python3

# This is a script for publishing the wasmer crates to crates.io.
# It should be run in the root of wasmer like `python3 scripts/publish.py --no-dry-run`.
# By default the script executes a test run and does not publish the crates to crates.io.

# install dependencies:
# pip3 install toposort

import argparse
import os
import re
import subprocess
import time

from typing import Optional
try:
    from toposort import toposort_flatten
except ImportError:
    print("Please install toposort, `pip3 install toposort`")


# TODO: find this automatically
target_version = "2.1.0"

# TODO: generate this by parsing toml files
dep_graph = {
    "wasmer-types": set([]),
    "wasmer-vm": set(["wasmer-types"]),
    "wasmer-compiler": set(["wasmer-vm", "wasmer-types"]),
    "wasmer-object": set(["wasmer-types", "wasmer-compiler"]),
    "wasmer-engine": set(["wasmer-types", "wasmer-vm", "wasmer-compiler"]),
    "wasmer-compiler-singlepass": set(["wasmer-types", "wasmer-vm", "wasmer-compiler"]),
    "wasmer-engine-universal": set(["wasmer-types", "wasmer-vm", "wasmer-compiler", "wasmer-engine"]),
    "wasmer": set(["wasmer-vm", "wasmer-compiler-singlepass",
                   "wasmer-compiler", "wasmer-engine", "wasmer-engine-universal",
                   "wasmer-types"]),
}

# where each crate is located in the `lib` directory
# TODO: this could also be generated from the toml files
location = {
    "wasmer-types": "types",
    "wasmer-vm": "vm",
    "wasmer-compiler": "compiler",
    "wasmer-object": "object",
    "wasmer-engine": "engine",
    "wasmer-compiler-singlepass": "compiler-singlepass",
    "wasmer-engine": "engine",
    "wasmer-engine-universal": "engine-universal",
    "wasmer": "api",
}

no_dry_run = False

def get_latest_version_for_crate(crate_name: str) -> Optional[str]:
    output = subprocess.run(["cargo", "search", crate_name], capture_output=True)
    rexp_src = '^{} = "([^"]+)"'.format(crate_name)
    prog = re.compile(rexp_src)
    haystack = output.stdout.decode("utf-8")
    for line in haystack.splitlines():
        result = prog.match(line)
        if result:
            return result.group(1)

def is_crate_already_published(crate_name: str) -> bool:
    found_string = get_latest_version_for_crate(crate_name)
    if found_string is None:
        return False

    return target_version == found_string

def publish_crate(crate: str):
    starting_dir = os.getcwd()
    os.chdir("lib/{}".format(location[crate]))

    global no_dry_run
    if no_dry_run:
        output = subprocess.run(["cargo", "publish"])
    else:
        print("In dry-run: not publishing crate `{}`".format(crate))

    os.chdir(starting_dir)

def main():
    os.environ['WASMER_PUBLISH_SCRIPT_IS_RUNNING'] = '1'
    parser = argparse.ArgumentParser(description='Publish the Wasmer crates to crates.io')
    parser.add_argument('--no-dry-run', default=False, action='store_true',
                        help='Run the script without actually publishing anything to crates.io')
    args = vars(parser.parse_args())

    global no_dry_run
    no_dry_run = args['no_dry_run']

    # get the order to publish the crates in
    order = list(toposort_flatten(dep_graph, sort=True))

    for crate in order:
        print("Publishing `{}`...".format(crate))
        if not is_crate_already_published(crate):
            publish_crate(crate)
        else:
            print("`{}` was already published!".format(crate))
            continue
        # sleep for 16 seconds between crates to ensure the crates.io index has time to update
        # this can be optimized with knowledge of our dep graph via toposort; we can even publish
        # crates in parallel; however this is out of scope for the first version of this script
        if no_dry_run:
            print("Sleeping for 16 seconds to allow the `crates.io` index to update...")
            time.sleep(16)
        else:
            print("In dry-run: not sleeping for crates.io to update.")


if __name__ == "__main__":
    main()
