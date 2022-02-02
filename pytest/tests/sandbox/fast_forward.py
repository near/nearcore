#!/usr/bin/env python3
# Fast forward sandbox

import sys, time
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster
from configured_logger import logger


def figure_out_binary():
    config = {
        'local': True,
        'release': False,
    }
    repo_dir = pathlib.Path(__file__).resolve().parents[3]
    # When run on NayDuck we end up with a binary called neard in target/debug
    # but when run locally the binary might be neard-sandbox or near-sandbox
    # instead.  Try to figure out whichever binary is available and use that.
    for release in ('release', 'debug'):
        root = repo_dir / 'target' / release
        for exe in ('neard-sandbox', 'near-sandbox', 'neard'):
            if (root / exe).exists():
                logger.info(
                    f'Using {(root / exe).relative_to(repo_dir)} binary')
                config['near_root'] = str(root)
                config['binary_name'] = exe
                return config

    assert False, ('Unable to figure out location of neard-sandbox binary; '
                   'Did you forget to run `make sandbox`?')


# startup a RPC node
BLOCKS_TO_FASTFORARD = 10000
CONFIG = figure_out_binary()
nodes = start_cluster(1, 0, 1, CONFIG, [["epoch_length", 10]], {})

# request to fast forward
nodes[0].json_rpc('sandbox_fast_forward', {
    "delta_height": BLOCKS_TO_FASTFORARD,
})

# wait a little for it to fast forward
time.sleep(3)

# Assert at the end that the node is past the amounts of blocks we specified
assert nodes[0].get_latest_block().height > BLOCKS_TO_FASTFORARD
