# This file is uploaded to each mocknet node and run there.
# It is responsibe for updating the genesis to the requirements of the test, such as changing the chain id and adding test-only accounts to the genesis state..

import base58
import json
import random
import requests
import string
import sys
import time
import os
from rc import pmap

sys.path.append('lib')
import mocknet
from configured_logger import logger

if __name__ == '__main__':
    logger.info(sys.argv)
    genesis_filename_in = sys.argv[1]
    genesis_filename_out = sys.argv[2]
    chain_id = sys.argv[3]
    validator_node_names = sys.argv[4].split(',')
    rpc_node_names = sys.argv[5].split(',')
    if rpc_node_names and len(rpc_node_names) == 1 and not rpc_node_names[0]:
        rpc_node_names = None
    done_filename = sys.argv[6]
    epoch_length = int(sys.argv[7])
    assert genesis_filename_in
    assert genesis_filename_out
    assert chain_id
    assert validator_node_names is not None and len(
        validator_node_names) > 0 and validator_node_names[0]
    assert done_filename
    assert epoch_length

    mocknet.create_genesis_file(validator_node_names,
                                genesis_filename_in,
                                genesis_filename_out,
                                rpc_node_names,
                                chain_id,
                                append=True,
                                epoch_length=epoch_length)

    with open(done_filename, 'w') as f:
        f.write('DONE')
