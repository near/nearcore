# Creates a genesis file from a template.
# This file is uploaded to each mocknet node and run on the node, producing identical genesis files across all nodes.
# This approach is significantly faster than the alternative, of uploading the genesis file to all mocknet nodes. Currently testnet state is a 17GB json file, and uploading that file to 100 machines over a 1Gbit/s connection would need 4 hours.

import pathlib
import sys

sys.path.append('lib')
import mocknet
from configured_logger import logger

if __name__ == '__main__':
    logger.info(sys.argv)
    genesis_filename_in = sys.argv[1]
    genesis_filename_out = sys.argv[2]
    chain_id = sys.argv[3]
    validator_node_names = None
    if sys.argv[4]:
        validator_node_names = sys.argv[4].split(',')
    rpc_node_names = None
    if sys.argv[5]:
        rpc_node_names = sys.argv[5].split(',')
    done_filename = sys.argv[6]
    epoch_length = int(sys.argv[7])

    assert genesis_filename_in
    assert genesis_filename_out
    assert chain_id
    assert validator_node_names
    assert done_filename
    assert epoch_length

    mocknet.create_genesis_file(validator_node_names,
                                genesis_filename_in,
                                genesis_filename_out,
                                rpc_node_names,
                                chain_id,
                                append=True,
                                epoch_length=epoch_length)

    pathlib.Path(done_filename).write_text('DONE')
