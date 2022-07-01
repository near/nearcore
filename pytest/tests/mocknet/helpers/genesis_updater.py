#!/usr/bin/env python3
"""
Creates a genesis file from a template.
This file is uploaded to each mocknet node and run on the node, producing identical genesis files across all nodes.
This approach is significantly faster than the alternative, of uploading the genesis file to all mocknet nodes.
Currently testnet state is a 17GB json file, and uploading that file to 100 machines over a 1Gbit/s connection would
need at 4 hours.
"""

import pathlib
import sys

# Don't use the pathlib magic because this file runs on a remote machine.
sys.path.append('lib')
import mocknet
from configured_logger import logger


def str_to_bool(arg):
    return arg.lower() == 'true'

def main(argv):
    logger.info(argv)
    assert len(argv) == 18

    genesis_filename_in = argv[1]
    genesis_filename_out = argv[2]
    records_filename_in = argv[3]
    records_filename_out = argv[4]
    config_filename_in = argv[5]
    config_filename_out = argv[6]

    chain_id = argv[7]
    validator_node_names = None
    if argv[8]:
        validator_node_names = argv[8].split(',')
    rpc_node_names = None
    if argv[9]:
        rpc_node_names = argv[9].split(',')
    done_filename = argv[10]
    epoch_length = int(argv[11])
    node_pks = None
    if argv[12]:
        node_pks = argv[12].split(',')
    increasing_stakes = float(argv[13])
    num_seats = float(argv[14])
    single_shard = str_to_bool(argv[15])
    all_node_pks = None
    if argv[16]:
        all_node_pks = argv[16].split(',')
    node_ips = None
    if argv[17]:
        node_ips = argv[17].split(',')

    assert genesis_filename_in
    assert genesis_filename_out
    assert records_filename_in
    assert records_filename_out
    assert config_filename_in
    assert config_filename_out
    assert chain_id
    assert validator_node_names
    assert done_filename
    assert epoch_length
    assert node_pks
    assert rpc_node_names
    assert num_seats
    assert all_node_pks
    assert node_ips

    mocknet.create_genesis_file(validator_node_names,
                                genesis_filename_in,
                                genesis_filename_out,
                                records_filename_in,
                                records_filename_out,
                                rpc_node_names=rpc_node_names,
                                chain_id=chain_id,
                                append=True,
                                epoch_length=epoch_length,
                                node_pks=node_pks,
                                increasing_stakes=increasing_stakes,
                                num_seats=num_seats,
                                single_shard=single_shard)
    mocknet.update_config_file(config_filename_in, config_filename_out,
                               all_node_pks, node_ips)

    logger.info(f'done_filename: {done_filename}')
    pathlib.Path(done_filename).write_text('DONE')


if __name__ == '__main__':
    main(sys.argv)
