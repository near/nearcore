#!/usr/bin/env python3
"""
Creates a genesis file from a template.
This file is uploaded to each mocknet node and run on the node, producing identical genesis files across all nodes.
This approach is significantly faster than the alternative, of uploading the genesis file to all mocknet nodes.
Currently testnet state is a 17GB json file, and uploading that file to 100 machines over a 1Gbit/s connection would
need at 4 hours.
"""

import os
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
    assert len(argv) == 17

    genesis_filename_in = argv[1]
    records_filename_in = argv[2]
    config_filename_in = argv[3]
    out_dir = argv[4]

    chain_id = argv[5]
    validator_keys = None
    if argv[6]:
        validator_keys = dict(map(lambda x: x.split('='), argv[6].split(',')))
    rpc_node_names = None
    if argv[7]:
        rpc_node_names = argv[7].split(',')
    done_filename = argv[8]
    epoch_length = int(argv[9])
    node_pks = None
    if argv[10]:
        node_pks = argv[10].split(',')
    increasing_stakes = float(argv[11])
    num_seats = float(argv[12])
    single_shard = str_to_bool(argv[13])
    all_node_pks = None
    if argv[14]:
        all_node_pks = argv[14].split(',')
    node_ips = None
    if argv[15]:
        node_ips = argv[15].split(',')
    if argv[16].lower() == 'none':
        neard = None
    else:
        neard = argv[16]

    assert genesis_filename_in
    assert records_filename_in
    assert config_filename_in
    assert out_dir
    assert chain_id
    assert validator_keys
    assert done_filename
    assert epoch_length
    assert node_pks
    assert rpc_node_names
    assert num_seats
    assert all_node_pks
    assert node_ips

    mocknet.neard_amend_genesis(
        neard=neard,
        validator_keys=validator_keys,
        genesis_filename_in=genesis_filename_in,
        records_filename_in=records_filename_in,
        out_dir=out_dir,
        rpc_node_names=rpc_node_names,
        chain_id=chain_id,
        epoch_length=epoch_length,
        node_pks=node_pks,
        increasing_stakes=increasing_stakes,
        num_seats=num_seats,
        single_shard=single_shard,
    )
    config_filename_out = os.path.join(out_dir, 'config.json')
    mocknet.update_config_file(
        config_filename_in,
        config_filename_out,
        all_node_pks,
        node_ips,
    )

    logger.info(f'done_filename: {done_filename}')
    pathlib.Path(done_filename).write_text('DONE')


if __name__ == '__main__':
    main(sys.argv)
