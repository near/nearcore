#!/usr/bin/env python3
# Spins up one validating node, one dumping node and one node to monitor the state parts dumped.
# Start all nodes.
# Before the end of epoch stop the dumping node.
# In the next epoch check the number of state sync files reported by the monitor. Should be 0
# Start the dumping node.
# In the following epoch check the number of state sync files reported.
# Should be non 0

import pathlib
import sys
import re
from itertools import islice, takewhile

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import key
from utils import poll_blocks, poll_epochs
from cluster import BaseNode, init_cluster, spin_up_node, load_config
import transaction
import state_sync_lib
from configured_logger import logger

EPOCH_LENGTH = 50
NUM_SHARDS = 4


def get_dump_check_metrics(target):
    metrics = target.get_metrics()
    metrics = [
        line.split(' ')
        for line in str(metrics).strip().split('\\n')
        if re.search('state_sync_dump_check.*{', line)
    ]
    metrics = {metric: int(val) for metric, val in metrics}
    return metrics


def create_account(node: BaseNode, account_id: str, nonce):
    """ Create an account with full access key and balance. """
    block_hash = node.get_latest_block().hash_bytes
    account = key.Key.from_random(account_id)
    balance = 10**24
    account_tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(
        node.signer_key,
        account.account_id,
        account,
        balance,
        nonce,
        block_hash,
    )
    response = node.send_tx_and_wait(account_tx, timeout=20)
    assert 'error' not in response, response
    assert 'Failure' not in response['result']['status'], response
    return account


def main():
    node_config_dump, node_config = state_sync_lib.get_state_sync_configs_pair()
    config = load_config()
    genesis_config_changes = [["epoch_length", EPOCH_LENGTH]]
    client_config_changes = {
        0: node_config,
        1: node_config_dump,
        2: node_config
    }
    near_root, node_dirs = init_cluster(
        1,
        2,
        4,
        config,
        genesis_config_changes,
        client_config_changes,
    )

    boot_node = spin_up_node(config, near_root, node_dirs[0], 0)
    logger.info('Started boot_node')
    dump_node = spin_up_node(config,
                             near_root,
                             node_dirs[1],
                             1,
                             boot_node=boot_node)
    dump_check = spin_up_node(config,
                              near_root,
                              node_dirs[2],
                              2,
                              boot_node=boot_node)

    # create account in each shard so that the state is not empty
    create_account(boot_node, "test0.test0", 1)
    create_account(boot_node, "test1.test0", 2)
    create_account(boot_node, "test2.test0", 3)
    create_account(boot_node, "test3.test0", 4)

    dump_check.kill()
    chain_id = boot_node.get_status()['chain_id']
    dump_folder = node_config_dump["state_sync"]["dump"]["location"][
        "Filesystem"]["root_dir"]
    rpc_address, rpc_port = boot_node.rpc_addr()
    local_rpc_address, local_rpc_port = dump_check.rpc_addr()
    cmd = dump_check.get_command_for_subprogram(
        ('state-parts-dump-check', '--chain-id', chain_id, '--root-dir',
         dump_folder, 'loop-check', '--rpc-server-addr',
         f"http://{rpc_address}:{rpc_port}", '--prometheus-addr',
         f"{local_rpc_address}:{local_rpc_port}", '--interval', '2'))
    dump_check.run_cmd(cmd=cmd)

    logger.info('Started nodes')

    # Close to the end of the epoch
    list(
        takewhile(lambda b: b.height < EPOCH_LENGTH - 5,
                  poll_blocks(boot_node)))
    # kill dumper node so that it can't dump state.
    node_height = dump_node.get_latest_block().height
    logger.info(f'Dump_node is @{node_height}')
    dump_node.kill()
    logger.info(f'Killed dump_node')

    # wait until next epoch starts
    list(
        takewhile(lambda b: b.height < EPOCH_LENGTH + 5,
                  poll_blocks(boot_node)))
    # Check the dumped stats
    metrics = get_dump_check_metrics(dump_check)
    assert sum([
        val for metric, val in metrics.items()
        if 'state_sync_dump_check_process_is_up' in metric
    ]) == NUM_SHARDS, f"Dumper process missing for some shards. {metrics}"

    # wait for 10 more blocks.
    list(islice(poll_blocks(boot_node), 10))
    # Start state dumper node to keep up with the network and dump state next
    # epoch.
    logger.info(f'Starting dump_node')
    dump_node.start(boot_node=boot_node)

    # wait for the next epoch
    list(
        takewhile(lambda height: height < 2,
                  poll_epochs(boot_node, epoch_length=EPOCH_LENGTH)))
    list(islice(poll_blocks(boot_node), 10))
    # State should have been dumped and reported as dumped.
    metrics = get_dump_check_metrics(dump_check)
    assert sum([
        val for metric, val in metrics.items()
        if 'state_sync_dump_check_num_parts_dumped' in metric
    ]) >= NUM_SHARDS, f"Some parts are missing. {metrics}"
    assert sum([
        val for metric, val in metrics.items()
        if 'state_sync_dump_check_num_header_dumped' in metric
    ]) == NUM_SHARDS, f"Some headers are missing. {metrics}"


if __name__ == "__main__":
    main()
