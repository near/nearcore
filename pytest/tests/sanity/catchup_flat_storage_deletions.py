#!/usr/bin/env python3
# Spins up a validator node tracking all shards and a non-validator node.
# Deletes an account.
# Restart the non-validator node.
# After the non-validator node does catchup, attempt to send a token to the deleted account.
# Observe that both nodes correctly execute the transaction and return an error.

import pathlib
import sys
import tempfile

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, load_config
import transaction
import utils

from configured_logger import logger

EPOCH_LENGTH = 10


def epoch_height(block_height):
    if block_height == 0:
        return 0
    if block_height <= EPOCH_LENGTH:
        # According to the protocol specifications, there are two epochs with height 1.
        return "1*"
    return int((block_height - 1) / EPOCH_LENGTH)


def print_balances(nodes, account_ids):
    for node in nodes:
        for account_id in account_ids:
            res = node.json_rpc(
                'query', {
                    'request_type': 'view_account',
                    'account_id': account_id,
                    'finality': 'optimistic'
                })
            logger.info(
                f"Lookup account '{account_id}' in node '{node.signer_key.account_id}': {res['result'] if 'result' in res else res['error']}"
            )


def main():
    state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / 'state_parts')

    config0 = {
        'state_sync': {
            'dump': {
                'location': {
                    'Filesystem': {
                        'root_dir': state_parts_dir
                    }
                },
                'iteration_delay': {
                    'secs': 0,
                    'nanos': 100000000
                },
            }
        },
        'store.state_snapshot_enabled': True,
        'tracked_shards': [0]  # Track all shards.
    }
    config1 = {
        'log_summary_period': {
            'secs': 0,
            'nanos': 500000000
        },
        'log_summary_style': 'plain',
        'state_sync': {
            'sync': {
                'ExternalStorage': {
                    'location': {
                        'Filesystem': {
                            'root_dir': state_parts_dir
                        }
                    }
                }
            }
        },
        'state_sync_enabled': True,
        'consensus.state_sync_timeout': {
            'secs': 0,
            'nanos': 500000000
        },
        # The schedule means that the node tracks all shards all the time except for epoch heights 2 and 3.
        # Those epochs correspond to block heights [EPOCH_LENGTH * 2 + 1, EPOCH_LENGTH * 4].
        'tracked_shard_schedule': [
            [0],  # epoch_height = 0 and 4
            [0],  # epoch_height = 1* and 1 and 5
            [],  # epoch_height = 2
            [],  # epoch_height = 3
        ],
        'tracked_shards': [],
    }

    config = load_config()
    nodes = start_cluster(1, 1, 1, config, [["epoch_length", EPOCH_LENGTH]], {
        0: config0,
        1: config1
    })
    [boot_node, node] = nodes

    logger.info('started the nodes')

    test_account_id = node.signer_key.account_id
    test_account_key = node.signer_key
    account_ids = [boot_node.signer_key.account_id, test_account_id]

    print_balances(nodes, account_ids)

    nonce = 10

    latest_block = utils.wait_for_blocks(boot_node,
                                         target=int(2.5 * EPOCH_LENGTH))
    epoch = epoch_height(latest_block.height)
    assert epoch == 2, f"epoch: {epoch}"
    node.kill()
    # Restart the node to make it start without opening any flat storages.
    node.start(boot_node=boot_node)
    logger.info(f'We are in epoch {epoch}, and the node is restarted')

    print_balances(nodes, account_ids)

    # Delete the account.
    latest_block_hash = boot_node.get_latest_block().hash_bytes
    nonce += 1
    tx = transaction.sign_delete_account_tx(test_account_key, test_account_id,
                                            boot_node.signer_key.account_id,
                                            nonce, latest_block_hash)
    result = boot_node.send_tx(tx)
    logger.info(result)
    logger.info(f'Deleted {test_account_id}')

    # Wait until the node tracks the shard and probably does the catchup.
    latest_block = utils.wait_for_blocks(boot_node,
                                         target=int(4.5 * EPOCH_LENGTH))
    epoch = epoch_height(latest_block.height)
    assert epoch == 4, f"epoch: {epoch}"
    logger.info(f'We are in epoch {epoch}')

    # Ensure the non-validator node has caught up.
    utils.wait_for_blocks(node, target=int(4.5 * EPOCH_LENGTH))
    logger.info(f'The other node is in sync')

    print_balances(nodes, account_ids)

    # Check that the lookup of a deleted account returns an error. Because it's deleted.
    test_account_balance = node.json_rpc(
        'query', {
            'request_type': 'view_account',
            'account_id': test_account_id,
            'finality': 'optimistic'
        })
    assert 'error' in test_account_balance, test_account_balance

    # Send tokens.
    # The transaction will be accepted and will detect that the receiver account was deleted.
    latest_block_hash = boot_node.get_latest_block().hash_bytes
    nonce += 1
    tx = transaction.sign_payment_tx(boot_node.signer_key, test_account_id, 1,
                                     nonce, latest_block_hash)
    logger.info(
        f'Sending a token from {boot_node.signer_key.account_id} to {test_account_id} now'
    )
    result = boot_node.send_tx_and_wait(tx, 10)
    assert 'result' in result and 'error' not in result, (
        'Expected "result" and no "error" in response, got: {}'.format(result))

    print_balances(nodes, account_ids)

    # Wait a bit more and check that the non-validator node is in sync.
    utils.wait_for_blocks(boot_node, target=int(5.8 * EPOCH_LENGTH))
    boot_node_latest_block_height = boot_node.get_latest_block().height
    node_latest_block_height = node.get_latest_block().height
    logger.info(
        f'The validator node is at block height {boot_node_latest_block_height} in epoch {epoch_height(boot_node_latest_block_height)}'
    )
    logger.info(
        f'The non-validator node is at block height {node_latest_block_height}')

    # Check that the non-validator node is not stuck, and is in-sync.
    assert boot_node_latest_block_height < int(
        0.5 * EPOCH_LENGTH) + node_latest_block_height


if __name__ == "__main__":
    main()
