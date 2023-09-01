#!/usr/bin/env python3
# Spins up a validator node tracking all shards and a non-validator node.
# Create an account
# Restart the non-validator node.
# Delete the accounts while the non-validator node is not tracking that shard.
# After the non-validator node does catchup, attempt to send a token to the deleted account.
# Observe that both nodes correctly execute the transaction and return an error.

import pathlib
import sys
import tempfile

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import start_cluster, load_config
import transaction
import utils
import key

from configured_logger import logger

EPOCH_LENGTH = 20

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
            res = node.json_rpc('query', {'request_type': 'view_account', 'account_id': account_id, 'finality': 'optimistic'})
            logger.info(f'{node.signer_key.account_id}: {account_id}: {res}')

def get_nonce_for_pk(node, account_id, pk):
    access_keys = node.json_rpc('query', { 'request_type': 'view_access_key_list', 'account_id': account_id, 'finality': 'optimistic' })
    if not access_keys['result']['keys']:
        raise KeyError(account_id)

    nonce = next((key['access_key']['nonce'] for key in access_keys['result']['keys'] if key['public_key'] == pk), None)
    if nonce is None:
        raise KeyError(f'Nonce for {account_id} {pk} not found')
    return nonce


def main():
    state_parts_dir = str(pathlib.Path(tempfile.gettempdir()) / 'state_parts')

    config0 = {
        'gc_num_epochs_to_keep': 100,
        'log_summary_period': {
            'secs': 0,
            'nanos': 500000000
        },
        'log_summary_style': 'plain',
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
        'tracked_shards': [0],
    }
    config1 = {
        'gc_num_epochs_to_keep': 100,
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
        'tracked_shard_schedule': [
            [0],
            [0],
            [],
            [],
            [0],
            [0],
            [0],
            [0],
        ],
        'tracked_shards': [],
    }

    config = load_config()
    nodes = start_cluster(1, 1, 1, config, [["epoch_length", EPOCH_LENGTH]], { 0: config0, 1: config1 })
    [boot_node, node] = nodes

    logger.info('started the nodes')

    sub_account_id = "sub." + boot_node.signer_key.account_id
    account_ids = [boot_node.signer_key.account_id, sub_account_id]

    print_balances(nodes, account_ids)

    # Create account.

    latest_block = boot_node.get_latest_block()
    latest_block_hash = latest_block.hash_bytes
    nonce1 = 10

    sub_account_key = key.Key.from_random(sub_account_id)

    logger.info(boot_node.signer_key.to_json())
    logger.info(sub_account_key.to_json())

    nonce1 += 1
    tx = transaction.sign_create_account_with_full_access_key_and_balance_tx(boot_node.signer_key, sub_account_id, sub_account_key, 100*(10**24), nonce1, latest_block_hash)
    result = boot_node.send_tx_and_wait(tx, 10)
    assert 'result' in result and 'error' not in result, ('Expected "result" and no "error" in response, got: {}'.format(result))
    logger.info(f'Created an account {sub_account_id}')

    print_balances(nodes, account_ids)

    # Send tokens to check the account exists.
    latest_block_hash = boot_node.get_latest_block().hash_bytes
    nonce2 = get_nonce_for_pk(boot_node, sub_account_id, sub_account_key.pk)
    nonce2 += 1
    tx = transaction.sign_payment_tx(boot_node.signer_key, sub_account_id, 1, nonce2, latest_block_hash)
    result = boot_node.send_tx_and_wait(tx, 10)
    assert 'result' in result and 'error' not in result, ('Expected "result" and no "error" in response, got: {}'.format(result))
    logger.info(f'Sent a token from {boot_node.signer_key.account_id} to {sub_account_id}')

    print_balances(nodes, account_ids)

    logger.info(boot_node.signer_key.to_json())
    logger.info(sub_account_key.to_json())

    # Send tokens back
    latest_block_hash = boot_node.get_latest_block().hash_bytes
    nonce2 += 1
    tx = transaction.sign_payment_tx(sub_account_key, boot_node.signer_key.account_id, 1, nonce2, latest_block_hash)
    result = boot_node.send_tx_and_wait(tx, 10)
    assert 'result' in result and 'error' not in result, ('Expected "result" and no "error" in response, got: {}'.format(result))
    logger.info(f'Sent a token from {sub_account_id} to {boot_node.signer_key.account_id}')

    print_balances(nodes, account_ids)

    latest_block = boot_node.get_latest_block()
    epoch = epoch_height(latest_block.height)
    assert epoch == 1 or epoch == "1*", f"epoch: {epoch}"
    logger.info(f'We are in epoch {epoch}')

    latest_block = utils.wait_for_blocks(boot_node, target=int(2.5 * EPOCH_LENGTH))
    epoch = epoch_height(latest_block.height)
    assert epoch == 2, f"epoch: {epoch}"
    node.kill()
    # Restart the node to make it start without opening any flat storages.
    node.start(boot_node=boot_node)
    logger.info(f'We are in epoch {epoch}, and the node is restarted')

    print_balances(nodes, account_ids)

    # Delete the account.
    latest_block_hash = boot_node.get_latest_block().hash_bytes
    nonce2 += 1
    tx = transaction.sign_delete_account_tx(sub_account_key, sub_account_id, boot_node.signer_key.account_id, nonce2, latest_block_hash)
    result = boot_node.send_tx(tx)
    logger.info(result)
    logger.info(f'Deleted {sub_account_id}')

    latest_block = utils.wait_for_blocks(boot_node, target=int(3.5 * EPOCH_LENGTH))
    epoch = epoch_height(latest_block.height)
    assert epoch == 3, f"epoch: {epoch}"
    print_balances(nodes, account_ids)
    logger.info(f'We are in epoch {epoch}')

    # Wait until the node tracks the shard and probably does the catchup.
    latest_block = utils.wait_for_blocks(boot_node, target=int(4.5 * EPOCH_LENGTH))
    epoch = epoch_height(latest_block.height)
    assert epoch == 4, f"epoch: {epoch}"
    logger.info(f'The time has come')
    # Ensure the non-validator node has caught up.
    utils.wait_for_blocks(node, target=int(4.5 * EPOCH_LENGTH))
    logger.info(f'The other node is in sync')

    print_balances(nodes, account_ids)

    for i in range(1):
        # Send tokens and expect the transaction to fail.
        latest_block_hash = boot_node.get_latest_block().hash_bytes
        nonce1 = get_nonce_for_pk(boot_node, boot_node.signer_key.account_id, boot_node.signer_key.pk)
        nonce1 += 1
        tx = transaction.sign_payment_tx(boot_node.signer_key, sub_account_id, 1, nonce1, latest_block_hash)
        logger.info(f'Sending a token from {boot_node.signer_key.account_id} to {sub_account_id} now')
        result = boot_node.send_tx_and_wait(tx, 10)
        assert 'result' in result and 'error' not in result, ('Expected "result" and no "error" in response, got: {}'.format(result))

        print_balances(nodes, account_ids)

    utils.wait_for_blocks(boot_node, target=int(5.5 * EPOCH_LENGTH))

    latest_block = boot_node.get_latest_block()
    assert latest_block.height < int(0.5 * EPOCH_LENGTH) + node.get_latest_block().height

if __name__ == "__main__":
    main()
