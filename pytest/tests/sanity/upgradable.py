#!/usr/bin/env python3
"""Test if the node is backwards compatible with the latest release."""

import base58
import json
import os
import pathlib
import re
import subprocess
import sys
import time
import typing
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import branches
import cluster
from configured_logger import logger
from transaction import sign_deploy_contract_tx, sign_function_call_tx, sign_payment_tx
import utils

_EXECUTABLES = None


def get_executables() -> branches.ABExecutables:
    global _EXECUTABLES
    if _EXECUTABLES is None:
        _EXECUTABLES = branches.prepare_ab_test()
        logger.info(f"Latest mainnet release is {_EXECUTABLES.release}")
    return _EXECUTABLES


def test_protocol_versions() -> None:
    """Verify that mainnet, testnet and current protocol versions differ by ≤ 1.

    Checks whether the protocol versions used by the latest mainnet, the latest
    testnet and current binary do not differed by more than one.  Some protocol
    features implementations rely on the fact that no protocol version is
    skipped.  See <https://github.com/near/nearcore/issues/4956>.

    This test downloads the latest official mainnet and testnet binaries.  If
    that fails for whatever reason, builds each of those executables.
    """
    executables = get_executables()
    testnet = branches.get_executables_for('testnet')

    def get_proto_version(exe: pathlib.Path) -> int:
        line = subprocess.check_output((exe, '--version'), text=True)
        m = re.search(r'\(release (.*?)\) .* \(protocol ([0-9]+)\)', line)
        assert m, (f'Unable to extract protocol version number from {exe};\n'
                   f'Got {line.rstrip()} on standard output')
        return m.group(1), int(m.group(2))

    main_release, main_proto = get_proto_version(executables.stable.neard)
    test_release, test_proto = get_proto_version(testnet.neard)
    _, head_proto = get_proto_version(executables.current.neard)

    logger.info(f'Got protocol {main_proto} in mainnet release {main_release}.')
    logger.info(f'Got protocol {test_proto} in testnet release {test_release}.')
    logger.info(f'Got protocol {head_proto} on master branch.')

    ok = (head_proto in (test_proto, test_proto + 1) and
          test_proto in (main_proto, main_proto + 1))
    assert ok, ('If changed, protocol version of a new release can increase by '
                'at most one.')


def test_upgrade() -> None:
    """Test that upgrade from ‘stable’ to ‘current’ binary is possible.

    1. Start a network with 3 `stable` nodes and 1 `new` node.
    2. Start switching `stable` nodes one by one with `new` nodes.
    3. Run for three epochs and observe that current protocol version of the
       network matches `new` nodes.
    """
    executables = get_executables()
    node_root = utils.get_near_tempdir('upgradable', clean=True)

    # Setup local network.
    cmd = (executables.stable.neard, f'--home={node_root}', 'localnet', '-v',
           '4', '--prefix', 'test')
    logger.info(' '.join(str(arg) for arg in cmd))
    subprocess.check_call(cmd)
    genesis_config_changes = [("epoch_length", 20),
                              ("num_block_producer_seats", 10),
                              ("num_block_producer_seats_per_shard", [10]),
                              ("block_producer_kickout_threshold", 80),
                              ("chunk_producer_kickout_threshold", 80)]
    node_dirs = [os.path.join(node_root, 'test%d' % i) for i in range(4)]
    for i, node_dir in enumerate(node_dirs):
        cluster.apply_genesis_changes(node_dir, genesis_config_changes)
        cluster.apply_config_changes(node_dir, {'tracked_shards': [0]})

        # Adjust changes required since #7486.  This is needed because current
        # stable release populates the deprecated migration configuration options.
        # TODO(mina86): Remove this once we get stable release which doesn’t
        # populate those fields by default.
        config_path = pathlib.Path(node_dir) / 'config.json'
        data = json.loads(config_path.read_text(encoding='utf-8'))
        data.pop('db_migration_snapshot_path', None)
        data.pop('use_db_migration_snapshot', None)
        config_path.write_text(json.dumps(data), encoding='utf-8')

    # Start 3 stable nodes and one current node.
    config = executables.stable.node_config()
    nodes = [
        cluster.spin_up_node(config, executables.stable.root, node_dirs[0], 0)
    ]
    for i in range(1, 3):
        nodes.append(
            cluster.spin_up_node(config,
                                 executables.stable.root,
                                 node_dirs[i],
                                 i,
                                 boot_node=nodes[0]))
    config = executables.current.node_config()
    nodes.append(
        cluster.spin_up_node(config,
                             executables.current.root,
                             node_dirs[3],
                             3,
                             boot_node=nodes[0]))

    time.sleep(2)

    # deploy a contract
    hash = nodes[0].get_latest_block().hash_bytes
    tx = sign_deploy_contract_tx(nodes[0].signer_key,
                                 utils.load_test_contract(), 1, hash)
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res

    # write some random value
    tx = sign_function_call_tx(nodes[0].signer_key,
                               nodes[0].signer_key.account_id,
                               'write_random_value', [], 10**13, 0, 2, hash)
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    utils.wait_for_blocks(nodes[0], count=20)

    # Restart stable nodes into new version.
    for i in range(3):
        nodes[i].kill()
        nodes[i].binary_name = config['binary_name']
        nodes[i].start(
            boot_node=nodes[0],
            extra_env={"NEAR_TESTS_IMMEDIATE_PROTOCOL_UPGRADE": "1"},
        )

    utils.wait_for_blocks(nodes[3], count=60)
    status0 = nodes[0].get_status()
    status3 = nodes[3].get_status()
    protocol_version = status0['protocol_version']
    latest_protocol_version = status3["latest_protocol_version"]
    assert protocol_version == latest_protocol_version, \
        "Latest protocol version %d should match active protocol version %d" % (
        latest_protocol_version, protocol_version)

    hash = base58.b58decode(
        status0['sync_info']['latest_block_hash'].encode('ascii'))

    # write some random value again
    tx = sign_function_call_tx(nodes[0].signer_key,
                               nodes[0].signer_key.account_id,
                               'write_random_value', [], 10**13, 0, 4, hash)
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    # hex_account_id = (b"I'm hex!" * 4).hex()
    hex_account_id = '49276d206865782149276d206865782149276d206865782149276d2068657821'
    tx = sign_payment_tx(key=nodes[0].signer_key,
                         to=hex_account_id,
                         amount=10**25,
                         nonce=5,
                         blockHash=hash)
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    # Successfully created a new account on transfer to hex
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    hex_account_balance = int(
        nodes[0].get_account(hex_account_id)['result']['amount'])
    assert hex_account_balance == 10**25


def main():
    test_protocol_versions()
    test_upgrade()


if __name__ == "__main__":
    main()
