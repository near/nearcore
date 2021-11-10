#!/usr/bin/env python3
"""Test if the node is backwards compatible with the latest release."""

import base58
import os
import pathlib
import re
import subprocess
import sys
import time
import typing

sys.path.append('lib')

import branches
import cluster
from configured_logger import logger
from utils import wait_for_blocks_or_timeout, load_test_contract, get_near_tempdir
from transaction import sign_deploy_contract_tx, sign_function_call_tx, sign_payment_tx, \
    sign_create_account_tx, sign_delete_account_tx, sign_create_account_with_full_access_key_and_balance_tx

_EXECUTABLES = None


def get_executables() -> branches.ABExecutables:
    global _EXECUTABLES
    if _EXECUTABLES is None:
        branch = branches.latest_rc_branch()
        logger.info(f"Latest rc release branch is {branch}")
        _EXECUTABLES = branches.prepare_ab_test(branch)
    return _EXECUTABLES


def test_protocol_versions() -> None:
    """Verify that ‘stable’ and ‘current’ protocol versions differ by at most 1.

    Checks whether the protocol number’s used by the latest release and current
    binary do not differed by more than one.  This is because some protocol
    features implementations rely on the fact that no protocol version is
    skipped.  See <https://github.com/near/nearcore/issues/4956>.
    """
    executables = get_executables()

    def get_proto_version(exe: pathlib.Path) -> int:
        line = subprocess.check_output((exe, '--version'), text=True)
        m = re.search(r'\(release (.*?)\) .* \(protocol ([0-9]+)\)', line)
        assert m, (f'Unable to extract protocol version number from {exe};\n'
                   f'Got {line.rstrip()} on standard output')
        return m.group(1), int(m.group(2))

    stable_release, stable_proto = get_proto_version(executables.stable.neard)
    curr_release, curr_proto = get_proto_version(executables.current.neard)
    assert stable_proto == curr_proto or stable_proto + 1 == curr_proto, (
        f'If changed, protocol version of a new release can increase by at '
        'most one.\n'
        f'Got protocol {stable_proto} version in release {stable_release}\n'
        f'but protocol {curr_proto} version in {curr_release}')
    logger.info(f'Got proto {stable_proto} in release {stable_release} and '
                f'and proto {curr_proto} in {curr_release}')


def test_upgrade() -> None:
    """Test that upgrade from ‘stable’ to ‘current’ binary is possible.

    1. Start a network with 3 `stable` nodes and 1 `new` node.
    2. Start switching `stable` nodes one by one with `new` nodes.
    3. Run for three epochs and observe that current protocol version of the
       network matches `new` nodes.
    """
    executables = get_executables()
    node_root = get_near_tempdir('upgradable', clean=True)

    # Setup local network.
    cmd = (executables.stable.neard, "--home=%s" % node_root, "testnet", "--v",
           "4", "--prefix", "test")
    logger.info(' '.join(str(arg) for arg in cmd))
    subprocess.check_call(cmd)
    genesis_config_changes = [("epoch_length", 20),
                              ("num_block_producer_seats", 10),
                              ("num_block_producer_seats_per_shard", [10]),
                              ("block_producer_kickout_threshold", 80),
                              ("chunk_producer_kickout_threshold", 80),
                              ("chain_id", "testnet")]
    node_dirs = [os.path.join(node_root, 'test%d' % i) for i in range(4)]
    for i, node_dir in enumerate(node_dirs):
        cluster.apply_genesis_changes(node_dir, genesis_config_changes)

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
    status = nodes[0].get_status()
    hash = status['sync_info']['latest_block_hash']
    tx = sign_deploy_contract_tx(nodes[0].signer_key, load_test_contract(), 1,
                                 base58.b58decode(hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res

    # write some random value
    tx = sign_function_call_tx(nodes[0].signer_key,
                               nodes[0].signer_key.account_id,
                               'write_random_value', [], 10**13, 0, 2,
                               base58.b58decode(hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    wait_for_blocks_or_timeout(nodes[0], 20, 120)

    # Restart stable nodes into new version.
    for i in range(3):
        nodes[i].kill()
        nodes[i].binary_name = config['binary_name']
        nodes[i].start(boot_node=nodes[0])

    wait_for_blocks_or_timeout(nodes[3], 60, 120)
    status0 = nodes[0].get_status()
    status3 = nodes[3].get_status()
    protocol_version = status0['protocol_version']
    latest_protocol_version = status3["latest_protocol_version"]
    assert protocol_version == latest_protocol_version, \
        "Latest protocol version %d should match active protocol version %d" % (
        latest_protocol_version, protocol_version)

    hash = status0['sync_info']['latest_block_hash']

    # write some random value again
    tx = sign_function_call_tx(nodes[0].signer_key,
                               nodes[0].signer_key.account_id,
                               'write_random_value', [], 10**13, 0, 4,
                               base58.b58decode(hash.encode('utf8')))
    res = nodes[0].send_tx_and_wait(tx, timeout=20)
    assert 'error' not in res, res
    assert 'Failure' not in res['result']['status'], res

    # hex_account_id = (b"I'm hex!" * 4).hex()
    hex_account_id = '49276d206865782149276d206865782149276d206865782149276d2068657821'
    tx = sign_payment_tx(key=nodes[0].signer_key,
                         to=hex_account_id,
                         amount=10**25,
                         nonce=5,
                         blockHash=base58.b58decode(hash.encode('utf8')))
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
