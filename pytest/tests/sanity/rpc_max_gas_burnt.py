"""Test max_gas_burnt_view client configuration.

Spins up two nodes with different max_gas_burnt_view client configuration,
deploys a smart contract and finally calls a view function against both nodes
expecting the one with low max_gas_burnt_view limit to fail.
"""

import sys
import base58
import base64
import json

sys.path.append('lib')
from cluster import start_cluster
from utils import load_binary_file
import transaction


def test_max_gas_burnt_view():
    nodes = start_cluster(
        2,
        0,
        1,
        config=None,
        genesis_config_changes=[],
        client_config_changes={1: {
            'max_gas_burnt_view': int(5e10)
        }})

    contract_key = nodes[0].signer_key
    contract = load_binary_file(
        '../runtime/near-test-contracts/res/test_contract_rs.wasm')

    # Deploy the fib smart contract
    status = nodes[0].get_status()
    latest_block_hash = status['sync_info']['latest_block_hash']
    deploy_contract_tx = transaction.sign_deploy_contract_tx(
        contract_key, contract, 10,
        base58.b58decode(latest_block_hash.encode('utf8')))
    deploy_contract_response = nodes[0].send_tx_and_wait(deploy_contract_tx, 10)

    def call_fib(node, n):
        args = base64.b64encode(bytes([n])).decode('ascii')
        return node.call_function(contract_key.account_id,
                                  'fibonacci',
                                  args,
                                  timeout=10).get('result')

    # Call view function of the smart contract via the first node.  This should
    # succeed.
    result = call_fib(nodes[0], 25)
    assert 'result' in result and 'error' not in result, (
        'Expected "result" and no "error" in response, got: {}'.format(result))
    n = int.from_bytes(bytes(result['result']), 'little')
    assert n == 75025, 'Expected result to be 75025 but got: {}'.format(n)

    # Same but against the second node.  This should fail because of gas limit.
    result = call_fib(nodes[1], 25)
    assert 'result' not in result and 'error' in result, (
        'Expected "error" and no "result" in response, got: {}'.format(result))
    error = result['error']
    assert 'HostError(GasLimitExceeded)' in error, (
        'Expected error due to GasLimitExceeded but got: {}'.format(error))

    # It should still succeed for small arguments.
    result = call_fib(nodes[1], 5)
    assert 'result' in result and 'error' not in result, (
        'Expected "result" and no "error" in response, got: {}'.format(result))
    n = int.from_bytes(bytes(result['result']), 'little')
    assert n == 5, 'Expected result to be 5 but got: {}'.format(n)


if __name__ == '__main__':
    test_max_gas_burnt_view()
