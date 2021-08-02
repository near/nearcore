# The test launches two validating node out of three validators.
# Transfer some tokens between two accounts (thus changing state).
# Query for no finality, doomslug finality

import sys, time, base58, random

sys.path.append('lib')

from cluster import start_cluster
from configured_logger import logger
from utils import TxContext
from transaction import sign_payment_tx

nodes = start_cluster(3, 1, 1, None,
                      [["min_gas_price", 0], ["epoch_length", 100]], {})

time.sleep(3)
# kill one validating node so that no block can be finalized
nodes[2].kill()
time.sleep(1)

acc0_balance = int(nodes[0].get_account('test0')['result']['amount'])
acc1_balance = int(nodes[0].get_account('test1')['result']['amount'])

token_transfer = 10
status = nodes[0].get_status()
latest_block_hash = status['sync_info']['latest_block_hash']
tx = sign_payment_tx(nodes[0].signer_key, 'test1', token_transfer, 1,
                     base58.b58decode(latest_block_hash.encode('utf8')))
logger.info(nodes[0].send_tx_and_wait(tx, timeout=20))

# wait for doomslug finality
time.sleep(5)
for i in range(2):
    acc_id = 'test0' if i == 0 else 'test1'
    acc_no_finality = nodes[0].get_account(acc_id)
    acc_doomslug_finality = nodes[0].get_account(acc_id, "near-final")
    acc_nfg_finality = nodes[0].get_account(acc_id, "final")
    if i == 0:
        assert int(acc_no_finality['result']
                   ['amount']) == acc0_balance - token_transfer
        assert int(acc_doomslug_finality['result']
                   ['amount']) == acc0_balance - token_transfer
        assert int(acc_nfg_finality['result']['amount']) == acc0_balance
    else:
        assert int(acc_no_finality['result']
                   ['amount']) == acc1_balance + token_transfer
        assert int(acc_doomslug_finality['result']
                   ['amount']) == acc1_balance + token_transfer
        assert int(acc_nfg_finality['result']['amount']) == acc1_balance
