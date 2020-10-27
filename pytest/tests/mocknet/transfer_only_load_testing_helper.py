import sys, time
from rc import pmap, run

from load_testing_helper import (get_test_accounts_from_args, send_transfer, write_tx_events)

sys.path.append('lib')
from mocknet import NUM_NODES, TX_OUT_FILE

TEST_TIMEOUT = 180
TRANSFER_SLEEP_TIME = 0.011111111111111112

def send_transfers(test_accounts, i0, start_time):
    while True:
        for (account, i) in test_accounts:
            if time.time() - start_time >= TEST_TIMEOUT:
                return
            send_transfer(account, i, i0)
            time.sleep(TRANSFER_SLEEP_TIME)

if __name__ == '__main__':
    test_accounts = get_test_accounts_from_args()
    run(f'rm -rf {TX_OUT_FILE}')

    i0 = test_accounts[0][1]

    start_time = time.time()

    send_transfers(test_accounts, i0, start_time)
    
    write_tx_events(test_accounts)
