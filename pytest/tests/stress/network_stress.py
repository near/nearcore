import sys, random, time, base58, requests

sys.path.append('lib')

from stress import stress_process, doit, monkey_staking, get_validator_ids, get_the_guy_to_mess_up_with, get_recent_hash, sign_payment_tx, expect_network_issues
from network import stop_network, resume_network, init_network_pillager

TIMEOUT = 300


@stress_process
def monkey_transactions_noval(stopped, error, nodes, nonces):
    while stopped.value == 0:
        validator_ids = get_validator_ids(nodes)

        from_ = random.randint(0, len(nodes) - 1)
        to = random.randint(0, len(nodes) - 1)
        while from_ == to:
            to = random.randint(0, len(nodes) - 1)
        amt = random.randint(0, 100)
        nonce_val, nonce_lock = nonces[from_]

        hash_, _ = get_recent_hash(nodes[-1])

        with nonce_lock:
            tx = sign_payment_tx(nodes[from_].signer_key, 'test%s' % to, amt,
                                 nonce_val.value,
                                 base58.b58decode(hash_.encode('utf8')))
            for validator_id in validator_ids:
                try:
                    tx_hash = nodes[validator_id].send_tx(tx)['result']
                except requests.exceptions.ReadTimeout:
                    pass

            nonce_val.value = nonce_val.value + 1

        time.sleep(0.1)


@stress_process
def monkey_network_hammering(stopped, error, nodes, nonces):
    s = [False for x in nodes]
    while stopped.value == 0:
        node_idx = random.randint(0, len(nodes) - 2)
        pid = nodes[node_idx].pid.value
        if s[node_idx]:
            print("Resuming network for process %s" % pid)
            resume_network(pid)
        else:
            print("Stopping network for process %s" % pid)
            stop_network(pid)
        s[node_idx] = not s[node_idx]

        time.sleep(0.5)
    for i, x in enumerate(s):
        if x:
            pid = nodes[i].pid.value
            print("Resuming network for process %s" % pid)
            resume_network(pid)


expect_network_issues()
init_network_pillager()
doit(3, 3, 3, 0,
     [monkey_network_hammering, monkey_transactions_noval, monkey_staking],
     TIMEOUT)
