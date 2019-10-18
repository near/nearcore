# Chaos Monkey test. Simulates random events and failures and makes sure the blockchain continues operating as expected
#
#     _.-._         ..-..         _.-._
#    (_-.-_)       /|'.'|\       (_'.'_)
#  mrf.\-/.        \)\-/(/        ,-.-.
#  __/ /-. \__   __/ ' ' \__   __/'-'-'\__
# ( (___/___) ) ( (_/-._\_) ) ( (_/   \_) )
#  '.Oo___oO.'   '.Oo___oO.'   '.Oo___oO.'
#
# Parameterized as:
#  `s`: number of shards
#  `n`: initial (and minimum) number of validators
#  `N`: max number of validators
#  `k`: number of observers (technically `k+1`, one more observer is used by the test)
#  `monkeys`: enabled monkeys (see below)
# Supports the following monkeys:
#  `node_set`: ocasionally spins up new nodes or kills existing ones, as long as the number of nodes doesn't exceed `N` and doesn't go below `n`. Also makes sure that for each shard there's at least one node that has been live sufficiently long
#  `node_restart`: ocasionally restarts nodes
#  `local_network`: ocasionally shuts down the network connection between random nodes
#  `global_network`: ocasionally shots down the network globally for several seconds
#  `transactions`: sends random transactions keeping track of expected balances
#  `staking`: runs staking transactions for validators. Presently the test doesn't track staking invariants, relying on asserts in the nearcore.
#             `staking2.py` tests some basic stake invariants
# This test also completely disables rewards, which simplifies ensuring total supply invariance and balance invariances

import sys, time, base58, random, inspect, traceback
from multiprocessing import Process, Value, Lock

sys.path.append('lib')

from cluster import init_cluster, spin_up_node
from utils import TxContext
from transaction import sign_payment_tx, sign_staking_tx

TIMEOUT = 1500 # after how much time to shut down the test
TIMEOUT_SHUTDOWN = 60 # time to wait after the shutdown was initiated before 
BLOCK_TIMEOUT = 20 # if two blocks are not produced within that many seconds, the test will fail
BALANCES_TIMEOUT = 30 # how long to tolerate for balances to update after txs are sent
MAX_STAKE = int(1e26)
EPOCH_LENGTH = 20

assert BALANCES_TIMEOUT * 2 <= TIMEOUT_SHUTDOWN
assert BLOCK_TIMEOUT * 2 <= TIMEOUT_SHUTDOWN

def stress_process(func):
    def wrapper(stopped, error, *args):
        try:
            func(stopped, error, *args)
        except:
            traceback.print_exc()
            error.value = 1
    wrapper.__name__ = func.__name__
    return wrapper

def get_recent_hash(node):
    # return the parent of the last block known to the observer
    # don't return the last block itself, since some validators might have not seen it yet
    # also returns the height of the actual last block (so the height doesn't match the hash!)
    status = node.get_status()
    hash_ = status['sync_info']['latest_block_hash']
    info = node.json_rpc('block', [hash_])
    hash_ = info['result']['header']['hash']
    return hash_, status['sync_info']['latest_block_height']


def get_validator_ids(nodes):
    # the [4:] part is a hack to convert test7 => 7
    return set([int(x['account_id'][4:]) for x in nodes[-1].get_status()['validators']])

@stress_process
def monkey_node_set():
    pass

@stress_process
def monkey_node_restart():
    pass

@stress_process
def monkey_local_network():
    pass

@stress_process
def monkey_global_network():
    pass

@stress_process
def monkey_transactions(stopped, error, nodes, nonces):
    def get_balances():
        acts = [
            nodes[-1].get_account("test%s" % i)['result']
            for i in range(len(nodes))
        ]
        return [int(x['amount']) + int(x['locked']) for x in acts]

    expected_balances = get_balances()
    min_balances = [x - MAX_STAKE for x in expected_balances]
    total_supply = (sum(expected_balances))
    print("TOTAL SUPPLY: %s" % total_supply)
    
    last_iter_switch = time.time()
    mode = 0 # 0 = send more tx, 1 = wait for balances
    tx_count = 0
    last_tx_set = []
    while stopped.value == 0:
        validator_ids = get_validator_ids(nodes)
        if time.time() - last_iter_switch > BALANCES_TIMEOUT:
            if mode == 0:
                print("%s TRANSACTIONS SENT. WAITING FOR BALANCES" % tx_count)
                mode = 1
            else: assert False, "Balances didn't update in time. Expected: %s, received: %s" % (expected_balances, get_balances())
            last_iter_switch = time.time()

        if mode == 0:
            from_ = random.randint(0, len(nodes) - 1)
            while min_balances[from_] < 0:
                from_ = random.randint(0, len(nodes) - 1)
            to = random.randint(0, len(nodes) - 1)
            while from_ == to:
                to = random.randint(0, len(nodes) - 1)
            amt = random.randint(0, min_balances[from_])
            nonce_val, nonce_lock = nonces[from_]

            hash_, _ = get_recent_hash(nodes[-1])

            with nonce_lock:
                tx = sign_payment_tx(nodes[from_].signer_key, 'test%s' % to, amt, nonce_val.value, base58.b58decode(hash_.encode('utf8')))
                last_tx_set.append(tx)
                for validator_id in validator_ids:
                    nodes[validator_id].send_tx(tx)
                nonce_val.value = nonce_val.value + 1

            expected_balances[from_] -= amt
            expected_balances[to] += amt
            min_balances[from_] -= amt

            tx_count += 1

        else:
            if get_balances() == expected_balances:
                print("BALANCES CAUGHT UP, BACK TO SPAMMING TXS")
                min_balances = [x - MAX_STAKE for x in expected_balances]
                tx_count = 0
                mode = 0
                last_tx_set = []
            else:
                for tx in last_tx_set:
                    for validator_id in validator_ids:
                        nodes[validator_id].send_tx(tx)
            
        if mode == 1: time.sleep(1)
        elif mode == 0: time.sleep(0.1)

    shutdown_started = time.time()
    while time.time() - shutdown_started < BALANCES_TIMEOUT:
        if expected_balances == get_balances():
            return

    assert False, "Balances didn't update in time. Expected: %s, received: %s" % (expected_balances, get_balances())

@stress_process
def monkey_staking(stopped, error, nodes, nonces):
    while stopped.value == 0:
        validator_ids = get_validator_ids(nodes)
        whom = random.randint(0, len(nonces) - 1)

        status = nodes[-1].get_status()
        hash_, height = get_recent_hash(nodes[-1])

        who_can_unstake = (height // EPOCH_LENGTH) % len(nodes)

        nonce_val, nonce_lock = nonces[whom]
        with nonce_lock:
            stake = random.randint(0.7 * MAX_STAKE // 1000000, MAX_STAKE // 1000000) * 1000000

            if whom == who_can_unstake:
                stake = 0

            tx = sign_staking_tx(nodes[whom].signer_key, nodes[whom].validator_key, stake, nonce_val.value, base58.b58decode(hash_.encode('utf8')))
            for validator_id in validator_ids:
                nodes[validator_id].send_tx(tx)
            nonce_val.value = nonce_val.value + 1

        time.sleep(1)
        

@stress_process
def blocks_tracker(stopped, error, nodes, nonces):
    # note that we do not do `white stopped.value == 0`. When the test finishes, we want
    # to wait for at least one more block to be produced
    mapping = {}
    height_to_hash = {}
    largest_height = 0
    largest_divergence = 0
    last_updated = time.time()
    done = False
    while not done:
        # always query the last validator, and a random one
        for val_id in [-1, random.randint(0, len(nodes) - 2)]:
            try:
                status = nodes[val_id].get_status()
                hash_ = status['sync_info']['latest_block_hash']
                height = status['sync_info']['latest_block_height']
                if height > largest_height:
                    if stopped.value != 0:
                        done = True
                    print("BLOCK TRACKER: new height %s" % largest_height)
                    largest_height = height
                    last_updated = time.time()

                elif time.time() - last_updated > BLOCK_TIMEOUT:
                    assert False, "Block production took more than %s seconds" % BLOCK_TIMEOUT

                if hash_ not in mapping:
                    block_info = nodes[val_id].json_rpc('block', [hash_])
                    confirm_height = block_info['result']['header']['height']
                    assert height == confirm_height
                    prev_hash = block_info['result']['header']['prev_hash']
                    if height in height_to_hash:
                        assert False, "Two blocks for the same height: %s and %s" % (height_to_hash[height], hash_)

                    height_to_hash[height] = hash_
                    mapping[hash_] = (prev_hash, height)

            except:
                # other monkeys can tamper with all the nodes but the last one, so exceptions are possible
                # the last node must always respond, so we rethrow
                if val_id == -1:
                    raise
        time.sleep(0.2)

    for B1, (P1, H1) in [x for x in mapping.items()]:
        for b2, (p2, h2) in [x for x in mapping.items()]:
            b1, p1, h1 = B1, P1, H1
            if abs(h1 - h2) < 8:
                initial_smaller_height = min(h1, h2)
                try:
                    while b1 != b2:
                        while h1 > h2:
                            (b1, (p1, h1)) = (p1, mapping[p1])
                        while h2 > h1:
                            (b2, (p2, h2)) = (p2, mapping[p2])
                        while h1 == h2 and b1 != b2:
                            (b1, (p1, h1)) = (p1, mapping[p1])
                            (b2, (p2, h2)) = (p2, mapping[p2])
                    assert h1 == h2
                    assert b1 == b2
                    assert p1 == p2
                    divergence = initial_smaller_height - h1
                except KeyError as e:
                    # some blocks were missing in the mapping, so do our best estimate
                    divergence = initial_smaller_height - min(h1, h2)
                    
                if divergence > largest_divergence:
                    largest_divergence = divergence
                        
    print("=== BLOCK TRACKER SUMMARY ===")
    print("Largest height:     %s" % largest_height)
    print("Largest divergence: %s" % largest_divergence)

    assert largest_divergence < len(nodes)


def doit(s, n, N, k, monkeys):
    assert 2 <= n <= N

    config = {'local': True, 'near_root': '../target/debug/'}
    local_config_changes = {}

    for i in range(N, N + k + 1):
        # make all the observers track all the shards
        local_config_changes[i] = {"tracked_shards": list(range(s))}

    near_root, node_dirs = init_cluster(N, s, k + 1, config, [["max_inflation_rate", 0], ["epoch_length", EPOCH_LENGTH], ["validator_kickout_threshold", 75]], local_config_changes)

    started = time.time()

    boot_node = spin_up_node(config, near_root, node_dirs[0], 0, None, None)
    nodes = [boot_node]

    for i in range(1, N + k + 1):
        if i < n or i >= N:
            node = spin_up_node(config, near_root, node_dirs[i], i, boot_node.node_key.pk, boot_node.addr())
            nodes.append(node)
        else:
            nodes.append(None)

    stopped = Value('i', 0)
    error = Value('i', 0)
    ps = []
    nonces = [(Value('i', 1), Lock()) for _ in range(N + k + 1)]

    def launch_process(func):
        nonlocal stopped, error, ps

        p = Process(target=func, args=(stopped, error, nodes, nonces))
        p.start()
        ps.append((p, func.__name__))

    def check_errors():
        nonlocal error, ps
        if error.value != 0:
            for (p, _) in ps:
                p.terminate()
            assert False, "At least one process failed, check error messages above"

    for monkey in monkeys:
        launch_process(globals()['monkey_%s' % monkey])

    launch_process(blocks_tracker)

    started = time.time()
    while time.time() - started < TIMEOUT:
        check_errors()
        time.sleep(1)
    
    print("")
    print("==========================================")
    print("# TIMEOUT IS HIT, SHUTTING DOWN THE TEST #")
    print("==========================================")
    stopped.value = 1
    started_shutdown = time.time()
    while True:
        check_errors()
        still_running = [name for (p, name) in ps if p.is_alive()]

        if len(still_running) == 0:
            break

        if time.time() - started_shutdown > TIMEOUT_SHUTDOWN:
            for (p, _) in ps:
                p.terminate()
            assert False, "The test didn't gracefully shut down in time\nStill running: %s" % (still_running)

    check_errors()


MONKEYS = dict([(name[7:], obj) for name, obj in inspect.getmembers(sys.modules[__name__]) if inspect.isfunction(obj) and name.startswith("monkey_")])

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage:\npython tests/stress/stress.py s n N k monkey1 monkey2 ...")
        sys.exit(1)

    s = int(sys.argv[1])
    n = int(sys.argv[2])
    N = int(sys.argv[3])
    k = int(sys.argv[4])
    monkeys = sys.argv[5:]

    assert len(monkeys) == len(set(monkeys)), "Monkeys should be unique"
    for monkey in monkeys:
        assert monkey in MONKEYS, "Unknown monkey \"%s\"" % monkey

    doit(s, n, N, k, monkeys)

