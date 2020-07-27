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
#  [v] `node_set`: ocasionally spins up new nodes or kills existing ones, as long as the number of nodes doesn't exceed `N` and doesn't go below `n`. Also makes sure that for each shard there's at least one node that has been live sufficiently long
#  [v] `node_restart`: ocasionally restarts nodes
#  [v] `local_network`: ocasionally briefly shuts down the network connection for a specific node
#  [ ] `global_network`: ocasionally shots down the network globally for several seconds
#  [v] `transactions`: sends random transactions keeping track of expected balances
#  [v] `staking`: runs staking transactions for validators. Presently the test doesn't track staking invariants, relying on asserts in the nearcore.
#                `staking2.py` tests some basic stake invariants
# This test also completely disables rewards, which simplifies ensuring total supply invariance and balance invariances

import sys, time, base58, random, inspect, traceback, requests
from multiprocessing import Process, Value, Lock

sys.path.append('lib')

from cluster import init_cluster, spin_up_node, load_config
from utils import TxContext, Unbuffered
from transaction import sign_payment_tx, sign_staking_tx
from network import init_network_pillager, stop_network, resume_network

sys.stdout = Unbuffered(sys.stdout)

TIMEOUT = 1500  # after how much time to shut down the test
TIMEOUT_SHUTDOWN = 120  # time to wait after the shutdown was initiated before
MAX_STAKE = int(1e26)
EPOCH_LENGTH = 20

block_timeout = 20  # if two blocks are not produced within that many seconds, the test will fail. The timeout is increased if nodes are restarted or network is being messed up with
balances_timeout = 15  # how long to tolerate for balances to update after txs are sent
tx_tolerance = 0.1

assert balances_timeout * 2 <= TIMEOUT_SHUTDOWN
assert block_timeout * 2 <= TIMEOUT_SHUTDOWN

network_issues_expected = False


def expect_network_issues():
    global network_issues_expected
    network_issues_expected = True


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
    return set([
        int(x['account_id'][4:]) for x in nodes[-1].get_status()['validators']
    ])


@stress_process
def monkey_node_set(stopped, error, nodes, nonces):

    def get_future_time():
        if random.choice([True, False]):
            return time.time() + random.randint(1, 5)
        else:
            return time.time() + random.randint(10, 30)

    nodes_stopped = [x.mess_with for x in nodes]
    change_status_at = [get_future_time() for x in nodes]
    while stopped.value == 0:
        for i, node in enumerate(nodes):
            if not node.mess_with:
                continue
            if time.time() < change_status_at[i]:
                continue
            if nodes_stopped[i]:
                print("Node set: starting node %s" % i)
                # figuring out a live node with `node_restart` monkey is not trivial
                # for simplicity just boot from the observer node
                # `node_restart` doesn't boot from the observer, increasing coverage
                boot_node = nodes[-1]
                node.start(boot_node.node_key.pk, boot_node.addr())
            else:
                node.kill()
                wipe = False
                if random.choice([True, False]):
                    wipe = True
                    #node.reset_data()
                print("Node set: stopping%s node %s" %
                      (" and wiping" if wipe else "", i))
            nodes_stopped[i] = not nodes_stopped[i]
            change_status_at[i] = get_future_time()


@stress_process
def monkey_node_restart(stopped, error, nodes, nonces):
    heights_after_restart = [0 for _ in nodes]
    while stopped.value == 0:
        node_idx = get_the_guy_to_mess_up_with(nodes)
        boot_node_idx = random.randint(0, len(nodes) - 2)
        while boot_node_idx == node_idx:
            boot_node_idx = random.randint(0, len(nodes) - 2)
        boot_node = nodes[boot_node_idx]

        node = nodes[node_idx]
        # don't kill the same node too frequently, give it time to reboot and produce something
        while True:
            _, h = get_recent_hash(node)
            assert h >= heights_after_restart[node_idx], "%s > %s" % (
                h, heights_after_restart[node_idx])
            if h > heights_after_restart[node_idx]:
                break
            time.sleep(1)

        print("NUKING NODE %s" % node_idx)
        node.kill()
        node.start(boot_node.node_key.pk, boot_node.addr())
        print("NODE %s IS BACK UP" % node_idx)

        _, heights_after_restart[node_idx] = get_recent_hash(node)

        time.sleep(5)


@stress_process
def monkey_local_network(stopped, error, nodes, nonces):
    while stopped.value == 0:
        # "- 2" below is because we don't want to kill the node we use to check stats
        node_idx = random.randint(0, len(nodes) - 2)
        nodes[node_idx].stop_network()
        if node_idx == get_the_guy_to_mess_up_with(nodes):
            time.sleep(5)
        else:
            time.sleep(1)
        nodes[node_idx].resume_network()
        time.sleep(5)


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
    mode = 0  # 0 = send more tx, 1 = wait for balances
    tx_count = 0
    last_tx_set = []

    rolling_tolerance = tx_tolerance

    # do not stop when waiting for balances
    while stopped.value == 0 or mode == 1:
        validator_ids = get_validator_ids(nodes)
        if time.time() - last_iter_switch > balances_timeout:
            if mode == 0:
                print("%s TRANSACTIONS SENT. WAITING FOR BALANCES" % tx_count)
                mode = 1
            else:
                print(
                    "BALANCES NEVER CAUGHT UP, CHECKING UNFINISHED TRANSACTIONS"
                )
                snapshot_expected_balances = [x for x in expected_balances]

                def revert_txs():
                    nonlocal expected_balances
                    good = 0
                    bad = 0
                    for tx in last_tx_set:
                        tx_happened = True
                        try:
                            response = nodes[-1].json_rpc(
                                'tx', [tx[3], "test%s" % tx[1]], timeout=1)

                            # due to #2195 if the tx was dropped, the query today times out.
                            if 'error' in response and 'data' in response[
                                    'error'] and response['error'][
                                        'data'] == 'Timeout':
                                tx_happened = False
                            elif 'result' in response and 'receipts_outcome' in response[
                                    'result']:
                                tx_happened = len(
                                    response['result']['receipts_outcome']) > 0
                            else:
                                assert False, response
                        # This exception handler is also due to #2195
                        except requests.exceptions.ReadTimeout:
                            tx_happened = False
                        except:
                            raise

                        if not tx_happened:
                            bad += 1
                            expected_balances[tx[1]] += tx[4]
                            expected_balances[tx[2]] -= tx[4]
                        else:
                            good += 1
                    return (good, bad)

                good, bad = revert_txs()
                if expected_balances == get_balances():
                    # reverting helped
                    print("REVERTING HELPED, TX EXECUTED: %s, TX LOST: %s" %
                          (good, bad))
                    bad_ratio = bad / (good + bad)
                    if bad_ratio > rolling_tolerance:
                        rolling_tolerance -= bad_ratio - rolling_tolerance
                        if rolling_tolerance < 0:
                            assert False
                    else:
                        rolling_tolerance = tx_tolerance

                    min_balances = [x - MAX_STAKE for x in expected_balances]
                    tx_count = 0
                    mode = 0
                    last_tx_set = []
                else:
                    # still no match, fail
                    print(
                        "REVERTING DIDN'T HELP, TX EXECUTED: %s, TX LOST: %s" %
                        (good, bad))
                    for step in range(
                            10
                    ):  # trace balances for 20 seconds to see if they are catching up
                        print(get_balances())
                        time.sleep(2)
                    expected_balances = snapshot_expected_balances
                    good, bad = revert_txs()
                    print(
                        "The latest and greatest stats on successful/failed: %s/%s"
                        % (good, bad))
                    assert False, "Balances didn't update in time. Expected: %s, received: %s" % (
                        expected_balances, get_balances())
            last_iter_switch = time.time()

        if mode == 0:
            # do not send more than 50 txs, so that at the end of the test we have time to query all of them. When #2195 is fixed, this condition can probably be safely removed
            if tx_count < 50:
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
                    tx = sign_payment_tx(nodes[from_].signer_key, 'test%s' % to,
                                         amt, nonce_val.value,
                                         base58.b58decode(hash_.encode('utf8')))
                    for validator_id in validator_ids:
                        try:
                            tx_hash = nodes[validator_id].send_tx(tx)['result']
                        except (requests.exceptions.ReadTimeout,
                                requests.exceptions.ConnectionError):
                            if not network_issues_expected and not nodes[
                                    validator_id].mess_with:
                                raise

                    last_tx_set.append((tx, from_, to, tx_hash, amt))
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
                rolling_tolerance = tx_tolerance
                last_tx_set = []

        if mode == 1:
            time.sleep(1)
        elif mode == 0:
            time.sleep(0.1)


def get_the_guy_to_mess_up_with(nodes):
    _, height = get_recent_hash(nodes[-1])
    return (height // EPOCH_LENGTH) % (len(nodes) - 1)


@stress_process
def monkey_staking(stopped, error, nodes, nonces):
    while stopped.value == 0:
        validator_ids = get_validator_ids(nodes)
        whom = random.randint(0, len(nonces) - 2)

        status = nodes[-1].get_status()
        hash_, _ = get_recent_hash(nodes[-1])

        who_can_unstake = get_the_guy_to_mess_up_with(nodes)

        nonce_val, nonce_lock = nonces[whom]
        with nonce_lock:
            stake = random.randint(0.7 * MAX_STAKE // 1000000,
                                   MAX_STAKE // 1000000) * 1000000

            if whom == who_can_unstake:
                stake = 0

            tx = sign_staking_tx(nodes[whom].signer_key,
                                 nodes[whom].validator_key, stake,
                                 nonce_val.value,
                                 base58.b58decode(hash_.encode('utf8')))
            for validator_id in validator_ids:
                try:
                    nodes[validator_id].send_tx(tx)
                except (requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectionError):
                    if not network_issues_expected and not nodes[
                            validator_id].mess_with:
                        raise
            nonce_val.value = nonce_val.value + 1

        time.sleep(1)


@stress_process
def blocks_tracker(stopped, error, nodes, nonces):
    # note that we do not do `white stopped.value == 0`. When the test finishes, we want
    # to wait for at least one more block to be produced
    mapping = {}
    height_to_hash = {}
    largest_height = 0
    largest_per_node = [0 for _ in nodes]
    largest_divergence = 0
    last_updated = time.time()
    done = False
    every_ten = False
    last_validators = None
    while not done:
        # always query the last validator, and a random one
        for val_id in [-1, random.randint(0, len(nodes) - 2)]:
            try:
                status = nodes[val_id].get_status()
                if status['validators'] != last_validators and val_id == -1:
                    last_validators = status['validators']
                    print(
                        "VALIDATORS TRACKER: validators set changed, new set: %s"
                        % [x['account_id'] for x in last_validators])
                hash_ = status['sync_info']['latest_block_hash']
                height = status['sync_info']['latest_block_height']
                largest_per_node[val_id] = height
                if height > largest_height:
                    if stopped.value != 0:
                        done = True
                    if not every_ten or largest_height % 10 == 0:
                        print("BLOCK TRACKER: new height %s" % largest_height)
                    if largest_height >= 20:
                        if not every_ten:
                            every_ten = True
                            print(
                                "BLOCK TRACKER: switching to tracing every ten blocks to reduce spam"
                            )
                    largest_height = height
                    last_updated = time.time()

                elif time.time() - last_updated > block_timeout:
                    assert False, "Block production took more than %s seconds" % block_timeout

                if hash_ not in mapping:
                    block_info = nodes[val_id].json_rpc('block', [hash_])
                    confirm_height = block_info['result']['header']['height']
                    assert height == confirm_height
                    prev_hash = block_info['result']['header']['prev_hash']
                    if height in height_to_hash:
                        assert False, "Two blocks for the same height: %s and %s" % (
                            height_to_hash[height], hash_)

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
    print("Per node: %s" % largest_per_node)

    if not network_issues_expected:
        assert largest_divergence < len(nodes)
    else:
        assert largest_divergence < 2 * len(nodes)


def doit(s, n, N, k, monkeys, timeout):
    global block_timeout, balances_timeout, tx_tolerance

    assert 2 <= n <= N

    config = load_config()
    local_config_changes = {}

    for i in range(N, N + k + 1):
        # make all the observers track all the shards
        local_config_changes[i] = {"tracked_shards": list(range(s))}

    near_root, node_dirs = init_cluster(
        N, k + 1, s, config,
        [["min_gas_price", 0], ["max_inflation_rate", [0, 1]],
         ["epoch_length", EPOCH_LENGTH],
         ["block_producer_kickout_threshold", 70]], local_config_changes)

    started = time.time()

    boot_node = spin_up_node(config, near_root, node_dirs[0], 0, None, None)
    boot_node.mess_with = False
    nodes = [boot_node]

    for i in range(1, N + k + 1):
        node = spin_up_node(config, near_root, node_dirs[i], i,
                            boot_node.node_key.pk, boot_node.addr())
        nodes.append(node)
        if i >= n and i < N:
            node.kill()
            node.mess_with = True
        else:
            node.mess_with = False

    monkey_names = [x.__name__ for x in monkeys]
    print(monkey_names)
    if 'monkey_local_network' in monkey_names or 'monkey_global_network' in monkey_names:
        print(
            "There are monkeys messing up with network, initializing the infra")
        if config['local']:
            init_network_pillager()
            expect_network_issues()
        block_timeout += 40
        tx_tolerance += 0.3
    if 'monkey_node_restart' in monkey_names:
        expect_network_issues()
    if 'monkey_node_restart' in monkey_names or 'monkey_node_set' in monkey_names:
        block_timeout += 40
        balances_timeout += 10
        tx_tolerance += 0.5

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
        launch_process(monkey)

    launch_process(blocks_tracker)

    started = time.time()
    while time.time() - started < timeout:
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
            assert False, "The test didn't gracefully shut down in time\nStill running: %s" % (
                still_running)

    check_errors()


MONKEYS = dict([(name[7:], obj)
                for name, obj in inspect.getmembers(sys.modules[__name__])
                if inspect.isfunction(obj) and name.startswith("monkey_")])

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(
            "Usage:\npython tests/stress/stress.py s n N k monkey1 monkey2 ...")
        sys.exit(1)

    s = int(sys.argv[1])
    n = int(sys.argv[2])
    N = int(sys.argv[3])
    k = int(sys.argv[4])
    monkeys = sys.argv[5:]

    assert len(monkeys) == len(set(monkeys)), "Monkeys should be unique"
    for monkey in monkeys:
        assert monkey in MONKEYS, "Unknown monkey \"%s\"" % monkey

    doit(s, n, N, k, [globals()["monkey_%s" % x] for x in monkeys], TIMEOUT)
