#!/usr/bin/env python3
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
#  [v] `packets_drop`: drop 10% of all the network packets
#  [ ] `global_network`: ocasionally shots down the network globally for several seconds
#  [v] `transactions`: sends random transactions keeping track of expected balances
#  [v] `staking`: runs staking transactions for validators. Presently the test doesn't track staking invariants, relying on asserts in the nearcore.
#                `staking2.py` tests some basic stake invariants
#  [v] `wipe_data`: only used in conjunction with `node_set` and `node_restart`. If present, nodes data folders will be periodically cleaned on restart
# This test also completely disables rewards, which simplifies ensuring total supply invariance and balance invariances

import sys, time, base58, random, inspect, traceback, requests, logging
import pathlib
from multiprocessing import Process, Value, Lock

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import init_cluster, spin_up_node, load_config
from configured_logger import logger
from transaction import sign_payment_tx, sign_staking_tx
from proxy_instances import RejectListProxy

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)

TIMEOUT = 1500  # after how much time to shut down the test
TIMEOUT_SHUTDOWN = 120  # time to wait after the shutdown was initiated before failing the test due to process stalling
MAX_STAKE = int(1e32)

# How many times to try to send transactions to each validator.
# Is only applicable in the scenarios where we expect failures in tx sends.
SEND_TX_ATTEMPTS = 10

# Block_header_fetch_horizon + state_fetch_horizon (which is equalto 5) need to be shorter than the epoch length.
# otherwise say epoch boundaries are H and H'. If the current height is H' + eps, and a node finished header sync at
# H' + eps - block_header_fetch_horizon, and then rolled state_fetch_horizon back, it will end up before H, and will
# try to state sync at the beginning of the epoch *two* epochs ago. No node will respond to such state requests.
BLOCK_HEADER_FETCH_HORIZON = 15

epoch_length = 50
block_timeout = 25  # if two blocks are not produced within that many seconds, the test will fail. The timeout is increased if nodes are restarted or network is being messed up with
balances_timeout = 20  # how long to tolerate for balances to update after txs are sent
restart_sync_timeout = 45  # for how long to wait for nodes to sync in `node_restart`
tx_tolerance = 0.1
wait_if_restart = False  # whether to wait between `kill` and `start`, is needed when nodes are proxied
wipe_data = False

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
        except Exception as ex:
            logger.info(f'Process {func.__name__} failed', exc_info=ex)
            error.value = 1

    wrapper.__name__ = func.__name__
    return wrapper


def get_recent_hash(node, sync_timeout):
    # return the parent of the last block known to the observer
    # don't return the last block itself, since some validators might have not seen it yet
    # also returns the height of the actual last block (so the height doesn't match the hash!)

    for attempt in range(sync_timeout):
        # use timeout=10 throughout, because during header sync processing headers takes up to 3-10s
        sync_info = node.get_status(timeout=10)['sync_info']
        block_hash = sync_info['latest_block_hash']
        info = node.json_rpc('block', [block_hash], timeout=10)
        sync_error = ('error' in info and
                      'unavailable on the node' in info['error']['data'])
        if not sync_info['syncing'] and not sync_error:
            break
        time.sleep(1)
    else:
        assert False, "Node hasn't synced in %s seconds" % sync_timeout

    assert 'result' in info, info
    hash_ = info['result']['header']['last_final_block']
    return hash_, sync_info['latest_block_height']


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
                logging.info("Node set: starting node %s" % i)
                # figuring out a live node with `node_restart` monkey is not trivial
                # for simplicity just boot from the observer node
                # `node_restart` doesn't boot from the observer, increasing coverage
                boot_node = nodes[-1]
                node.start(boot_node=boot_node)
            else:
                node.kill()
                wipe = False
                if wipe_data and random.choice([True, False]):
                    wipe = True
                    node.reset_data()
                logging.info("Node set: stopping%s node %s" %
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
            _, h = get_recent_hash(node, restart_sync_timeout)
            assert h >= heights_after_restart[node_idx], "%s > %s" % (
                h, heights_after_restart[node_idx])
            if h > heights_after_restart[node_idx] + (5
                                                      if not wipe_data else 10):
                break
            time.sleep(1)

        reset_data = wipe_data and random.choice([True, False, False])
        logging.info(
            "NUKING NODE %s%s" %
            (node_idx, " AND WIPING ITS STORAGE" if reset_data else ""))

        node.kill()
        if reset_data:
            node.reset_data()

        if wait_if_restart:
            time.sleep(7)
        node.start(boot_node=boot_node)
        logging.info("NODE %s IS BACK UP" % node_idx)

        _, new_height = get_recent_hash(node, restart_sync_timeout)
        assert new_height >= heights_after_restart[node_idx]
        heights_after_restart[node_idx] = new_height

        time.sleep(5)


@stress_process
def monkey_packets_drop(stopped, error, nodes, nonces):
    # no implementation needed, packet drop is configured on start
    pass


@stress_process
def monkey_local_network(stopped, error, nodes, nonces):
    last_height = 0
    last_time_height_updated = time.time()

    while stopped.value == 0:
        _, cur_height = get_recent_hash(nodes[-1], 15)
        if cur_height == last_height and time.time(
        ) - last_time_height_updated > 10:
            time.sleep(25)
        else:
            last_height = cur_height
            last_time_height_updated = time.time()
            time.sleep(5)

        # "- 2" below is because we don't want to kill the node we use to check stats
        node_idx = random.randint(0, len(nodes) - 2)
        node = nodes[node_idx]
        logging.info(f'ISOLATING NODE {node_idx}')
        node.proxy.reject_list[0] = node_idx
        if node_idx == get_the_guy_to_mess_up_with(nodes):
            time.sleep(5)
        else:
            time.sleep(1)
        logging.info(f'RESTORING NODE {node_idx} NETWORK')
        node.proxy.reject_list[0] = -1


@stress_process
def monkey_wipe_data(stopped, error, nodes, nonces):
    # no implementation needed, wipe_data is implemented inside node_set and node_restart
    pass


@stress_process
def monkey_transactions(stopped, error, nodes, nonces):

    def get_balances():
        acts = [
            nodes[-1].get_account("test%s" % i, timeout=10)['result']
            for i in range(len(nodes))
        ]
        return [int(x['amount']) + int(x['locked']) for x in acts]

    expected_balances = get_balances()
    min_balances = [x - MAX_STAKE for x in expected_balances]
    total_supply = (sum(expected_balances))
    logging.info("TOTAL SUPPLY: %s" % total_supply)

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
                logging.info("%s TRANSACTIONS SENT. WAITING FOR BALANCES" %
                             tx_count)
                mode = 1
            else:
                logging.info(
                    "BALANCES NEVER CAUGHT UP, CHECKING UNFINISHED TRANSACTIONS"
                )

                def revert_txs():
                    nonlocal expected_balances
                    good = 0
                    bad = 0
                    for tx in last_tx_set:
                        tx_happened = True

                        response = nodes[-1].json_rpc(
                            'tx', [tx[3], "test%s" % tx[1]], timeout=10)

                        if 'error' in response and 'data' in response[
                                'error'] and "doesn't exist" in response[
                                    'error']['data']:
                            tx_happened = False
                        elif 'result' in response and 'receipts_outcome' in response[
                                'result']:
                            tx_happened = len(
                                response['result']['receipts_outcome']) > 0
                        else:
                            assert False, response

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
                    logging.info(
                        "REVERTING HELPED, TX EXECUTED: %s, TX LOST: %s" %
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
                    logging.info(
                        "REVERTING DIDN'T HELP, TX EXECUTED: %s, TX LOST: %s" %
                        (good, bad))

                    assert False, "Balances didn't update in time. Expected: %s, received: %s" % (
                        expected_balances, get_balances())
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

            hash_, _ = get_recent_hash(nodes[-1], 15)

            with nonce_lock:
                tx = sign_payment_tx(nodes[from_].signer_key, 'test%s' % to,
                                     amt, nonce_val.value,
                                     base58.b58decode(hash_.encode('utf8')))

                # Loop trying to send the tx to all the validators, until at least one receives it
                tx_hash = None
                for send_attempt in range(SEND_TX_ATTEMPTS):
                    shuffled_validator_ids = [x for x in validator_ids]
                    random.shuffle(shuffled_validator_ids)
                    for validator_id in shuffled_validator_ids:
                        try:
                            info = nodes[validator_id].send_tx(tx)
                            if 'error' in info:
                                pass

                            elif 'result' in info:
                                tx_hash = info['result']
                                break

                            else:
                                assert False, info

                        except (requests.exceptions.ReadTimeout,
                                requests.exceptions.ConnectionError):
                            if not network_issues_expected and not nodes[
                                    validator_id].mess_with:
                                raise

                    if tx_hash is not None:
                        break

                    time.sleep(1)

                else:
                    assert False, "Failed to send the transation after %s attempts" % SEND_TX_ATTEMPTS

                last_tx_set.append((tx, from_, to, tx_hash, amt))
                nonce_val.value = nonce_val.value + 1

            expected_balances[from_] -= amt
            expected_balances[to] += amt
            min_balances[from_] -= amt

            tx_count += 1

        else:
            if get_balances() == expected_balances:
                logging.info("BALANCES CAUGHT UP, BACK TO SPAMMING TXS")
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
    global epoch_length
    _, height = get_recent_hash(nodes[-1], 15)
    return (height // epoch_length) % (len(nodes) - 1)


@stress_process
def monkey_staking(stopped, error, nodes, nonces):
    while stopped.value == 0:
        validator_ids = get_validator_ids(nodes)
        whom = random.randint(0, len(nonces) - 2)

        status = nodes[-1].get_status()
        hash_, _ = get_recent_hash(nodes[-1], 15)

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
                    logging.info(
                        "VALIDATORS TRACKER: validators set changed, new set: %s"
                        % [x['account_id'] for x in last_validators])
                hash_ = status['sync_info']['latest_block_hash']
                height = status['sync_info']['latest_block_height']
                largest_per_node[val_id] = height
                if height > largest_height:
                    if stopped.value != 0:
                        done = True
                    if not every_ten or largest_height % 10 == 0:
                        logging.info("BLOCK TRACKER: new height %s" %
                                     largest_height)
                    if largest_height >= 20:
                        if not every_ten:
                            every_ten = True
                            logging.info(
                                "BLOCK TRACKER: switching to tracing every ten blocks to reduce spam"
                            )
                    largest_height = height
                    last_updated = time.time()

                elif time.time() - last_updated > block_timeout:
                    assert False, "Block production took more than %s seconds" % block_timeout

                if hash_ not in mapping:
                    block_info = nodes[val_id].json_rpc('block', [hash_],
                                                        timeout=10)
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

    logging.info("=== BLOCK TRACKER SUMMARY ===")
    logging.info("Largest height:     %s" % largest_height)
    logging.info("Largest divergence: %s" % largest_divergence)
    logging.info("Per node: %s" % largest_per_node)

    if not network_issues_expected:
        assert largest_divergence < len(nodes)
    else:
        assert largest_divergence < 2 * len(nodes)


def doit(s, n, N, k, monkeys, timeout):
    global block_timeout, balances_timeout, tx_tolerance, epoch_length, wait_if_restart, wipe_data, restart_sync_timeout

    assert 2 <= n <= N

    config = load_config()
    local_config_changes = {}

    monkey_names = [x.__name__ for x in monkeys]
    proxy = None
    logging.info(monkey_names)

    for i in range(N + k + 1):
        local_config_changes[i] = {
            "consensus": {
                "block_header_fetch_horizon": BLOCK_HEADER_FETCH_HORIZON,
                "state_sync_timeout": {
                    "secs": 0,
                    "nanos": 500000000
                }
            },
            "state_sync_enabled": True,
            "view_client_throttle_period": {
                "secs": 0,
                "nanos": 0
            },
            "store.state_snapshot_enabled": True,
        }
    for i in range(N, N + k + 1):
        # make all the observers track all the shards
        local_config_changes[i]["tracked_shards"] = list(range(s))
    if 'monkey_wipe_data' in monkey_names:
        # When data can be deleted, with the short epoch length while the node with deleted data folder is syncing,
        # other nodes can run sufficiently far ahead to GC the old data. Have one archival node to address it.
        # It is also needed, because the balances timeout is longer, and the txs can get GCed on the observer node
        # by the time it gets to checking their status.
        local_config_changes[N + k]['archive'] = True

    if 'monkey_local_network' in monkey_names or 'monkey_packets_drop' in monkey_names or 'monkey_node_restart' in monkey_names:
        expect_network_issues()
        block_timeout += 40

    if 'monkey_local_network' in monkey_names or 'monkey_packets_drop' in monkey_names:
        assert config[
            'local'], 'Network stress operations only work on local nodes'
        drop_probability = 0.05 if 'monkey_packets_drop' in monkey_names else 0

        reject_list = RejectListProxy.create_reject_list(1)
        proxy = RejectListProxy(reject_list, drop_probability)
        tx_tolerance += 0.3

    if 'monkey_local_network' in monkey_names or 'monkey_packets_drop' in monkey_names:
        # add 15 seconds + 10 seconds for each unique network-related monkey
        balances_timeout += 15

        if 'monkey_local_network' in monkey_names:
            balances_timeout += 10

        if 'monkey_packets_drop' in monkey_names:
            wait_if_restart = True
            balances_timeout += 10

    if 'monkey_node_restart' in monkey_names or 'monkey_node_set' in monkey_names:
        balances_timeout += 10
        tx_tolerance += 0.5

    if 'monkey_wipe_data' in monkey_names:
        assert 'monkey_node_restart' in monkey_names or 'monkey_node_set' in monkey_names
        wipe_data = True
        balances_timeout += 25

        # if nodes can restart, we should give them way more time to sync.
        # if packets can also be dropped, each state-sync-related request or response lost adds 10 seconds
        # to the sync process.
        restart_sync_timeout = 45 if 'monkey_packets_drop' not in monkey_names else 90
        block_timeout += (10
                          if 'monkey_packets_drop' not in monkey_names else 40)

    # We need to make sure that the blocks that include txs are not garbage collected. From the first tx sent until
    # we check balances time equal to `balances_timeout * 2` passes, and the block production is capped at 1.7/s.
    # The GC keeps five epochs of blocks.
    min_epoch_length = (int((balances_timeout * 2) * 1.7) + 4) // 5
    epoch_length = max(epoch_length, min_epoch_length)

    logger.info(
        f"block_timeout: {block_timeout}, balances_timeout: {balances_timeout}, tx_tolerance: {tx_tolerance}, epoch_length: {epoch_length}, wait_if_restart: {wait_if_restart}, wipe_data: {wipe_data}, restart_sync_timeout: {restart_sync_timeout}"
    )

    near_root, node_dirs = init_cluster(
        N, k + 1, s, config,
        [["min_gas_price", 0], ["max_inflation_rate", [0, 1]],
         ["epoch_length", epoch_length], [
             "block_producer_kickout_threshold", 10
         ], ["chunk_producer_kickout_threshold", 10]], local_config_changes)

    started = time.time()

    boot_node = spin_up_node(config, near_root, node_dirs[0], 0, proxy=proxy)
    boot_node.stop_checking_store()
    boot_node.mess_with = False
    nodes = [boot_node]

    for i in range(1, N + k + 1):
        node = spin_up_node(config,
                            near_root,
                            node_dirs[i],
                            i,
                            boot_node=boot_node,
                            proxy=proxy)
        node.stop_checking_store()
        nodes.append(node)
        if i >= n and i < N:
            node.kill()
            node.mess_with = True
        else:
            node.mess_with = False

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

    logging.info("")
    logging.info("==========================================")
    logging.info("# TIMEOUT IS HIT, SHUTTING DOWN THE TEST #")
    logging.info("==========================================")
    stopped.value = 1
    started_shutdown = time.time()
    proxies_stopped = False

    while True:
        check_errors()
        still_running = [name for (p, name) in ps if p.is_alive()]

        if len(still_running) == 0:
            break

        # If the test is running with proxies, `node_restart` and `node_set` can get
        # stuck because the proxies now are their child processes. We can't kill the
        # proxies rigth away, because that would interfere with block production, and
        # might prevent other workers (e.g. block_tracker) from completing in a timely
        # manner. Thus, kill the proxies some time into the shut down process.
        if time.time(
        ) - started_shutdown > TIMEOUT_SHUTDOWN / 2 and not proxies_stopped:
            logging.info(
                "Shutdown is %s seconds in, shutting down proxies if any" %
                (TIMEOUT_SHUTDOWN / 2))
            if boot_node.proxy is not None:
                boot_node.proxy.global_stopped.value = 1
                for p in boot_node.proxy.ps:
                    p.terminate()
            proxies_stopped = True

        if time.time() - started_shutdown > TIMEOUT_SHUTDOWN:
            for (p, _) in ps:
                p.terminate()
            assert False, "The test didn't gracefully shut down in time\nStill running: %s" % (
                still_running)

    check_errors()

    logging.info("Shut down complete, executing store validity checks")
    for node in nodes:
        node.is_check_store = True
        node.check_store()


MONKEYS = dict([(name[7:], obj)
                for name, obj in inspect.getmembers(sys.modules[__name__])
                if inspect.isfunction(obj) and name.startswith("monkey_")])

if __name__ == "__main__":
    if len(sys.argv) < 5:
        logger.info(
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
