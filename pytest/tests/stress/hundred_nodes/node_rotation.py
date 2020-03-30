import sys
from enum import Enum
import random
import base58
import time
import os

sys.path.append('lib')
from cluster import collect_gcloud_config, GCloudNode, Key
from transaction import sign_staking_tx
from utils import user_name

def stop_node(machine):
    machine.run('tmux send-keys -t python-rc C-c')
    machine.kill_detach_tmux()
    print(f'{machine} killed')

def start_node(machine):
    machine.run_detach_tmux(
        'cd nearcore && export RUST_LOG=diagnostic=trace && export RUST_BACKTRACE=1 && target/release/near run')
    print(f'{machine} started')


class NodeState(Enum):
    NOTRUNNING = 1
    SYNCING = 2
    # running but have not staked
    NONVALIDATING = 3
    # staked but not validating
    STAKED = 4
    # validating
    VALIDATING = 5
    # unstaked but still validating
    UNSTAKED = 6

    def __repr__(self):
        return str(self.name).split('.')[-1].lower()


class RemoteNode(GCloudNode):
    def __init__(self, instance_name, node_dir):
        super().__init__(instance_name)
        self.validator_key = Key.from_json_file(os.path.join(node_dir, "validator_key.json"))
        self.node_key = Key.from_json_file(os.path.join(node_dir, "node_key.json"))
        self.signer_key = Key.from_json_file(os.path.join(node_dir, "signer0_key.json"))
        self.account_key_nonce = super().get_nonce_for_pk(self.signer_key.account_id, self.signer_key.pk)

        status = super().get_status()
        if 'error' in status or status is None:
            self.state = NodeState.NOTRUNNING
        else:
            validators = set(map(lambda x: x['account_id'], status['validators']))
            if self.signer_key.account_id in validators:
                self.state = NodeState.VALIDATING
            elif status['sync_info']['syncing']:
                self.state = NodeState.SYNCING
            else:
                self.state = NodeState.NONVALIDATING

    def send_staking_tx(self, stake):
        status = self.get_status()
        hash_ = status['sync_info']['latest_block_hash']
        self.account_key_nonce += 1
        tx = sign_staking_tx(nodes[index].signer_key, nodes[index].validator_key, stake, self.account_key_nonce, base58.b58decode(hash_.encode('utf8')))
        res = self.send_tx_and_wait(tx, timeout=15)
        assert 'error' not in res, f'node: {node.signer_key} result: {res}'

    def send_unstaking_tx(self):
        self.send_staking_tx(0)

    def change_state(self, cur_validators):
        if self.state is NodeState.NOTRUNNING:
            start_node(self.machine)
            self.state = NodeState.SYNCING
        elif self.state is NodeState.SYNCING:
            node_status = self.get_status()
            if not node_status['sync_info']['syncing']:
                self.state = NodeState.NONVALIDATING
        elif self.state is NodeState.NONVALIDATING:
            shutdown = bool(random.getrandbits(1))
            if shutdown:
                stop_node(self.machine)
                self.state = NodeState.NOTRUNNING
            else:
                stake = int(list(cur_validators.values())[0])
                self.send_staking_tx(stake)
                self.state = NodeState.STAKED
        elif self.state is NodeState.STAKED:
            if f'node{index}' in cur_validators:
                self.state = NodeState.VALIDATING
        elif self.state is NodeState.VALIDATING:
            self.send_unstaking_tx()
            self.state = NodeState.UNSTAKED
        elif self.state is NodeState.UNSTAKED:
            if f'node{index}' not in cur_validators:
                self.state = NodeState.NONVALIDATING
        else:
            assert False, "unexpected state"

num_nodes = 100
collect_gcloud_config(num_nodes)
nodes = [RemoteNode(f'pytest-node-{user_name()}-{i}', f'/tmp/near/node{i}') for i in range(num_nodes)]

while True:
    # find a node that is not syncing and get validator information
    validator_info = None
    for i in range(num_nodes):
        status = nodes[i].get_status()
        if not status['sync_info']['syncing']:
            validator_info = nodes[i].get_validators()
            break
    if validator_info is None:
        assert False, "all nodes are syncing"
    assert 'error' not in validator_info, validator_info
    cur_validators = dict(map(lambda x: (x['account_id'], x['stake']), validator_info['result']['current_validators']))
    print(f'current validators: {cur_validators}')

    # choose 5 nodes and change their state
    node_indices = random.sample(range(100), 5)
    for index in node_indices:
        cur_state = nodes[index].state
        nodes[index].change_state(cur_validators)
        print(f'node {index} changed its state from {cur_state} to {nodes[index].state}')

    print(dict(enumerate(map(lambda x: x.state, nodes))))
    time.sleep(600)





