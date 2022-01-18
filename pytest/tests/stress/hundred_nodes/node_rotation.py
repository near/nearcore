#!/usr/bin/env python3
import base58
from enum import Enum
import os
import pathlib
import random
import sys
import tempfile
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from cluster import GCloudNode, Key
from configured_logger import logger
from transaction import sign_staking_tx
from utils import user_name, collect_gcloud_config


def stop_node(machine):
    machine.run('tmux send-keys -t python-rc C-c')
    machine.kill_detach_tmux()
    logger.info(f'{machine} killed')


def start_node(machine):
    machine.run_detach_tmux(
        'cd nearcore && export RUST_LOG=diagnostic=trace && export RUST_BACKTRACE=1 && target/release/near run'
    )
    logger.info(f'{machine} started')


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
        self.validator_key = Key.from_json_file(node_dir / 'validator_key.json')
        self.node_key = Key.from_json_file(node_dir / 'node_key.json')
        self.signer_key = Key.from_json_file(node_dir / 'signer0_key.json')
        self.last_synced_height = 0

        try:
            status = super().get_status()
            validators = set(
                map(lambda x: x['account_id'], status['validators']))
            if self.signer_key.account_id in validators:
                self.state = NodeState.VALIDATING
            elif status['sync_info']['syncing']:
                self.state = NodeState.SYNCING
            else:
                self.state = NodeState.NONVALIDATING
            self.account_key_nonce = super().get_nonce_for_pk(
                self.signer_key.account_id, self.signer_key.pk)
        except Exception:
            start_node(self.machine)
            time.sleep(20)
            self.state = NodeState.SYNCING
            self.account_key_nonce = None

    def send_staking_tx(self, stake):
        hash_ = self.get_latest_block().hash_bytes
        if self.account_key_nonce is None:
            self.account_key_nonce = self.get_nonce_for_pk(
                self.signer_key.account_id, self.signer_key.pk)
        self.account_key_nonce += 1
        tx = sign_staking_tx(nodes[index].signer_key,
                             nodes[index].validator_key, stake,
                             self.account_key_nonce, hash_)
        logger.info(f'{self.signer_key.account_id} stakes {stake}')
        res = self.send_tx_and_wait(tx, timeout=15)
        if 'error' in res or 'Failure' in res['result']['status']:
            logger.info(res)

    def send_unstaking_tx(self):
        self.send_staking_tx(0)

    def change_state(self, cur_validators):
        if self.state is NodeState.NOTRUNNING:
            if bool(random.getrandbits(1)):
                start_node(self.machine)
                self.state = NodeState.SYNCING
        elif self.state is NodeState.SYNCING:
            node_status = self.get_status()
            if not node_status['sync_info']['syncing']:
                self.state = NodeState.NONVALIDATING
            else:
                cur_height = node_status['sync_info']['latest_block_height']
                assert cur_height > self.last_synced_height + 10, f'current height {cur_height} did not change much from last synced height: {self.last_synced_height}'
                self.last_synced_height = cur_height
        elif self.state is NodeState.NONVALIDATING:
            if bool(random.getrandbits(1)):
                stop_node(self.machine)
                self.state = NodeState.NOTRUNNING
            else:
                stake = int(list(cur_validators.values())[0])
                self.send_staking_tx(stake)
                self.state = NodeState.STAKED
        elif self.state is NodeState.STAKED:
            if self.signer_key.account_id in cur_validators:
                self.state = NodeState.VALIDATING
        elif self.state is NodeState.VALIDATING:
            assert self.signer_key.account_id in cur_validators, f'invariant failed: {self.signer_key.account_id} not in {cur_validators}'
            if bool(random.getrandbits(1)):
                self.send_unstaking_tx()
                self.state = NodeState.UNSTAKED
        elif self.state is NodeState.UNSTAKED:
            if self.signer_key.account_id not in cur_validators:
                self.state = NodeState.NONVALIDATING
        else:
            assert False, "unexpected state"


num_nodes = 100
collect_gcloud_config(num_nodes)
nodes = [
    RemoteNode(f'pytest-node-{user_name()}-{i}',
               pathlib.Path(tempfile.gettempdir()) / 'near' / f'node{i}')
    for i in range(num_nodes)
]

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
    cur_validators = dict(
        map(lambda x: (x['account_id'], x['stake']),
            validator_info['result']['current_validators']))
    prev_epoch_kickout = validator_info['result']['prev_epoch_kickout']
    logger.info(
        f'validators kicked out in the previous epoch: {prev_epoch_kickout}')
    for validator_kickout in prev_epoch_kickout:
        assert validator_kickout['reason'] != 'Unstaked' or validator_kickout[
            'reason'] != 'DidNotGetASeat' or not validator_kickout.startswith(
                'NotEnoughStake'), validator_kickout
    logger.info(f'current validators: {cur_validators}')

    # choose 5 nodes and change their state
    node_indices = random.sample(range(100), 5)
    for index in node_indices:
        cur_state = nodes[index].state
        nodes[index].change_state(cur_validators)
        logger.info(
            f'node {index} changed its state from {cur_state} to {nodes[index].state}'
        )

    logger.info(dict(enumerate(map(lambda x: x.state, nodes))))
    time.sleep(600)
