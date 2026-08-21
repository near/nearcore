#!/usr/bin/env python3
# Tests `neard mirror` using the fork-network workflow.
# Builds two images (target=forked state, source=source chain), starts 4
# target validators, runs mirror to replay source txs, then validates
# via per-tx-type test case hooks.
# cspell:ignore bhash

import base58
import json
import os
import pathlib
import shutil
import sys
import time

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from cluster import spin_up_node, init_cluster, load_config
from configured_logger import logger
import key
import mldsa65
import transaction
import utils

import mirror_utils

MIRROR_DIR = pathlib.Path.home() / '.near' / 'test-mirror'
TARGET_VALIDATORS = mirror_utils.TARGET_VALIDATORS


class TestContext:
    """Shared mutable state passed to all test case hooks."""

    def __init__(self, node, signer_key, nonce):
        self.node = node
        self.signer_key = signer_key
        self.nonce = nonce
        self.bhash = None

    def next_nonce(self):
        n = self.nonce
        self.nonce += 1
        return n


class MirrorTestCase:
    """
    Base class for per-tx-type test cases.

    Hooks:
      pre_fork(ctx)            — called once during pre-fork. Returns senders.
      check_fork(node)         — called once on a node booted from forked state.
      post_fork(ctx)           — called once during post-fork. Returns senders.
      on_post_fork_block(ctx)  — called each block during post-fork traffic.
      check(node)              — called once on the target chain for validation.

    Senders are objects with send_if_inited(node, transfers, block_hash).
    The orchestrator drives traffic from returned senders each block.
    """
    name = 'base'

    def pre_fork(self, ctx):
        return []

    def check_fork(self, node):
        pass

    def post_fork(self, ctx):
        return []

    def on_post_fork_block(self, ctx):
        pass

    def check(self, node):
        pass


class ImplicitAccountTest(MirrorTestCase):
    name = 'implicit_account'

    def __init__(self):
        self.implicit1 = mirror_utils.ImplicitAccount()
        self.implicit2 = mirror_utils.ImplicitAccount()

    def pre_fork(self, ctx):
        self.implicit1.transfer_to(ctx.node, ctx.signer_key, 10**24, ctx.bhash,
                                   ctx.next_nonce())
        return [self.implicit1]

    def post_fork(self, ctx):
        # Send 1 yocto (fails to create), then enough to actually create
        self.implicit2.transfer_to(ctx.node, ctx.signer_key, 1, ctx.bhash,
                                   ctx.next_nonce())
        time.sleep(2)
        self.implicit2.transfer_to(ctx.node, ctx.signer_key, 10**24, ctx.bhash,
                                   ctx.next_nonce())
        return [self.implicit1, self.implicit2]

    def check(self, node):
        mapped_id = mirror_utils.map_account_no_secret(
            self.implicit2.account_id())
        res = node.get_account(mapped_id, do_assert=False)
        assert 'error' not in res, \
            f'implicit account {mapped_id} not found on target'
        logger.info(f'{self.name}: implicit account verified')


class AddKeyTest(MirrorTestCase):
    name = 'add_key'

    def __init__(self):
        self.new_key = None

    def post_fork(self, ctx):
        k = key.Key.from_random('test0')
        self.new_key = mirror_utils.AddedKey(k)
        mirror_utils.send_add_access_key(ctx.node, ctx.signer_key, k,
                                         ctx.next_nonce(), ctx.bhash)
        return [self.new_key]

    def check(self, node):
        mapped_pk = mirror_utils.map_key_no_secret(self.new_key.key.pk)
        nonce = node.get_nonce_for_pk('test0', mapped_pk, finality='final')
        assert nonce is not None, \
            f'added key {mapped_pk} not found on target'
        logger.info(f'{self.name}: add key check passed')


class ContractTest(MirrorTestCase):
    name = 'contract'

    def __init__(self):
        self.contract_key = None
        self.contract_extra_key = None
        self.sub_key = None

    def pre_fork(self, ctx):
        mirror_utils.deploy_addkey_contract(ctx.node, ctx.signer_key,
                                            mirror_utils.CONTRACT_PATH,
                                            ctx.next_nonce(), ctx.bhash)
        return []

    def check_fork(self, node):
        assert mirror_utils.contract_deployed(node, 'test0'), \
            'addkey contract not deployed on forked state'
        logger.info(f'{self.name}: contract deployed on forked state')

    def post_fork(self, ctx):
        contract_key = key.Key.from_random('test0')
        contract_extra_key = key.Key.from_random('test0')
        mirror_utils.call_addkey(ctx.node,
                                 ctx.signer_key,
                                 contract_key,
                                 ctx.next_nonce(),
                                 ctx.bhash,
                                 extra_actions=[
                                     transaction.create_full_access_key_action(
                                         contract_extra_key.decoded_pk())
                                 ])
        self.contract_key = mirror_utils.AddedKey(contract_key)
        self.contract_extra_key = mirror_utils.AddedKey(contract_extra_key)

        self.sub_key = mirror_utils.AddedKey(key.Key.from_random('test0.test0'))
        mirror_utils.call_create_account(ctx.node, ctx.signer_key,
                                         self.sub_key.key.account_id,
                                         self.sub_key.key.pk, ctx.next_nonce(),
                                         ctx.bhash)
        return [self.contract_key, self.contract_extra_key, self.sub_key]

    def check(self, node):
        # contract_key added by contract execution (unmapped)
        nonce = node.get_nonce_for_pk('test0',
                                      self.contract_key.key.pk,
                                      finality='final')
        assert nonce is not None, \
            f'contract key {self.contract_key.key.pk} not found on target'

        # the mirror must also synthesize the mapped version of contract_key
        # from the promise-created AddKey receipt; both coexist on test0
        mapped_contract_pk = mirror_utils.map_key_no_secret(
            self.contract_key.key.pk)
        nonce = node.get_nonce_for_pk('test0',
                                      mapped_contract_pk,
                                      finality='final')
        assert nonce is not None, \
            f'mapped contract key {mapped_contract_pk} not found on target'

        # contract_extra_key is a direct AddKey action (mapped)
        mapped_pk = mirror_utils.map_key_no_secret(
            self.contract_extra_key.key.pk)
        nonce = node.get_nonce_for_pk('test0', mapped_pk, finality='final')
        assert nonce is not None, \
            f'contract extra key {mapped_pk} not found on target'

        # Sub-account created via contract. The mirror pre-creates it with the
        # mapped key via an extra CreateAccount tx, and the contract call's own
        # CreateAccount receipt then fails on target, so the unmapped key the
        # contract passed never lands there.
        res = node.get_account('test0.test0', do_assert=False)
        assert 'error' not in res, 'account test0.test0 not found on target'
        mapped_sub_pk = mirror_utils.map_key_no_secret(self.sub_key.key.pk)
        nonce = node.get_nonce_for_pk('test0.test0',
                                      mapped_sub_pk,
                                      finality='final')
        assert nonce is not None, \
            f'mapped sub key {mapped_sub_pk} not found on test0.test0'

        assert mirror_utils.contract_deployed(node, 'test0'), \
            'contract not deployed on target test0'

        logger.info(f'{self.name}: contract checks passed')


class CreateSubaccountTest(MirrorTestCase):
    name = 'create_subaccount'

    def __init__(self):
        self.subaccount_key = None
        self._contract_deployed = False
        self._staked = False

    def post_fork(self, ctx):
        k = mirror_utils.create_subaccount(ctx.node,
                                           'foo',
                                           ctx.signer_key,
                                           ctx.next_nonce(),
                                           ctx.bhash,
                                           extra_key=True)
        self.subaccount_key = mirror_utils.AddedKey(k)
        return [self.subaccount_key]

    def on_post_fork_block(self, ctx):
        if not self.subaccount_key.inited():
            return
        if not self._contract_deployed:
            self.subaccount_key.nonce += 1
            mirror_utils.deploy_addkey_contract(ctx.node,
                                                self.subaccount_key.key,
                                                mirror_utils.CONTRACT_PATH,
                                                self.subaccount_key.nonce,
                                                ctx.bhash)
            self._contract_deployed = True
        elif not self._staked and mirror_utils.contract_deployed(
                ctx.node, self.subaccount_key.account_id()):
            self.subaccount_key.nonce += 1
            mirror_utils.call_stake(ctx.node, self.subaccount_key.key, 10**28,
                                    self.subaccount_key.key.pk,
                                    self.subaccount_key.nonce, ctx.bhash)
            self._staked = True

    def check(self, node):
        res = node.get_account('foo.test0', do_assert=False)
        assert 'error' not in res, 'account foo.test0 not found on target'
        assert mirror_utils.contract_deployed(node, 'foo.test0'), \
            'contract not deployed on foo.test0 on target'

        logger.info(f'{self.name}: subaccount checks passed')


class GasKeyTest(MirrorTestCase):
    name = 'gas_key'

    def __init__(self):
        self.gas_key_1 = key.Key.from_random('test0')
        self.num_nonces_1 = 3
        self.gas_key_2 = key.Key.from_random('test0')
        self.num_nonces_2 = 2
        self.gk1_recipient = mirror_utils.ImplicitAccount()
        self.gk2_recipient = mirror_utils.ImplicitAccount()
        self._transfers_sent = False
        self._block_count = 0

    def pre_fork(self, ctx):
        mirror_utils.send_add_gas_key(ctx.node, ctx.signer_key,
                                      self.gas_key_1, self.num_nonces_1,
                                      ctx.next_nonce(), ctx.bhash)
        mirror_utils.fund_gas_key(ctx.node, ctx.signer_key,
                                  self.gas_key_1.decoded_pk(), 10**24,
                                  ctx.next_nonce(), ctx.bhash)
        return []

    def check_fork(self, node):
        mapped_pk = mirror_utils.map_key_no_secret(self.gas_key_1.pk)
        nonces = mirror_utils.get_gas_key_nonces(node, 'test0', mapped_pk)
        assert nonces is not None, \
            f'gas key nonces not found for mapped {mapped_pk} on forked state'
        assert len(nonces) == self.num_nonces_1, \
            f'expected {self.num_nonces_1} nonce indexes on forked state, ' \
            f'got {len(nonces)}'
        logger.info(
            f'{self.name}: forked state has {len(nonces)} nonces for gas_key_1')

    def post_fork(self, ctx):
        mirror_utils.send_add_gas_key(ctx.node, ctx.signer_key,
                                      self.gas_key_2, self.num_nonces_2,
                                      ctx.next_nonce(), ctx.bhash)
        mirror_utils.fund_gas_key(ctx.node, ctx.signer_key,
                                  self.gas_key_2.decoded_pk(), 10**24,
                                  ctx.next_nonce(), ctx.bhash)
        mirror_utils.withdraw_from_gas_key(ctx.node, ctx.signer_key,
                                           self.gas_key_2.decoded_pk(), 10**22,
                                           ctx.next_nonce(), ctx.bhash)
        return []

    def on_post_fork_block(self, ctx):
        # Wait for gas key add/fund txs to land before sending V1 transfers.
        self._block_count += 1
        if self._transfers_sent or self._block_count <= 10:
            return
        # Gas key V1 tx nonces are uniquely identified by the (nonce, nonce_index)
        # pair, so reusing the same nonce value across different nonce_index slots
        # is valid. The * 1000000 + 1 ensures the nonce exceeds the current
        # access key nonce so the transaction is accepted.
        for gas_key, num_nonces, recipient in [
            (self.gas_key_1, self.num_nonces_1, self.gk1_recipient),
            (self.gas_key_2, self.num_nonces_2, self.gk2_recipient),
        ]:
            for nonce_index in range(num_nonces):
                mirror_utils.send_gas_key_transfer(ctx.node, gas_key,
                                                   recipient.account_id(),
                                                   10**24,
                                                   ctx.nonce * 1000000 + 1,
                                                   nonce_index, ctx.bhash)
        self._transfers_sent = True

    def check(self, node):
        for label, gas_key, num_nonces in [
            ('gas_key_1', self.gas_key_1, self.num_nonces_1),
            ('gas_key_2', self.gas_key_2, self.num_nonces_2),
        ]:
            mapped_pk = mirror_utils.map_key_no_secret(gas_key.pk)
            nonce = node.get_nonce_for_pk('test0', mapped_pk, finality='final')
            assert nonce is not None, \
                f'{label} {mapped_pk} not found on target'
            nonces = mirror_utils.get_gas_key_nonces(node, 'test0', mapped_pk)
            assert nonces is not None, \
                f'{label} nonces not found on target'
            assert len(nonces) == num_nonces, \
                f'expected {num_nonces} nonces for {label}, got {len(nonces)}'

        # Gas key 2 balance should be reduced by withdrawal
        mapped_pk2 = mirror_utils.map_key_no_secret(self.gas_key_2.pk)
        balance = mirror_utils.get_gas_key_balance(node, 'test0', mapped_pk2)
        assert balance is not None, 'gas key 2 balance not found on target'
        upper = 10**24 - 10**22
        assert balance < upper, \
            f'gas key 2 balance {balance} not below {upper}'

        # V1 transfers created implicit accounts
        for label, recipient in [
            ('gk1_recipient', self.gk1_recipient),
            ('gk2_recipient', self.gk2_recipient),
        ]:
            mapped_id = mirror_utils.map_account_no_secret(
                recipient.account_id())
            res = node.get_account(mapped_id, do_assert=False)
            assert 'error' not in res, \
                f'{label} {mapped_id} not found on target'

        logger.info(f'{self.name}: all gas key checks passed')


class MlDsa65Test(MirrorTestCase):
    """ML-DSA-65 access and gas keys must survive the fork, remapped, and the
    mirror must replay the transactions they sign."""
    name = 'mldsa65'

    def __init__(self):
        self.access_key = mldsa65.MlDsa65Key.from_seed_testonly(
            'test0', 'mirror-pq-offline-test')
        self.gas_key = mldsa65.MlDsa65Key.from_seed_testonly(
            'test0', 'mirror-pq-offline-test-gas')
        self.gas_num_nonces = 3
        self.txs_per_key = 2
        # Added post-fork, so the mirror maps the AddKey action itself instead
        # of picking the key up from the forked state.
        self.added_key = mldsa65.MlDsa65Key.from_seed_testonly(
            'test0', 'mirror-pq-offline-test-added')
        self.access_signer = mirror_utils.AddedKey(self.access_key)
        self.added_signer = mirror_utils.AddedKey(self.added_key)
        self.access_recipient = mirror_utils.ImplicitAccount()
        self.added_recipient = mirror_utils.ImplicitAccount()
        self.gas_recipient = mirror_utils.ImplicitAccount()
        self.fork_access_nonce = None
        self.fork_gas_nonces = None
        self._sent = {'access': 0, 'added': 0, 'gas': 0}

    def signers(self):
        return [('access', self.access_signer, self.access_recipient),
                ('added', self.added_signer, self.added_recipient)]

    def pre_fork(self, ctx):
        mirror_utils.send_add_mldsa65_access_key(ctx.node, ctx.signer_key,
                                                 self.access_key,
                                                 ctx.next_nonce(), ctx.bhash)
        mirror_utils.send_add_mldsa65_gas_key(ctx.node, ctx.signer_key,
                                              self.gas_key, self.gas_num_nonces,
                                              ctx.next_nonce(), ctx.bhash)
        mirror_utils.fund_mldsa65_gas_key(ctx.node,
                                          ctx.signer_key, self.gas_key, 10**24,
                                          ctx.next_nonce(), ctx.bhash)
        return []

    def check_fork(self, node):
        keys = node.get_access_key_list('test0',
                                        finality='final')['result']['keys']
        pks = [k['public_key'] for k in keys]

        # Neither source key may survive.
        for label, mldsa_key in [('access', self.access_key),
                                 ('gas', self.gas_key)]:
            assert mldsa_key.pk not in pks, \
                f'forked state still carries source ML-DSA-65 {label} ' \
                f'key {mldsa_key.pk}'

        pq = {
            k['public_key']: k
            for k in keys
            if k['public_key'].startswith(mldsa65.HANDLE_PREFIX)
        }
        assert len(pq) == 2, \
            f'expected 2 ML-DSA-65 keys on the forked test0, got {pks}'

        access_mapped = mirror_utils.map_mldsa65_key_no_secret(self.access_key)
        access = pq.get(access_mapped.pk)
        assert access is not None, \
            f'mapped ML-DSA-65 access key {access_mapped.pk} missing, ' \
            f'got {sorted(pq)}'
        assert access['access_key']['permission'] == 'FullAccess', \
            f'mapped ML-DSA-65 access key is not full access: {access}'
        # Baseline for check(): mirrored txs signed with the mapped key must
        # push this nonce past what the forked state started with.
        self.fork_access_nonce = access['access_key']['nonce']

        gas_mapped = mirror_utils.map_mldsa65_key_no_secret(self.gas_key)
        assert gas_mapped.pk in pq, \
            f'mapped ML-DSA-65 gas key {gas_mapped.pk} missing, ' \
            f'got {sorted(pq)}'
        # view_gas_key_nonces takes a full pubkey, not the handle state stores,
        # so this queries the mapped key the way the mirror itself does.
        nonces = mirror_utils.get_gas_key_nonces(node, 'test0',
                                                 gas_mapped.full_pk)
        assert nonces is not None, \
            f'no gas key nonces for mapped {gas_mapped.pk} on forked state'
        assert len(nonces) == self.gas_num_nonces, \
            f'expected {self.gas_num_nonces} nonce indexes on forked ' \
            f'state, got {len(nonces)}'
        # Baseline for check(): replayed V1 txs must push these past this.
        self.fork_gas_nonces = nonces
        balance = mirror_utils.get_gas_key_balance(node, 'test0',
                                                   gas_mapped.full_pk)
        assert balance is not None and balance > 0, \
            f'mapped ML-DSA-65 gas key has no balance on forked state: {balance}'

        logger.info(f'{self.name}: forked state carries both mapped '
                    'ML-DSA-65 keys')

    def post_fork(self, ctx):
        mirror_utils.send_add_mldsa65_access_key(ctx.node,
                                                 ctx.signer_key, self.added_key,
                                                 ctx.next_nonce(), ctx.bhash)
        # No senders: an ML-DSA-65 tx is ~5.3 KiB, so these keys send a couple
        # of txs each from on_post_fork_block rather than one per block, which
        # would crowd other test cases' txs out of the target chunks.
        return []

    def on_post_fork_block(self, ctx):
        # Each ML-DSA-65 key funds an implicit account of its own, so nothing
        # but a replayed ML-DSA-65-signed tx can create it on the target. The
        # second transfer makes the mirror map a second nonce for the key.
        for label, signer, recipient in self.signers():
            sent = self._sent[label]
            if sent >= self.txs_per_key:
                continue
            signer.send_if_inited(ctx.node, [(recipient.account_id(), 10**24)],
                                  ctx.bhash)
            if signer.inited():
                self._sent[label] = sent + 1
                logger.info(f'{self.name}: ML-DSA-65 {label} key sent '
                            f'{self._sent[label]} tx(s) to '
                            f'{recipient.account_id()}')

        # The gas key signs V1 txs instead, so the mirror maps its full pubkey
        # through NonceKind::GasKey(i) / view_gas_key_nonces rather than the
        # access key path. One tx per nonce index: a gas key V1 nonce is keyed
        # by (nonce, nonce_index), and * 1000000 + 1 keeps it above the current
        # access key nonce.
        nonce_index = self._sent['gas']
        if nonce_index < self.txs_per_key:
            mirror_utils.send_gas_key_transfer(ctx.node, self.gas_key,
                                               self.gas_recipient.account_id(),
                                               10**24, ctx.nonce * 1000000 + 1,
                                               nonce_index, ctx.bhash)
            self._sent['gas'] = nonce_index + 1

    def check(self, node):
        for label, mldsa_key in [('access', self.access_key),
                                 ('gas', self.gas_key),
                                 ('added', self.added_key)]:
            mapped = mirror_utils.map_mldsa65_key_no_secret(mldsa_key)
            nonce = node.get_nonce_for_pk('test0', mapped.pk, finality='final')
            assert nonce is not None, \
                f'mapped ML-DSA-65 {label} key {mapped.pk} not found on target'
            # The added key only exists on target because the mirror mapped a
            # post-fork AddKey action; its nonce moves only if the mirror also
            # signed replayed txs with the mapped ML-DSA-65 key.
            if label == 'added':
                assert nonce > 0, \
                    f'no tx signed with the mapped ML-DSA-65 added key ' \
                    f'landed on target, nonce is {nonce}'
            elif label == 'access':
                assert nonce > self.fork_access_nonce, \
                    f'no tx signed with the mapped ML-DSA-65 access key ' \
                    f'landed on target, nonce is still {nonce}'

        # The mapped gas key's per-index nonces only move if the mirror
        # re-signed V1 txs with it, which goes through fetch_gas_key_nonces
        # with the full 1952-byte pubkey.
        gas_mapped = mirror_utils.map_mldsa65_key_no_secret(self.gas_key)
        nonces = mirror_utils.get_gas_key_nonces(node, 'test0',
                                                 gas_mapped.full_pk)
        assert nonces is not None, \
            f'no gas key nonces for mapped {gas_mapped.pk} on target'
        assert len(nonces) == self.gas_num_nonces, \
            f'expected {self.gas_num_nonces} nonces on target, got {len(nonces)}'
        for i in range(self.txs_per_key):
            assert nonces[i] > self.fork_gas_nonces[i], \
                f'no V1 tx signed with the mapped ML-DSA-65 gas key landed ' \
                f'on target for nonce index {i}: {nonces[i]} vs forked ' \
                f'{self.fork_gas_nonces[i]}'

        for label, count in self._sent.items():
            assert count == self.txs_per_key, \
                f'ML-DSA-65 {label} key signed only {count} source txs'
        for label, recipient in [('access', self.access_recipient),
                                 ('added', self.added_recipient),
                                 ('gas', self.gas_recipient)]:
            mapped_id = mirror_utils.map_account_no_secret(
                recipient.account_id())
            res = node.get_account(mapped_id, do_assert=False)
            assert 'error' not in res, \
                f'account {mapped_id} funded by the ML-DSA-65 {label} key ' \
                f'not found on target'

        logger.info(f'{self.name}: target chain carries the mapped ML-DSA-65 '
                    'keys and the txs they signed')


def build_images(config, test_cases):
    """Phases 1-5: build target image (forked state) and source image (chain with traffic).

    Returns (target_img, source_img, validator_keys, end_source_height).
    """
    neard = os.path.join(config['near_root'],
                         config.get('binary_name', 'neard'))

    # Clean up any stale state from previous runs
    if MIRROR_DIR.exists():
        shutil.rmtree(MIRROR_DIR)

    # Phase 1: start 1 source node (single validator, 4 shards, archive)
    logger.info('Phase 1: starting source node')
    near_root, node_dirs = init_cluster(
        num_nodes=1,
        num_observers=0,
        num_shards=4,
        config=config,
        genesis_config_changes=[["epoch_length", 100]],
        client_config_changes={
            0: {
                "tracked_shards_config": "AllShards",
                "archive": True,
                # A pending state snapshot locks flat head updates; on a slow
                # machine it can outlive this short chain, leaving the flat
                # head at genesis, and fork-network forks at the flat head.
                # Nothing here needs snapshots (no state sync), so disable.
                # This config propagates to the forked target nodes too.
                "store.state_snapshot_config.state_snapshot_type": "Disabled",
            }
        },
    )
    source_node = spin_up_node(config,
                               near_root,
                               node_dirs[0],
                               ordinal=0,
                               single_node=True)

    # Phase 2: call pre_fork once, then drive traffic from returned senders,
    # until height > 12.
    logger.info('Phase 2: issuing pre-fork transactions')
    ctx = TestContext(source_node, source_node.signer_key, nonce=2)

    # The source node's RPC can briefly refuse connections right after it first
    # reports ready, so retry until it is reliably serving before querying it.
    source_node.wait_for_rpc(timeout=30)
    tip = source_node.get_latest_block()
    ctx.bhash = base58.b58decode(tip.hash.encode('utf8'))

    senders = []
    for tc in test_cases:
        senders.extend(tc.pre_fork(ctx))
    receivers = list(set(s.account_id() for s in senders)) + ['test0']

    for height, block_hash in utils.poll_blocks(source_node,
                                                timeout=mirror_utils.TIMEOUT):
        ctx.bhash = base58.b58decode(block_hash.encode('utf8'))

        for s in senders:
            s.send_if_inited(ctx.node, [(r, height) for r in receivers],
                             ctx.bhash)
        if height > 12:
            break

    # Phase 3: stop source, copy its DB, run fork-network on the copy -> target image
    logger.info('Phase 3: creating target image via fork-network')
    source_node.kill()
    source_home = pathlib.Path(source_node.node_dir)
    fork_base = MIRROR_DIR / 'fork-base'
    os.makedirs(MIRROR_DIR, exist_ok=True)

    mirror_utils.copy_near_home(source_home, fork_base)
    shutil.copyfile(source_home / 'validator_key.json',
                    fork_base / 'validator_key.json')
    validator_keys = mirror_utils.fork_network(neard, fork_base,
                                               TARGET_VALIDATORS)

    # Phase 4: verify forked state via check_fork hooks
    logger.info('Phase 4: verifying forked state')
    check_fork_dir = MIRROR_DIR / 'check-fork'
    mirror_utils.copy_near_home(fork_base, check_fork_dir)
    with open(check_fork_dir / 'validator_key.json', 'w') as f:
        json.dump(validator_keys[0].to_json(), f, indent=2)
    temp_node = spin_up_node(config,
                             near_root,
                             str(check_fork_dir),
                             ordinal=1,
                             single_node=True)
    # check_fork queries use finality=final
    utils.wait_for_final_block(temp_node)

    for tc in test_cases:
        tc.check_fork(temp_node)
    logger.info('forked state verified')
    temp_node.kill()

    # Phase 5: restart source, call post_fork once, then drive traffic for ~100
    # blocks to source image.
    logger.info('Phase 5: sending traffic to create source image')
    source_node.start()
    # Wait for the source node to be ready to accept RPCs after restart.
    time.sleep(5)

    tip = source_node.get_latest_block()
    ctx.bhash = base58.b58decode(tip.hash.encode('utf8'))

    start_source_height = tip.height

    senders = []
    for tc in test_cases:
        senders.extend(tc.post_fork(ctx))

    for height, block_hash in utils.poll_blocks(source_node,
                                                timeout=mirror_utils.TIMEOUT):
        ctx.bhash = base58.b58decode(block_hash.encode('utf8'))

        tx = transaction.sign_payment_tx(source_node.signer_key,
                                         source_node.signer_key.account_id, 300,
                                         ctx.next_nonce(), ctx.bhash)
        source_node.send_tx(tx)

        receivers = list(set(s.account_id() for s in senders)) + ['test0']
        for s in senders:
            s.send_if_inited(ctx.node, [(r, height) for r in receivers],
                             ctx.bhash)
        for tc in test_cases:
            tc.on_post_fork_block(ctx)

        if height - start_source_height >= 100:
            break

    end_source_height = source_node.get_latest_block().height
    source_node.kill()
    logger.info('Phase 5: source stopped, source image ready')

    return fork_base, source_home, validator_keys, end_source_height


def run_mirror(config, test_cases, target_img, source_img, validator_keys,
               end_source_height):
    """Phases 6-7: start target network, run mirror, validate."""
    near_root = config['near_root']

    logger.info('Phase 6: setting up target network')
    for name in ['stdout', 'stderr', 'config.json']:
        p = MIRROR_DIR / name
        if p.exists():
            p.unlink()

    # Set up working copies for the source and target mirror dirs.
    source_dir = MIRROR_DIR / 'source'
    mirror_utils.copy_near_home(source_img, source_dir)
    shutil.copyfile(source_img / 'validator_key.json',
                    source_dir / 'validator_key.json')
    mirror_utils.copy_near_home(target_img, MIRROR_DIR / 'target')

    # Prepare 4 target validator dirs: copy target image + validator key + AllShards.
    # Ordinals determine TCP port offsets in spin_up_node; start at 2 to avoid
    # conflicts with ordinals 0-1 used by init_cluster during build_images.
    base_ordinal = 2
    target_node_dirs = []
    for i, name in enumerate(TARGET_VALIDATORS):
        d = MIRROR_DIR / f'test_target_{name}'
        target_node_dirs.append(str(d))
        mirror_utils.copy_near_home(target_img, d)
        with open(d / 'validator_key.json', 'w') as f:
            json.dump(validator_keys[i].to_json(), f, indent=2)
        with open(d / 'config.json') as f:
            cfg = json.load(f)
        cfg['tracked_shards_config'] = 'AllShards'
        with open(d / 'config.json', 'w') as f:
            json.dump(cfg, f, indent=2)

    logger.info('Starting 4 target validators')
    target_nodes = []
    for i in range(len(TARGET_VALIDATORS)):
        node = spin_up_node(config,
                            near_root,
                            target_node_dirs[i],
                            base_ordinal + i,
                            boot_node=target_nodes or None)
        target_nodes.append(node)

    # Point mirror's target node at all validators
    target_home = MIRROR_DIR / 'target'
    with open(target_home / 'config.json') as f:
        target_cfg = json.load(f)
    target_cfg['network']['boot_nodes'] = ','.join(
        n.addr_with_pk() for n in target_nodes)
    with open(target_home / 'config.json', 'w') as f:
        json.dump(target_cfg, f, indent=2)

    # Start mirror (--no-secret: fork-network uses identity key mapping)
    logger.info('Starting mirror process')
    mirror = mirror_utils.MirrorProcess(near_root, str(source_dir),
                                        end_source_height,
                                        config.get('binary_name', 'neard'))
    time_limit = mirror_utils.allowed_run_time(target_node_dirs[0],
                                               mirror.start_time,
                                               end_source_height)

    while True:
        time.sleep(5)
        if not mirror.restart_once():
            break
        elapsed = time.time() - mirror.start_time
        if elapsed > time_limit:
            mirror.process.terminate()
            mirror.process.wait()
            assert False, f'mirror process timed out after {int(elapsed)}s'

    # Let target validators finalize the remaining mirrored blocks.
    logger.info('Waiting for target chain to settle')
    time.sleep(15)

    # Phase 7: validate that mirror replayed a substantial fraction of txs.
    # Can't require exact match: blocks before the fork point are baked into
    # forked state and some mapped txs fail (nonce conflicts, etc.).
    logger.info('Phase 7: validating results')
    # Next free ordinal: skip base_ordinal..base_ordinal+3 (4 target validators)
    # and +1 for the mirror target node that also binds ports.
    source_ordinal = base_ordinal + len(TARGET_VALIDATORS) + 1
    source_node = spin_up_node(config,
                               near_root,
                               str(source_dir),
                               source_ordinal,
                               single_node=True)
    # Wait for the source node to be ready to accept RPC queries.
    time.sleep(5)

    with open(os.path.join(target_node_dirs[0], 'genesis.json')) as f:
        genesis_height = json.load(f)['genesis_height']
    total_source = mirror_utils.count_total_txs(source_node,
                                                min_height=genesis_height)
    total_target = mirror_utils.count_total_txs(target_nodes[0])
    logger.info(f'source txs: {total_source}, target txs: {total_target}')
    assert total_target >= total_source * 0.5, \
        f'target has too few txs: {total_target} vs source {total_source}'

    for tc in test_cases:
        tc.check(target_nodes[0])

    logger.info('offline_test PASSED')


def main():
    config = load_config()
    test_cases = [
        ImplicitAccountTest(),
        AddKeyTest(),
        ContractTest(),
        CreateSubaccountTest(),
        GasKeyTest(),
        MlDsa65Test(),
    ]
    target_img, source_img, validator_keys, end_source_height = build_images(
        config, test_cases)
    run_mirror(config, test_cases, target_img, source_img, validator_keys,
               end_source_height)


if __name__ == '__main__':
    main()
