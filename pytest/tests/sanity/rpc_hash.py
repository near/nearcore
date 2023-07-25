#!/usr/bin/env python3
"""Test computing block hash from data provided by JSON RPCs."""

import hashlib
import json
import pathlib
import sys
import typing
import unittest

import base58

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

from configured_logger import logger
import cluster
import utils
import serializer

import messages
import messages.crypto
import messages.block

config = cluster.load_config()
binary_protocol_version = cluster.get_binary_protocol_version(config)
assert binary_protocol_version is not None
BLOCK_HEADER_V4_PROTOCOL_VERSION = 138


def serialize(msg: typing.Any) -> bytes:
    return serializer.BinarySerializer(messages.schema).serialize(msg)


def compute_block_hash(header: typing.Dict[str, typing.Any],
                       msg_version: int) -> str:
    """Computes block hash based on given block header.

    Args:
        header: JSON representation of the block header.
        msg_version: Version of the BlockHeaderInnerRest to use when serialising
            and computing hash.  This depends on protocol version used when the
            block was generated.
    Returns:
        Base58-encoded block hash.
    """

    def get_int(key: str) -> typing.Optional[int]:
        value = header.get(key)
        if value is None:
            return None
        return int(value)

    def get_hash(key: str) -> typing.Optional[bytes]:
        value = header.get(key)
        if value is None:
            return value
        result = base58.b58decode(value)
        assert len(result) == 32, (key, value, len(result))
        return result

    def sha256(*chunks: bytes) -> bytes:
        return hashlib.sha256(b''.join(chunks)).digest()

    prev_hash = get_hash('prev_hash')

    inner_lite = messages.block.BlockHeaderInnerLite()
    inner_lite.height = get_int('height')
    inner_lite.epoch_id = get_hash('epoch_id')
    inner_lite.next_epoch_id = get_hash('next_epoch_id')
    inner_lite.prev_state_root = get_hash('prev_state_root')
    inner_lite.outcome_root = get_hash('outcome_root')
    inner_lite.timestamp = get_int('timestamp_nanosec')
    inner_lite.next_bp_hash = get_hash('next_bp_hash')
    inner_lite.block_merkle_root = get_hash('block_merkle_root')
    inner_lite_blob = serialize(inner_lite)
    inner_lite_hash = sha256(inner_lite_blob)

    inner_rest_msg = {
        1: messages.block.BlockHeaderInnerRest,
        2: messages.block.BlockHeaderInnerRestV2,
        3: messages.block.BlockHeaderInnerRestV3,
        4: messages.block.BlockHeaderInnerRestV4,
    }[msg_version]

    inner_rest = inner_rest_msg()
    # Some of the fields are superfluous in some of the versions of the message
    # but that’s quite all right.  Serialiser will ignore them and
    # unconditionally setting them makes the code simpler.
    inner_rest.block_body_hash = get_hash('block_body_hash')
    inner_rest.chunk_receipts_root = get_hash('chunk_receipts_root')
    inner_rest.chunk_headers_root = get_hash('chunk_headers_root')
    inner_rest.chunk_tx_root = get_hash('chunk_tx_root')
    inner_rest.chunks_included = get_int('chunks_included')
    inner_rest.challenges_root = get_hash('challenges_root')
    inner_rest.random_value = get_hash('random_value')
    # TODO: Handle non-empty list.
    inner_rest.validator_proposals = header['validator_proposals']
    inner_rest.chunk_mask = bytes(header['chunk_mask'])
    inner_rest.gas_price = get_int('gas_price')
    inner_rest.total_supply = get_int('total_supply')
    # TODO: Handle non-empty list.
    inner_rest.challenges_result = header['challenges_result']
    inner_rest.last_final_block = get_hash('last_final_block')
    inner_rest.last_ds_final_block = get_hash('last_ds_final_block')
    inner_rest.block_ordinal = get_int('block_ordinal')
    inner_rest.prev_height = get_int('prev_height')
    inner_rest.epoch_sync_data_hash = get_hash('epoch_sync_data_hash')
    inner_rest.approvals = [
        approval and messages.crypto.Signature(approval)
        for approval in header['approvals']
    ]
    inner_rest.latest_protocol_version = get_int('latest_protocol_version')
    inner_rest_blob = serialize(inner_rest)
    inner_rest_hash = sha256(inner_rest_blob)

    inner_hash = sha256(inner_lite_hash + inner_rest_hash)
    block_hash = sha256(inner_hash + prev_hash)

    return base58.b58encode(block_hash).decode('ascii')


class HashTestCase(unittest.TestCase):

    def __init__(self, *args, **kw) -> None:
        super().__init__(*args, **kw)
        self.maxDiff = None

    def test_compute_block_hash(self):
        """Tests that compute_block_hash function works correctly.

        This is a sanity check for cases when someone modifies the test code
        itself.  If this test breaks than all the other tests will break as
        well.  This test runs the compute_block_hash function on a well-known
        static input checking expected hash.
        """
        header = {
            'height':
                4,
            'prev_height':
                3,
            'epoch_id':
                '11111111111111111111111111111111',
            'next_epoch_id':
                'AuatKw3hiGmXed3uT2u4Die6ZRGZhEHn34kTyVpGnYLM',
            'prev_hash':
                'BUcVEkMq3DcZzDGgeh1sb7FFuD86XYcXpEt25Cf34LuP',
            'prev_state_root':
                'Bn786g4GdJJigSP4qRSaVCfeMotWVX88cV1LTZhD6o3z',
            'block_body_hash':
                '4K3NiGuqYGqKPnYp6XeGd2kdN4P9veL6rYcWkLKWXZCu',  # some arbitrary string
            'chunk_receipts_root':
                '9ETNjrt6MkwTgSVMMbpukfxRshSD1avBUUa4R4NuqwHv',
            'chunk_headers_root':
                'Fk7jeakmi8eruvv4L4ToKs7MV1YG64ETZtASQYjGBWK1',
            'chunk_tx_root':
                '7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t',
            'outcome_root':
                '7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t',
            'chunks_included':
                1,
            'challenges_root':
                '11111111111111111111111111111111',
            'timestamp':
                1642022757141096960,
            'timestamp_nanosec':
                '1642022757141096960',
            'random_value':
                'GxYrjCxQtfG2K7hX6w4aPs3usTskzfCkbVc2icSQMF7h',
            'validator_proposals': [],
            'chunk_mask': [True],
            'gas_price':
                '1000000000',
            'block_ordinal':
                4,
            'rent_paid':
                '0',
            'validator_reward':
                '0',
            'total_supply':
                '3000000000000000000000000000000000',
            'challenges_result': [],
            'last_final_block':
                'GTudmqKJQjEVCrdi31vcHqXoEpvEScmZ9BhBf3gPJ4pp',
            'last_ds_final_block':
                'BUcVEkMq3DcZzDGgeh1sb7FFuD86XYcXpEt25Cf34LuP',
            'next_bp_hash':
                '236RGxQc2xSqukyiBkixtZSqKu679ZxeS6vP8zzAL9vW',
            'block_merkle_root':
                'Gf3uWgULzc5WDuaAq4feehh7M1TFRFxTWVv2xH6AsnpA',
            'epoch_sync_data_hash':
                '4JTQn5LGcxdx4xstsAXgXHcP3oHKatzdzHBw6atBDSWV',
            'approvals': [
                'ed25519:5Jdeg8rk5hAbcooyxXQSTcxBgUK39Z8Qtfkhqmpi26biU26md5wBiFvkAEGXrMyn3sgq3cTMG8Lr3HD7RxWPjkPh',
                'ed25519:4vqTaN6bucu6ALsb1m15e8HWGGxLQeKJhWrcU8zPRrzfkZbakaSzW8rfas2ZG89rFKheZUyrnZRKooRny6YKFyKi'
            ],
            'signature':
                'ed25519:5mGi9dyuyt7TnSpPFjbEWSJThDdiEV9NNQB11knXvRbxSv8XfBT5tdVVFypeqpZjeB3fD7qgJpWhTj3KvdGbcXdu',
            'latest_protocol_version':
                50
        }

        for msg_ver, block_hash in (
            (1, '3ckGjcedZiN3RnvfiuEN83BtudDTVa9Pub4yZ8R737qt'),
            (2, 'Hezx56VTH815G6JTzWqJ7iuWxdR9X4ZqGwteaDF8q2z'),
            (3, 'Finjr87adnUqpFHVXbmAWiVAY12EA9G4DfUw27XYHox'),
            (4, '2QfdGyGWByEeL2ZSy8u2LoBa4pdDwf5KoDrr94W6oeB6'),
        ):
            self.assertEqual(block_hash, compute_block_hash(header, msg_ver))

        # Now try with a different block body hash
        header[
            'block_body_hash'] = '4rMxTeTF9LehPbzB2xhVa4xWVtbyjRfvL7qsxc8sL7WP'
        for msg_ver, block_hash in (
            (1, '3ckGjcedZiN3RnvfiuEN83BtudDTVa9Pub4yZ8R737qt'),
            (2, 'Hezx56VTH815G6JTzWqJ7iuWxdR9X4ZqGwteaDF8q2z'),
            (3, 'Finjr87adnUqpFHVXbmAWiVAY12EA9G4DfUw27XYHox'),
            (4, '3Cdm4sS9b4jdypMezP8ta6p2ecyRSJC9uaGUJTY18MUH'),
        ):
            self.assertEqual(block_hash, compute_block_hash(header, msg_ver))

        # Now try witohut epoch_sync_data_hash
        header['epoch_sync_data_hash'] = None
        for msg_ver, block_hash in (
            (1, '3ckGjcedZiN3RnvfiuEN83BtudDTVa9Pub4yZ8R737qt'),
            (2, 'Hezx56VTH815G6JTzWqJ7iuWxdR9X4ZqGwteaDF8q2z'),
            (3, '82v8RAc66tWpdjRCsoSrgnzpU6JMhpjbWKmUEcfkzX6T'),
            (4, '9BYhkbWkKTLJj46goq5WPEzUJDf5juHJnBu2jjoHL7yc'),
        ):
            self.assertEqual(block_hash, compute_block_hash(header, msg_ver))

        # Now try with one approval missing
        header['approvals'][1] = None
        for msg_ver, block_hash in (
            (1, 'EE2JtxdqWLBDKqNARxdFDqH36mEJ1xJ6LjQ9f7qCSDRE'),
            (2, '2WdpJD5dYPjEMn3EYbm1BhGgCAX7ksxJGTQm4xHazBxt'),
            (3, '3bx6vfbH8GrYp8UFMagiBgYyKMH63D7Qo5J7jCsNbh9o'),
            (4, 'CTDBpUpCdhdCjCMfaFD5r96PyKDK756aXw69toLYEaSH'),
        ):
            self.assertEqual(block_hash, compute_block_hash(header, msg_ver))

    def _test_block_hash(self,
                         msg_version: int,
                         protocol_version: typing.Optional[int] = None) -> None:
        """Starts a cluster, fetches blocks and computes their hashes.

        The cluster is started with genesis configured to use given protocol
        version.  The code fetches blocks until: 1) a block with all approvals
        set is encountered, 2) another block with at least one approval missing
        and 3) at least ten blocks total are checked.

        Args:
            msg_version: Version of the BlockHeaderInnerRest to use when
                serialising and computing hash.
            protocol_version: If given, protocol version to use in the cluster
                (which will be set in genesis); If not given, cluster will be
                started with the newest supported protocol version.
        """
        genesis_overrides = []
        if protocol_version:
            genesis_overrides = [['protocol_version', protocol_version]]

        nodes = ()
        try:
            nodes = cluster.start_cluster(4, 0, 4, None, genesis_overrides, {})
            got_all_set = False
            got_some_unset = False
            count = 0
            for block_id in utils.poll_blocks(nodes[0]):
                header = nodes[0].get_block(block_id.hash)['result']['header']
                self.assertEqual((block_id.height, block_id.hash),
                                 (header['height'], header['hash']),
                                 (block_id, header))
                got = compute_block_hash(header, msg_version)
                self.assertEqual(header['hash'], got, header)

                if all(header['approvals']):
                    if not got_all_set:
                        nodes[1].kill()
                        got_all_set = True
                elif any(approval is None for approval in header['approvals']):
                    got_some_unset = True

                count += 1
                if got_all_set and got_some_unset and count >= 10:
                    break
        finally:
            for node in nodes:
                node.cleanup()

    def test_block_hash_v1(self):
        """Starts a cluster using protocol version 24 and verifies block hashes.

        The cluster is started with a protocol version in which the first
        version of the BlockHeaderInnerRest has been used.
        """
        self._test_block_hash(1, 24)

    def test_block_hash_v2(self):
        """Starts a cluster using protocol version 42 and verifies block hashes.

        The cluster is started with a protocol version in which the second
        version of the BlockHeaderInnerRest has been used.
        """
        self._test_block_hash(2, 42)

    def test_block_hash_v3(self):
        """Starts a cluster using protocol version 50 and verifies block hashes.

        The cluster is started with a protocol version in which the third
        version of the BlockHeaderInnerRest has been used.
        """
        self._test_block_hash(3, 50)

    if binary_protocol_version >= BLOCK_HEADER_V4_PROTOCOL_VERSION:

        def test_block_hash_v4(self):
            """Starts a cluster using protocol version 138 and verifies block hashes.

            The cluster is started with a protocol version in which the fourth
            version of the BlockHeaderInnerRest has been used.
            """
            self._test_block_hash(4, 138)

    def test_block_hash_latest(self):
        """Starts a cluster using latest protocol and verifies block hashes.

        The cluster is started with the newest protocol version supported by the
        node.  If this test fails while others pass this may indicate that a new
        BlockHeaderInnerRest message has been introduced and this test needs to
        be updated to support it.
        """
        if binary_protocol_version >= BLOCK_HEADER_V4_PROTOCOL_VERSION:
            self._test_block_hash(4)
        else:
            self._test_block_hash(3)


if __name__ == '__main__':
    unittest.main()
