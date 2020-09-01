# This test makes sure that we do not regress our happy-case scenario of chunk propagation.
# Spins up four nodes with two shards with large enough block production time, and ensures
# that only the necessary partial encoded chunk messages are sent.

import sys, time, asyncio, hashlib
import multiprocessing

sys.path.append('lib')

from cluster import start_cluster
from peer import *
from proxy import ProxyHandler

MESSAGE = 0
REQUEST = 1
RESPONSE = 2

MIN_HEIGHT = 6
MAX_HEIGHT = 10

TIMEOUT = 30
success = multiprocessing.Value('i', 0)

manager = multiprocessing.Manager()
hash_to_metadata = manager.dict()
msgs = manager.dict()

class Handler(ProxyHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def handle(self, msg, fr, to):
        if msg.enum == 'Block':
            if msg.Block.BlockV1.header.BlockHeaderV2.inner_lite.height >= MAX_HEIGHT:
                success.value = 1

        if msg.enum == 'Routed':
            msg_kind = msg.Routed.body.enum
            if msg_kind == 'PartialEncodedChunk':
                header = msg.Routed.body.PartialEncodedChunk.header
                height = header.inner.height_created
                shard_id = header.inner.shard_id
                hash_ = header.chunk_hash()
                if height >= MIN_HEIGHT:
                    print("MSG %s %s %s %s" % (height, shard_id, fr, to))
                    assert (MESSAGE, height, shard_id, fr, to) not in msgs
                    msgs[(MESSAGE, height, shard_id, fr, to)] = 1
                hash_to_metadata[hash_] = (height, shard_id)
            if msg_kind == 'PartialEncodedChunkRequest':
                hash_ = msg.Routed.body.PartialEncodedChunkRequest.chunk_hash
                (height, shard_id) = hash_to_metadata[hash_]
                if height >= MIN_HEIGHT:
                    print("REQ %s %s %s %s" % (height, shard_id, fr, to))
                    assert (REQUEST, height, shard_id, fr, to) not in msgs
                    msgs[(REQUEST, height, shard_id, fr, to)] = 1
            if msg_kind == 'PartialEncodedChunkResponse':
                hash_ = msg.Routed.body.PartialEncodedChunkResponse.chunk_hash
                (height, shard_id) = hash_to_metadata[hash_]
                if height >= MIN_HEIGHT:
                    print("RESP %s %s %s %s" % (height, shard_id, fr, to))
                    assert (RESPONSE, height, shard_id, fr, to) not in msgs
                    msgs[(RESPONSE, height, shard_id, fr, to)] = 1


        return True


start_cluster(
    4, 0, 2, None,
    [["min_gas_price", 0], ["max_inflation_rate", [0, 1]], ["epoch_length", 10],
     ["block_producer_kickout_threshold", 80]], {}, Handler)

started = time.time()

while True:
    assert time.time() - started < TIMEOUT
    time.sleep(1)

    if success.value == 1:
        break

# Verify that we see only the messages we expect.
# Heights before MIN_HEIGHT might be affected by nodes spinning up.
# Also exclude MAX_HEIGHT - 1, because some responses might have not arrived for it yet

# We are not asserging that the responses set is equal to the requests set, because if
# a node receives the request before they learn about the chunk, they do not respond, and
# thus the set of responses might be smaller than the set of requests.

for height in range(MIN_HEIGHT, MAX_HEIGHT - 1):
    for shard_id in range(2):
        # the ordinal of the chunk producer 
        producer = -1
        # the ordinal of the other block producer in the same shard
        follower = -1

        message_receivers = set()
        request_receivers = set()
        for fr in range(4):
            for to in range(4):
                if (MESSAGE, height, shard_id, fr, to) in msgs:
                    assert producer in [-1, fr]
                    producer = fr
                    message_receivers.add(to)
                if (REQUEST, height, shard_id, fr, to) in msgs:
                    assert follower in [-1, fr]
                    follower = fr
                    request_receivers.add(to)
                if (RESPONSE, height, shard_id, fr, to) in msgs:
                    assert follower in [-1, to]
                    follower = to

        assert message_receivers == set([0, 1, 2, 3]) - set([producer]), (height, shard_id, message_receivers)
        assert request_receivers == set([0, 1, 2, 3]) - set([follower]), (height, shard_id, request_receivers)


