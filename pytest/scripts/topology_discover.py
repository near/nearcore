import sys
import asyncio
import json
import os
from pprint import pprint

sys.path.append('lib')

import nacl.signing
from cluster import start_cluster
from peer import ED_PREFIX, connect, run_handshake, create_peer_request
from utils import obj_to_string
from messages import schema
import base58

ED_PREFIX = "ed25519:"

def encoded_public_key(key_pair):
    return ED_PREFIX + base58.b58encode(key_pair.verify_key.encode()).decode()

CURRENT_TRACKER = set()

async def inspect_node(addr):
    target_addr = str(addr)

    if target_addr in CURRENT_TRACKER:
        return

    print("Start inspecting node:", target_addr)
    CURRENT_TRACKER.add(target_addr)

    key_pair = nacl.signing.SigningKey.generate()
    try:
        conn = await connect(addr, verbose=True)
    except Exception:
        print(f"Fail connection with {target_addr}. Disconnecting")
        return

    public_key = encoded_public_key(key_pair)
    print(public_key)
    await run_handshake(conn, public_key, key_pair)

    peer_request = create_peer_request()
    await conn.send(peer_request)

    accounts = {}
    rev_accounts = {}

    peer_id = {}
    edges = {}

    num_accounts = 0
    num_edges = 0
    num_peer_id = 0

    def get_peer_id_index(pid):
        data = peer_id.get(pid, None)
        if data == None:
            data = len(peer_id)
            peer_id[pid] = data
        return data

    messages_received = 0

    while True:
        message = await conn.recv()

        if message.enum == 'Sync':
            for account in message.Sync.accounts:
                index = get_peer_id_index(account.peer_id.data.hex())
                accounts[account.account_id] = index
                rev_accounts[index] = account.account_id

            for edge in message.Sync.edges:
                u = get_peer_id_index(edge.peer0.data.hex())
                v = get_peer_id_index(edge.peer1.data.hex())
                # edges.append((u, v, edge.nonce))
                edges[(u, v)] = edge.nonce

        elif message.enum == 'PeersResponse':
            for peer_info in message.PeersResponse:
                if peer_info.addr is None:
                    continue

                ip = '.'.join(map(str, list(peer_info.addr.V4[0])))
                port = peer_info.addr.V4[1]

                loop = asyncio.get_event_loop()
                loop.create_task(inspect_node((ip, port)))

        if len(accounts) > num_accounts or len(edges) > num_edges or len(peer_id) > num_peer_id:
            num_peer_id = len(peer_id)
            num_accounts = len(accounts)
            num_edges = len(edges)

            data = {
                'peer_id' : peer_id,
                'accounts': accounts,
                'edges': [(key[0], key[1], value) for key, value in edges.items()]
            }

            # pprint(data)

            print(num_accounts, num_edges, num_peer_id)

            # print("writing")
            # with open(f'data/info-{messages_received}-{target_addr}.json', 'w') as f:
            #     json.dump(data, f, indent=2)
            # print("Finish writing")

        messages_received += 1
        if (messages_received & (messages_received - 1)) == 0:
            print(f"Target: {target_addr} Messages received: {messages_received}")


if __name__ == '__main__':
    os.makedirs('data', exist_ok=True)
    # asyncio.run(inspect_node(('127.0.0.1', 24567)))
    # asyncio.run(inspect_node(('52.43.182.44', 24567)))
    asyncio.run(inspect_node(('35.196.191.90', 24567)))

