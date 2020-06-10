import sys, time
import multiprocessing, struct, socket, select

sys.path.append('lib')

from cluster import init_cluster, spin_up_node, load_config, LocalNode
from peer import *
from messages.crypto import *
from messages.network import *
from messages.block import *

schema = dict(crypto_schema + network_schema + block_schema)

config = load_config()
near_root, node_dirs = init_cluster(
    2, 0, 1, config,
    [["min_gas_price", 0], ["max_inflation_rate", [0, 1]], ["epoch_length", 10],
     ["block_producer_kickout_threshold", 80]], {2: {
         "tracked_shards": [0]
     }})

started = time.time()

def proxify_node(node, incoming_handler, response_handler, stopped, error):
    assert type(node) == LocalNode, type(node)
    old_port = node.port
    new_port = old_port + 100

    def listener():
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_inc:
                s_inc.settimeout(1)
                s_inc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s_inc.bind(("127.0.0.1", new_port))
                s_inc.listen()
                while 0 == stopped.value and 0 == error.value:
                    try:
                        conn, addr = s_inc.accept()
                    except socket.timeout:
                        continue
                    print('Connected by', addr)

                    conn.setblocking(0)

                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_node:
                        s_node.connect(("127.0.0.1", old_port))

                        s_node.setblocking(0)

                        while 0 == stopped.value and 0 == error.value:
                            print("BEFORE")
                            ready = select.select([conn], [], [], 1)
                            if not ready[0]:
                                print("TIMEOUT")
                                break

                            data = conn.recv(4)
                            print("AFTER")

                            if not data:
                                break
                            data = conn.recv(struct.unpack('I', data)[0])
                            decision = incoming_handler(data, addr)
                            if type(decision) == bytes:
                                data = decision
                                decision = True
                            assert type(decision) == bool, type(decision)

                            if decision:
                                s_node.sendall(struct.pack('I', len(data)))
                                s_node.sendall(data)

                                print("BEFORE2")
                                ready = select.select([s_node], [], [], 1)
                                if not ready[0]:
                                    print("TIMEOUT2")
                                    continue

                                len_bytes = s_node.recv(4)
                                print("AFTER2")
                                if not len_bytes:
                                    continue

                                len_ = struct.unpack('I', len_bytes)[0]
                                response = s_node.recv(len_)

                                decision = response_handler(response, addr)

                                if type(decision) == bytes:
                                    response = decision
                                    decision = True
                                assert type(decision) == bool, type(decision)

                                if decision:
                                    conn.sendall(struct.pack('I', len(response)))
                                    conn.sendall(response)
                                else:
                                    break
                            else:
                                break
        except:
            error.value = 1
            raise

    p = multiprocessing.Process(target=listener, args=())
    p.start()

    node.port = new_port
    return p

def handler(msg, peer):
    try:
        obj = BinarySerializer(schema).deserialize(msg, PeerMessage)
        assert BinarySerializer(schema).serialize(obj) == msg

        print("   > ", obj.enum)

        if obj.enum == 'Handshake':
            obj.Handshake.listen_port += 100
            return BinarySerializer(schema).serialize(obj)

    except:
        print("ERROR", int(msg[0]), peer)
        if msg[0] == 13:
            raise
        #raise

    return True

stopped = multiprocessing.Value('i', 0)
error = multiprocessing.Value('i', 0)

try:
    boot_node = spin_up_node(config, near_root, node_dirs[0], 0, None, None)
    p1 = proxify_node(boot_node, handler, handler, stopped, error)

    node1 = spin_up_node(config, near_root, node_dirs[1], 1, boot_node.node_key.pk,
                         boot_node.addr())
    p2 = proxify_node(node1, handler, handler, stopped, error)

    time.sleep(10)

finally:
    stopped.value = 1
    p1.join()
    p2.join()

