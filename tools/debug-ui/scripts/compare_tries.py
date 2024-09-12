'''Performs diff of two tries between two nodes, by querying their Entity Debug API.
This is useful when a node is stuck at InvalidStateRoot to debug why.'''
import json
import requests

USE_COLD_STORAGE = False

class EntityAPI:
    def __init__(self, host):
        self.endpoint = "http://{}/debug/api/entity".format(host)

    def query(self, query_name, cold_storage=False, **kwargs):
        args = kwargs
        if len(args) == 0:
            args = None
        if cold_storage:
            query = {
                query_name: args,
                "use_cold_storage": cold_storage
            }
        else:
            query = {
                query_name: args,
            }
        result = requests.post(self.endpoint, json=query)
        return EntityDataValue.of(result.json())
    
    def get_trie_node(self, shard_uid, trie_node_hash):
        return self.query('RawTrieNodeByHash', shard_uid=shard_uid, trie_node_hash=trie_node_hash)
    
    def get_trie_value(self, shard_uid, trie_value_hash):
        return self.query('RawTrieValueByHash', shard_uid=shard_uid, trie_value_hash=trie_value_hash)


class EntityDataValue:
    @staticmethod
    def of(value):
        if isinstance(value, str) or value is None:
            return value
        return EntityDataValue(value)

    def __init__(self, value):
        self.value = value

    def __getitem__(self, key):
        if isinstance(key, int):
            key = str(key)
        entries = self.value['entries']

        return EntityDataValue.of([entry['value'] for entry in entries if entry['name'] == key][0])
    
    def __contains__(self, key):
        entries = self.value['entries']

        return any(entry['name'] == key for entry in entries)
    
    def array(self):
        return [EntityDataValue.of(entry['value']) for entry in self.value['entries']]
    
    def __iter__(self):
        return iter(self.array())
    
    def __str__(self):
        return json.dumps(self.value)

class TrieIterator:
    def __init__(self, api: EntityAPI, shard_uid: str, state_root: str):
        self.api = api
        self.shard_uid = shard_uid
        self.state_root = state_root
        self.node = None

    def iterate(self):
        yield from self.iterate_node_hash(self.state_root, '')
    
    def iterate_node_hash(self, hash, path):
        dig = yield path, 'node', hash
        if dig:
            body = self.api.get_trie_node(self.shard_uid, hash)
            yield from self.iterate_node_body(body, path)

    def iterate_value_hash(self, hash, path):
        dig = yield path, 'value_ref', hash
        if dig:
            value = self.api.get_trie_value(self.shard_uid, hash)
            yield path + ' ', 'value', value
    
    def iterate_node_body(self, node, path):
        if 'value_hash' in node:
            leaf_path = path
            if 'extension' in node:
                leaf_path += node['extension']
            yield from self.iterate_value_hash(node['value_hash'], leaf_path)
        if 'children' in node:
            for i in range(16):
                child = node['children'][i]
                if child == 'null':
                    continue
                nibble = '0123456789abcdef'[i]
                if child is not None:
                    yield from self.iterate_node_hash(child, path + nibble)
        if 'child' in node:
            next_path = path + node['extension']
            yield from self.iterate_node_hash(node['child'], next_path)

class IterWithCurrent:
    def __init__(self, iter):
        self.iter = iter
        self.current = None
        self.done = False
        self.next(None)
    
    def next(self, to_send):
        if self.done:
            raise Exception('Iteration done')
        try:
            self.current = self.iter.send(to_send)
        except StopIteration:
            self.current = None
            self.done = True

class TrieDiffer:
    def __init__(self, api_a: EntityAPI, api_b: EntityAPI, shard_uid: str):
        self.api_a = api_a
        self.api_b = api_b
        self.shard_uid = shard_uid

    def diff_tries(self, root_a: str, root_b: str):
        diffs = []
        iter_a = IterWithCurrent(TrieIterator(self.api_a, self.shard_uid, root_a).iterate())
        iter_b = IterWithCurrent(TrieIterator(self.api_b, self.shard_uid, root_b).iterate())
        while not iter_a.done or not iter_b.done:
            if len(diffs) > 100:
                print("Too many diffs; stopping.")
                break
            (path_a, kind_a, value_a) = iter_a.current
            (path_b, kind_b, value_b) = iter_b.current
            if path_a == path_b and kind_a == kind_b and value_a == value_b:
                print('\033[A\u001b[32m[Match] {} {} {}\u001b[0m'.format(path_a, kind_a, value_a))
                iter_a.next(False)
                iter_b.next(False)
            elif path_a == path_b and kind_a == kind_b == 'value':
                print('\033[A\u001b[31m[Mismatch] {} value {} <=> {}\u001b[0m\n'.format(path_a, value_a, value_b))
                diffs.append((path_a[:-1], value_a, value_b))
                iter_a.next(None)
                iter_b.next(None)
            elif path_a <= path_b:
                if kind_a == 'value':
                    print('\033[A\u001b[31m[Mismatch] {} value {} <=> {}\u001b[0m\n'.format(path_a, value_a, None))
                    diffs.append((path_a[:-1], value_a, None))
                iter_a.next(True)
            else:
                if kind_b == 'value':
                    print('\033[A\u001b[31m[Mismatch] {} value {} <=> {}\u001b[0m\n'.format(path_b, None, value_b))
                    diffs.append((path_b[:-1], None, value_b))
                iter_b.next(True)
        print()
        return diffs

good_node_addr = input('Enter the address of the good node, including port (e.g. 127.0.0.1:3030): ')
stuck_node_addr = input('Enter the address of the stuck node, including port (e.g. 127.0.0.1:3030): ')

good_node = EntityAPI(good_node_addr)
stuck_node = EntityAPI(stuck_node_addr)

print("[Sanity check] Good node head is at ", good_node.query('TipAtHead')['height'])
print("[Sanity check] Stuck node head is at ", stuck_node.query('TipAtHead')['height'])

good_state_root = input('Enter the good state root available on the good node: ')
stuck_state_root = input('Enter the bad state root available on the stuck node: ')
shard_uid = input('Enter the shard UID (e.g. s0.v3): ')

print('[Sanity check] Querying good state root...', end='')
good_node.query('RawTrieNodeByHash', shard_uid=shard_uid, trie_node_hash=good_state_root)
print('valid.')
print('[Sanity check] Querying bad state root...', end='')
stuck_node.query('RawTrieNodeByHash', shard_uid=shard_uid, trie_node_hash=stuck_state_root)
print('valid.')

print('Comparing trie roots:', good_state_root, stuck_state_root)

differ = TrieDiffer(good_node, stuck_node, shard_uid)
diffs = differ.diff_tries(good_state_root, stuck_state_root)
for (path, value_a, value_b) in diffs:
    print('Mismatch in trie at path:', path)
    print('  Good:', value_a)
    print('  Bad: ', value_b)
    print()
print('Diff done.')
