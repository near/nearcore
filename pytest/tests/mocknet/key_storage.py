from typing import NamedTuple
import json
import sys
import pathlib

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))
from key import Key


class NodeKeys(NamedTuple):
    node_key: Key
    validator_key: Key

    @classmethod
    def from_random(cls, node_id: str, account_id: str) -> 'NodeKeys':
        return cls(Key.from_random(account_id=node_id),
                   Key.from_random(account_id=account_id))


class NodeWithKeys(NamedTuple):
    node_id: str
    account_id: str
    keys: NodeKeys


class KeyStorage:
    def __init__(self):
        self.nodes = []

    def add_random(self, node_id: str, account_id: str):
        self.nodes.append(
            NodeWithKeys(node_id=node_id, account_id=account_id,
                         keys=NodeKeys.from_random(node_id, account_id)))

    def get_boot_nodes(self, ip_mapping, port_mapping, without_node_id=None):
        addrs = []
        for node in self.nodes:
            if node.node_id == without_node_id:
                continue
            port = port_mapping.get(node.node_id)
            ip = ip_mapping.get(node.node_id)
            addrs.append(f'{node.keys.node_key.pk}@{ip}:{port}')
        return ",".join(addrs)

    def get_validator_json_str(self, stake=1000):
        return json.dumps([{
            "account_id": n.account_id,
            "public_key": n.keys.validator_key.pk,
            "amount": stake,
        } for n in self.nodes])
