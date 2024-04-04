import unittest
from key_storage import KeyStorage
import re
import json

IDS = [(x, f'{x}.near') for x in ["first", "second", "third"]]
FIRST = IDS[0]
SECOND = IDS[1]
THIRD = IDS[2]

PK_RE = r"ed25519:\w{44}"


class KeyStorageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.key_storage = KeyStorage()
        for x in IDS:
            self.key_storage.add_random(x[0], x[1])

    def test_boot_nodes(self):
        ip_mapping = {x[0]: f"0.0.0.{i + 1}" for (i, x) in enumerate(IDS)}
        port_mapping = {x[0]: 4441 + i for (i, x) in enumerate(IDS)}
        ip_port_res = [r"@0.0.0.1:4441", r"@0.0.0.2:4442", r"@0.0.0.3:4443"]
        node_res = [PK_RE + x for x in ip_port_res]

        boot_nodes = self.key_storage.get_boot_nodes(ip_mapping=ip_mapping,
                                                     port_mapping=port_mapping)
        print(boot_nodes)
        self.assertEqual(
            re.fullmatch(r",".join(node_res), boot_nodes) is not None,
            True
        )

        boot_nodes = self.key_storage.get_boot_nodes(ip_mapping=ip_mapping,
                                                     port_mapping=port_mapping,
                                                     without_node_id=FIRST[0])
        print(boot_nodes)
        self.assertEqual(
            re.fullmatch(
                r",".join([node_res[1], node_res[2]]), boot_nodes
            ) is not None,
            True
        )

        boot_nodes = self.key_storage.get_boot_nodes(ip_mapping=ip_mapping,
                                                     port_mapping=port_mapping,
                                                     without_node_id=SECOND[0])
        print(boot_nodes)
        self.assertEqual(
            re.fullmatch(
                r",".join([node_res[0], node_res[2]]), boot_nodes
            ) is not None,
            True
        )

        boot_nodes = self.key_storage.get_boot_nodes(ip_mapping=ip_mapping,
                                                     port_mapping=port_mapping,
                                                     without_node_id=THIRD[0])
        print(boot_nodes)
        self.assertEqual(
            re.fullmatch(
                r",".join([node_res[0], node_res[1]]), boot_nodes
            ) is not None,
            True
        )

        boot_nodes = self.key_storage.get_boot_nodes(
            ip_mapping=ip_mapping, port_mapping=port_mapping,
            without_node_id="not a node id")
        print(boot_nodes)
        self.assertEqual(
            re.fullmatch(
                r",".join(node_res), boot_nodes
            ) is not None,
            True
        )

    def test_validator_list(self):
        validators = json.loads(
            self.key_storage.get_validator_json_str(stake=456)
        )
        print(validators)
        for v, id in zip(validators, IDS):
            self.assertEqual(v["amount"], 456)
            self.assertEqual(v["account_id"], id[1])


if __name__ == '__main__':
    unittest.main()
