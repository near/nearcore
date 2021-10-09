"""
Protocol upgrade parameters, including:
* Adding `latest_protocol_version` to BlockHeaderInnerRest.
* Collecting this information inside EpochManager.
* Switching protocol version to the next one based on `protocol_upgrade_stake_threshold` and `protocol_upgrade_num_epochs`.
* Removing `config_version`.
"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')),
                   object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 21

config['protocol_version'] = 22

config.pop('config_version')
config['protocol_upgrade_stake_threshold'] = [4, 5]
config['protocol_upgrade_num_epochs'] = 2

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
