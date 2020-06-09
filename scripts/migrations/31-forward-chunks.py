"""
Chunk part owners automatically forward their parts to validators tracking the chunk's shard.
Protocol changes include:
* PartialEncodedChunk hash is now calculated by hashing the concatenation of the inner header
  hash and the merkle root. This allows validation of the forwarded parts.
* A new PartialEncodedChunkForward message is defined for the purpose of forwarding chunk parts.
"""

import sys
import os
import json
from collections import OrderedDict

home = sys.argv[1]
output_home = sys.argv[2]

config = json.load(open(os.path.join(home, 'output.json')), object_pairs_hook=OrderedDict)

assert config['protocol_version'] == 30

config['protocol_version'] = 31

json.dump(config, open(os.path.join(output_home, 'output.json'), 'w'), indent=2)
