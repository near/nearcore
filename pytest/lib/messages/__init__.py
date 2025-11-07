from .block import block_schema
from .bridge import bridge_schema
from .crypto import crypto_schema
from .shard import shard_schema
from .tx import tx_schema

schema = dict(block_schema + bridge_schema + crypto_schema + shard_schema +
              tx_schema)
