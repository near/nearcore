from .block import block_schema
from .crypto import crypto_schema
from .network import network_schema
from .shard import shard_schema
from .tx import tx_schema

schema = dict(block_schema + crypto_schema + network_schema + shard_schema + tx_schema)
