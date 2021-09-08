class StateRootNode:
    pass


shard_schema = [[
    StateRootNode, {
        'kind': 'struct',
        'fields': [['data', ['u8']], ['memory_usage', 'u64']]
    }
]]
