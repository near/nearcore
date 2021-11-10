class Proof:
    pass


bridge_schema = [[
    Proof, {
        'kind':
            'struct',
        'fields': [
            ['log_index', 'u64'],
            ['log_entry_data', ['u8']],
            ['receipt_index', 'u64'],
            ['receipt_data', ['u8']],
            ['header_data', ['u8']],
            ['proof', [['u8']]],
        ]
    }
]]
