"""
Protocol updates correct computation of inflation and total supply according to NEP. No changes to state records.
"""

assert config['protocol_version'] == 9

config['protocol_version'] = 10
config['online_max_threshold'] = [99, 100]
config['online_min_threshold'] = [90, 100]
