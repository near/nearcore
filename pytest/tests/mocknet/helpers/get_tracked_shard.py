"""
This script is used to get the shard that is being tracked by the node.
"""

import requests
import re
import sys


def get_tracked_shard():
    try:
        response = requests.get('http://localhost:3030/metrics')
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error: Failed to fetch metrics: {e}", file=sys.stderr)
        sys.exit(1)

    # Find the line with tracked shards
    for line in response.text.splitlines():
        if 'near_client_tracked_shards' in line and 'shard_id=' in line and ' 1' in line:
            match = re.search(r'shard_id="(\d+)"', line)
            if match:
                shard = match.group(1)
                if not shard.isdigit():
                    print("Error: Invalid shard index format", file=sys.stderr)
                    print(f"shard={shard}", file=sys.stderr)
                    sys.exit(1)
                return shard

    # If we get here, no valid shard was found
    print("Error: Failed to get valid shard index from metrics",
          file=sys.stderr)
    sys.exit(1)


def main():
    print(get_tracked_shard())


if __name__ == "__main__":
    main()
