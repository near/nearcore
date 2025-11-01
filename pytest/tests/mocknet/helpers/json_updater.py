"""
This script is used to update a JSON file with one or more patch files.
"""

import os
import json
import sys
import argparse


def deep_merge(original: dict, patch: dict) -> dict:
    """Deep merge two dictionaries, recursively merging nested dictionaries.
    
    Args:
        original: The original dictionary to merge into
        patch: The dictionary containing updates to apply
        
    Returns:
        The merged dictionary
    """
    result = original.copy()

    for key, value in patch.items():
        if key not in result:
            result[key] = value
            continue

        assert isinstance(result[key], dict) == isinstance(value, dict)
        if isinstance(result[key], dict):
            # If both values are dictionaries, merge them recursively
            result[key] = deep_merge(result[key], value)
        else:
            # For non-dictionary values, overwrite with patch value
            result[key] = value

    return result


def update_json(original_path: str, patch_paths: list[str]):
    """Update original json with multiple patch json files if they all exist.
    
    Args:
        original_path: Path to the original JSON file
        patch_paths: List of paths to patch JSON files to apply in sequence
    """
    if not os.path.exists(original_path):
        print(f"Error: Original file {original_path} does not exist")
        sys.exit(1)

    for patch_path in patch_paths:
        if not os.path.exists(patch_path):
            print(f"Error: Patch file {patch_path} does not exist")
            sys.exit(1)

    with open(original_path) as f:
        original = json.load(f)

    for patch_path in patch_paths:
        with open(patch_path) as f:
            patch = json.load(f)
            original = deep_merge(original, patch)

    # TODO: set "gas_limit" to int everywhere once bench.sh is deprecated.
    # Was needed as a workaround for jq 1.6 bigint bug.
    if original_path.endswith('genesis.json'):
        original['gas_limit'] = int(original['gas_limit'])

    with open(original_path, 'w') as f:
        json.dump(original, f, indent=4)


def main():
    parser = argparse.ArgumentParser(
        description='Update a JSON file with one or more patch files.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        'original',
        help='Path to the original JSON file to update',
    )
    parser.add_argument(
        'patches',
        nargs='+',
        help='One or more patch JSON files to apply in sequence',
    )

    args = parser.parse_args()
    update_json(args.original, args.patches)


if __name__ == '__main__':
    main()
