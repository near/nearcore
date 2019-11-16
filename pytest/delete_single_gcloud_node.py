#!/usr/bin/env python
import argparse
from rc import gcloud

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', default='pytest-node', help='Name of the single gcloud node')
    args = parser.parse_args()
    m = gcloud.get(args.name)
    if m:
        print(f'Deleting instance {m.name}')
        m.delete()
        print('Deleted')