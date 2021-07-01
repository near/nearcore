#!/usr/bin/env python3

import nodelib

if __name__ == "__main__":
    print("Stopping NEAR docker containers...")
    nodelib.stop_docker()
