#!/usr/bin/env python

import nodelib

if __name__ == "__main__":
    print("Stopping NEAR docker containers...")
    nodelib.stop_docker()
