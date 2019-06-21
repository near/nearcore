#!/usr/bin/python

import subprocess


if __name__ == "__main__":
    print("Starting unittest nodes with test.near account and seed key of alice.near")
    subprocess.call(['rm', '-rf', 'testdir'])
    subprocess.call([
        'cargo', 'run', '-p', 'near', '--', '--home=testdir/',
        'init', '--test-seed=alice.near', '--account-id=test.near', '--fast'])
    subprocess.call(['cargo', 'run', '-p', 'near', '--', '--home=testdir/', 'run'])
