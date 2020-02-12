#!/bin/bash

## Split near/res/state into several at most 40M files to fit github
split -b 40000000 near/res/testnet_records.json near/res/testnet_records.json.
