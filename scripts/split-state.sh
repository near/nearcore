#!/bin/bash

## Split near/res/state into several at most 40M files to fit github
split -b 40M near/res/state near/res/state.