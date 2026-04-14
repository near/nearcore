#!/bin/bash
set -e

# Install nearcore pytest requirements in a venv
if [ -f /workspace/pytest/requirements.txt ]; then
  /opt/nearcore-venv/bin/python -m pip install --upgrade pip
  /opt/nearcore-venv/bin/python -m pip install --no-cache-dir -r /workspace/pytest/requirements.txt
fi
