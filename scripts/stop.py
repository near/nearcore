#!/usr/bin/env python3

import nodelib
import os
import sys

from pathlib import Path
sys.path.append(str(Path(os.path.abspath(__file__)).parent.parent / 'pytest/lib'))
from configured_logger import logger


if __name__ == "__main__":
    logger.info("Stopping NEAR docker containers...")
    nodelib.stop_docker()
