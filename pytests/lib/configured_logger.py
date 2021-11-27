import logging
import uuid
import sys

from typing import Optional

# LogLevel type since logging lib doesn't define its own enum/type for it
LogLevel = int


def new_logger(
    name: Optional[str] = None,
    level: LogLevel = logging.DEBUG,
    outfile: Optional[str] = None,
) -> logging.Logger:
    """
    Create a new configured logger. Used mainly by pytests.

    :param name: The name of the logger. Defaults to "test".
    :param level: The logging level. Defaults to DEBUG to log everything.
    :param outfile: Optional to set. When set, will log to a file instead of stdout.
    :return: The configured logger.
    """
    # If name is not specified, create one so that this can be a separate logger.
    if name is None:
        name = f"logger_{uuid.uuid1()}"

    log = logging.getLogger(name)
    log.setLevel(level)
    fmt = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s',
                            '%Y-%m-%d %H:%M:%S')

    if outfile is not None:
        handler = logging.FileHandler(outfile)
    else:
        handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(fmt)

    log.addHandler(handler)
    log.propagate = False

    return log


# global logger for testing purposes:
logger = new_logger("test")
