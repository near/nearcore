

from configured_logger import new_logger

L = new_logger(outfile="/tmp/something.txt")
L.info("womp")
