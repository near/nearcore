import sys
sys.path.append('lib')
from configured_logger import logger


logger.info("I am running!")

print ('Argument List:', str(sys.argv))
