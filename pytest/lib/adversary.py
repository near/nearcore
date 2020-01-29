# For adversarial test make sure to always call `setup_adversarial_test`
# at the beginning to use adversarial compiled binary.
#
# ```
# from adversary import setup_adversarial_test
# setup_adversarial_test()
# ```
import subprocess

ADVERSARIAL_TEST = False
ADVERSARIAL_PATH = "./.adversary/debug"


def prepare_adversarial_binary():
    print("... Building adversarial node")
    build = subprocess.Popen(["cargo", "build", "-p", "near", "--target-dir", ".adversary", "--features", "adversarial"])
    out, err = build.communicate()
    assert 0 == build.returncode, err


def setup_adversarial_test():
    global ADVERSARIAL_TEST
    ADVERSARIAL_TEST = True

    prepare_adversarial_binary()
