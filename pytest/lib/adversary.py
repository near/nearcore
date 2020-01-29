import os, subprocess

ADVERSARIAL_TEST = False

HONEST_PATH = "../target/debug"
ADVERSARIAL_PATH = "./.adversary/debug"

def prepare_adversarial_binary():
    honest_near = os.path.join(HONEST_PATH, "near")
    adversarial_near = os.path.join(ADVERSARIAL_PATH, "near")
    assert os.path.exists(honest_near)

    need_to_recompile = False
    if not os.path.exists(adversarial_near):
        print("Adversarial binary is missing, recompiling")
        need_to_recompile = True
    else:
        honest_mtime = os.path.getmtime(honest_near)
        adversarial_mtime = os.path.getmtime(adversarial_near)

        if honest_mtime > adversarial_mtime:
            print("Adversarial binary is older than honest, recompiling")
            need_to_recompile = True

    if need_to_recompile:
        print("... Building adversarial node")
        build = subprocess.Popen(["cargo", "build", "-p", "near", "--target-dir", ".adversary", "--features", "adversarial"])
        out, err = build.communicate()
        assert 0 == build.returncode, err

def setup_adversarial_test():
    global ADVERSARIAL_TEST
    ADVERSARIAL_TEST = True

    prepare_adversarial_binary()
