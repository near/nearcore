import os, subprocess, sys

if not os.path.exists('.consent'):
    print("+---------------------------------------------------+", file=sys.stderr)
    print("+ You are about to run a test that uses adversarial +", file=sys.stderr)
    print("+ infrastructure. The infrastructure uses some git  +", file=sys.stderr)
    print("+ manipulations, including an invocation of         +", file=sys.stderr)
    print("+                                                   +", file=sys.stderr)
    print("+     git reset --hard HEAD~                        +", file=sys.stderr)
    print("+                                                   +", file=sys.stderr)
    print("+ after cherry-picking. While Alex who wrote this   +", file=sys.stderr)
    print("+ terrible code made everything possible to his     +", file=sys.stderr)
    print("+ best knowledge to prevent loss of work, you need  +", file=sys.stderr)
    print("+ to understand that Alex has no idea how git works +", file=sys.stderr)
    print("+ and that any uncommitted changes can be lost      +", file=sys.stderr)
    print("+ after running such tests.                         +", file=sys.stderr)
    print("+                                                   +", file=sys.stderr)
    print("+ Create a file named `.consent` in `pytest` dir to +", file=sys.stderr)
    print("+ get rid of this message and actually run the test +", file=sys.stderr)
    print("+---------------------------------------------------+", file=sys.stderr)
    sys.exit(1)

# A branch name or commit hash of the adversarial branch
# The value will be passed to `git cherrypick <here>` command.
ADVERSARIAL_BRANCH = 'adversary'

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
        # Test that there are no uncommitted changes
        check_uncommitted = subprocess.Popen(["git", "diff-index", "--quiet", "HEAD", "--"])
        out, err = check_uncommitted.communicate()
        assert 0 == check_uncommitted.returncode, "Make sure you have no uncommitted changes before running any adversarial tests.\n%s" % err

        print("... Cherry-picking the adversarial branch")
        cherrypick = subprocess.Popen(["git", "cherry-pick", ADVERSARIAL_BRANCH])
        out, err = cherrypick.communicate()
        assert 0 == cherrypick.returncode, err

        print("... Building adversarial node")
        build = subprocess.Popen(["cargo", "build", "-p", "near", "--target-dir", ".adversary"])
        out, err = build.communicate()
        assert 0 == build.returncode, err

        # Hope that `diff-index` above was sufficiently reliable and no changes get lost here
        print("... Git-resetting HARD to HEAD~.")
        git_reset = subprocess.Popen(["git", "reset", "--hard", "HEAD~"])
        out, err = git_reset.communicate()
        assert 0 == git_reset.returncode, err

def corrupt_node(node):
    if not os.path.exists('.adv_on_top'):
        assert 'LocalNode' in str(type(node)), "Node type %s != LocalNode. There's no support for remote nodes yet." % str(type(node))
        prepare_adversarial_binary()

        print("Corrupting node %s:%s" % node.addr())
        node.near_root = ADVERSARIAL_PATH

