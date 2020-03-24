
import os
import subprocess


def current_branch():
    return subprocess.check_output([
        "git", "rev-parse", "--symbolic-full-name", "--abbrev-ref", "HEAD"]).strip().decode()


def compile_binary(branch):
    """For given branch, compile binary.

    Stashes current changes, switches branch and then returns everything back.
    """
    # TODO: download pre-compiled binary from github for beta/stable?
    prev_branch = current_branch()
    stash_output = subprocess.check_output(['git', 'stash'])
    subprocess.check_output(['git', 'checkout', branch])
    subprocess.check_output(['cargo', 'build', '-p', 'near'])
    subprocess.check_output(['cargo', 'build', '-p', 'state-viewer'])
    os.rename('../target/debug/near', '../target/debug/near-%s' % branch)
    os.rename('../target/debug/state-viewer', '../target/debug/state-viewer-%s' % branch)
    print(f'=== prev_branch: {prev_branch}')
    subprocess.check_output(['git', 'checkout', prev_branch])
    if stash_output != b"No local changes to save\n":
        subprocess.check_output(['git', 'stash', 'pop'])


def prepare_ab_test(other_branch):
    name = current_branch()
    compile_binary(name)
    compile_binary(other_branch)
    return '../target/debug/', [other_branch, name]
