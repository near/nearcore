
import os
import subprocess


def current_branch():
    return subprocess.check_output([
        "git", "rev-parse", "--abbrev-ref", "HEAD"]).strip().decode()


def compile_binary(branch):
    """For given branch, compile binary."""
    # TODO: download pre-compiled binary from github for beta/stable?
    prev_branch = current_branch()
    subprocess.call(['git', 'checkout', branch])
    subprocess.call(['cargo', 'build', '-p', 'near'])
    subprocess.call(['cargo', 'build', '-p', 'state-viewer'])
    os.rename('../target/debug/near', '../target/debug/near-%s' % branch)
    os.rename('../target/debug/state-viewer', '../target/debug/state-viewer-%s' % branch)
    subprocess.call(['git', 'checkout', prev_branch])


def prepare_ab_test(other_branch):
    name = current_branch()
    compile_binary(name)
    compile_binary(other_branch)
    return '../target/debug', [other_branch, name]
