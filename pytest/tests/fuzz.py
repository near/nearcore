"""Wrapper starting `cargo-fuzz` based fuzzers."""

import os
import pathlib
import shlex
import signal
import subprocess
import sys
import typing

import psutil


def run(directory: str, fuzz_target: str, timeout=typing.Optional[int]) -> int:
    """Executes `cargo fuzz run` on given target in given directory.

    Args:
        directory: Directory within the repository to execute the `cargo fuzz`
            command in.
        fuzz_target: The `cargo fuzz run` target.
        timeout: Optional timeout in seconds denoting maximum time the fuzzer
            can run.  If given, the fuzzer will be killed after given time
            passes and run is considered a success, i.e. function returns 0.

    Returns:
        The exit status of the fuzzer except that if timeout is specified and
        fuzzer runs over allotted interval it is killed and the function returns
        zero denoting success.  Similarly, if script is interrupted with Ctrl+C,
        the fuzzer is killed and run is considered a success.

        This behaviour is because the fuzzer normally runs until it finds
        a failure at which point it exits with non-zero exit code.
    """
    cwd = pathlib.Path(__file__).resolve().parents[2] / directory
    args = ('cargo', 'fuzz', 'run', fuzz_target, '--', '-len_control=0'
            '-prefer_small=0', '-max_len=4000000', '-rss_limit_mb=10240')
    os.environ['RUSTC_BOOTSTRAP'] = '1'

    # libfuzzer has a -max_total_time flag however it does not measure time
    # compilation takes.  Because of that, rather than relying on that option
    # we’re handling timeout over the entire command ourselves.  We’re still
    # using the option though just to make sure the process terminates if we
    # don’t handle timeout correctly.
    if timeout:
        args += (f'-max_total_time={timeout}',)

    cmd = ' '.join(shlex.quote(str(arg)) for arg in args)
    sys.stderr.write(f'+ ( cd {shlex.quote(str(cwd))} && {cmd} )\n')

    # Secondly, we’re not using `subprocess.call(..., timeout=...)` because that
    # sends KILL signal only to the process spawned by the call and doesn’t kill
    # its children.  So instead, we’re handling the timeout ourselves and
    # killing the whole process tree.
    with subprocess.Popen(args, cwd=cwd) as proc:
        try:
            return proc.wait(timeout=timeout)
        except (KeyboardInterrupt, subprocess.TimeoutExpired):
            _kill_process_tree(proc.pid)
            print('No failures found.')
            return 0


def _kill_process_tree(pid: int) -> None:
    """Kills a process tree (i.e. process and all its decedents)."""

    proc = psutil.Process(pid)
    procs = proc.children(recursive=True) + [proc]
    for proc in procs:
        try:
            proc.send_signal(signal.SIGKILL)
        except psutil.NoSuchProcess:
            pass


def _get_timeout() -> typing.Optional[int]:
    """Returns test timeout configured on NayDuck

    Returns:
        When run on NayDuck, returns timeout in seconds that NayDuck gives us to
        execute.  If we run over that time, the test will be reported as timed
        out.  When not running on NayDuck, returns None denoting no timeout.
    """
    timeout = os.environ.get('NAYDUCK_TIMEOUT')
    if timeout:
        try:
            n = int(timeout)
            if n > 60:
                # Reserve five seconds for the time this script takes; it’s way
                # more than enough.
                return n - 5
        except ValueError:
            pass
        print(f'Invalid NAYDUCK_TIMEOUT value ‘{timeout}’, ignoring.',
              file=sys.stderr)
    print(
        'No valid NAYDUCK_TIMEOUT environment variable found.\n'
        'Test will run until failure is found or it’s interrupted.',
        file=sys.stderr)
    return None


def main(argv: typing.Sequence[str]) -> typing.Union[int, str, None]:
    if len(argv) != 3:
        return f'usage: {argv[0]} <directory> <fuzz-target>'
    _, directory, fuzz_target = argv
    return run(directory, fuzz_target, timeout=_get_timeout())


if __name__ == '__main__':
    sys.exit(main(sys.argv))
