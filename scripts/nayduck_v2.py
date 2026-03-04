#!/usr/bin/env python3
"""Nayduck CLI — inspect runs, view logs, schedule and manage CI tests.

A command-line interface for the NayDuck CI system used by nearcore.
See https://nayduck.nearone.org/ for the web interface.

Subcommands
-----------

runs     List recent NayDuck runs with pass/fail summaries.
         Filter by branch substring, PR number, or limit count.

run      Inspect a specific run: see every test with its status, duration,
         and test ID. Use --failed to focus on non-passing tests only.

test     Deep-dive into a single test: metadata, retry count, pass/fail
         history, bisect info (first bad / last good commit), and a list
         of all available log files with sizes.

logs     Fetch and display log content for a test. Auto-selects the log
         with a stack trace (falling back to stderr). Supports --type to
         pick a specific log, --all to dump every log, --tail N to show
         only the last N lines, and --output FILE to save to disk.

history  Show how a test has performed across recent runs: pass rate,
         per-run status, and durations. Filter to a specific branch
         with --branch.

nightly  Quick-check the last nightly run. Same output as 'run' but
         automatically resolves the most recent nightly run ID.

build    Show build metadata (status, features, release mode, duration)
         and optionally display build stdout/stderr with --logs.

schedule Submit a new test run to NayDuck. Reads test specs from a file
         (default: nightly/nightly.txt) or stdin. Resolves branch and
         SHA from the local git repo. Use --dry-run to preview without
         submitting.

cancel   Cancel all PENDING tests and builds in a run. Already running
         tests are not affected.

retry    Re-queue FAILED and TIMEOUT tests in a run for re-execution.

Usage examples
--------------

    # Browse recent runs, find runs for a specific PR
    nayduck_v2.py runs
    nayduck_v2.py runs --limit 10
    nayduck_v2.py runs --pr 15238
    nayduck_v2.py runs --branch master

    # Inspect a run, filter to failures
    nayduck_v2.py run 4078
    nayduck_v2.py run 4078 --failed

    # Get machine-readable JSON for scripting
    nayduck_v2.py run 4078 --json

    # Deep-dive into a specific test
    nayduck_v2.py test 1202517

    # View logs — auto-picks the stack trace log
    nayduck_v2.py logs 1202517
    # View stderr specifically, last 100 lines
    nayduck_v2.py logs 1202517 --type stderr --tail 100
    # Dump all logs to files
    nayduck_v2.py logs 1202517 --all --output /tmp/test_logs/

    # Check test flakiness across runs
    nayduck_v2.py history 1202517
    nayduck_v2.py history 1202517 --branch master

    # Check the latest nightly
    nayduck_v2.py nightly
    nayduck_v2.py nightly --failed

    # Inspect a build (useful when tests fail at build stage)
    nayduck_v2.py build 12660
    nayduck_v2.py build 12660 --logs

    # Schedule a new test run
    nayduck_v2.py schedule --branch my-branch --test-file nightly/ci.txt
    nayduck_v2.py schedule --dry-run --test-file nightly/ci.txt
    echo "pytest sanity/block_production.py" | nayduck_v2.py schedule --stdin

    # Manage existing runs
    nayduck_v2.py retry 4078
    nayduck_v2.py cancel 4078
"""

import argparse
import getpass
import json
import os
import pathlib
import requests
import subprocess
import sys
import time
import typing
from datetime import timedelta

NAYDUCK_BASE_HREF = "https://nayduck.nearone.org"
REPO_DIR = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_TEST_FILE = "nightly/nightly.txt"

STATUS_COLORS = {
    "PASSED": "green",
    "FAILED": "red",
    "TIMEOUT": "red",
    "BUILD FAILED": "red",
    "CHECKOUT FAILED": "red",
    "SCP FAILED": "red",
    "RUNNING": "cyan",
    "BUILDING": "cyan",
    "PENDING": "yellow",
    "IGNORED": "yellow",
    "CANCELED": "yellow",
    "SKIPPED": "yellow",
    "BUILD DONE": "green",
}
FAIL_STATUSES = {
    "FAILED", "TIMEOUT", "BUILD FAILED", "CHECKOUT FAILED", "SCP FAILED"
}

# ANSI codes, empty when piped.
_ANSI = {
    "red": "\033[31m",
    "green": "\033[32m",
    "yellow": "\033[33m",
    "cyan": "\033[36m",
    "bold": "\033[1m",
    "reset": "\033[0m",
}
S: dict = _ANSI if sys.stdout.isatty() else {k: "" for k in _ANSI}


class NayduckError(Exception):
    """Raised when a NayDuck API call fails."""


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _colored(status: str) -> str:
    """Wrap a status string in its color code."""
    color = STATUS_COLORS.get(status)
    return f"{S.get(color, '')}{status}{S['reset']}" if color else status


def _duration(start_ms, end_ms) -> str:
    """Format a ms time range as 'H:MM:SS', or '-' if missing."""
    if start_ms is None or end_ms is None:
        return "-"
    return str(timedelta(milliseconds=max(0, end_ms - start_ms))).split(".")[0]


def _time_ago(timestamp_ms) -> str:
    """Format a ms timestamp as relative time like '3h ago'."""
    if timestamp_ms is None:
        return "-"
    delta = timedelta(milliseconds=max(0,
                                       int(time.time() * 1000) - timestamp_ms))
    if delta.days:
        return f"{delta.days}d ago"
    hours = delta.seconds // 3600
    if hours:
        return f"{hours}h ago"
    return f"{delta.seconds // 60}m ago"


def _trunc(text: str, width: int) -> str:
    """Truncate text to width, adding '..' if too long."""
    return text[:width - 2] + ".." if len(text) > width else text


def _table(headers, rows, widths):
    """Print a simple aligned table."""
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(S["bold"] + fmt.format(*headers) + S["reset"])
    print("-" * (sum(widths) + 2 * (len(widths) - 1)))
    for row in rows:
        print(fmt.format(*[_trunc(str(c), w) for c, w in zip(row, widths)]))


# ---------------------------------------------------------------------------
# Infrastructure (auth + API)
# ---------------------------------------------------------------------------


def _github_auth(code_path: pathlib.Path) -> str:
    """Prompt user to authenticate via NayDuck's GitHub OAuth flow."""
    print(f"Go to the following link in your browser:\n\n"
          f"{NAYDUCK_BASE_HREF}/login/cli\n")
    code = getpass.getpass("Enter authorization code: ")
    code_path.parent.mkdir(parents=True, exist_ok=True)
    code_path.write_text(code)
    return code


def _get_auth_code() -> typing.Tuple[pathlib.Path, str]:
    """Load NayDuck auth code from ~/.config/nayduck-code, or prompt."""
    code_path = (pathlib.Path(
        os.environ.get("XDG_CONFIG_HOME") or pathlib.Path.home() / ".config") /
                 "nayduck-code")
    if code_path.is_file():
        return code_path, code_path.read_text().strip()
    return code_path, _github_auth(code_path)


def _api_get(path: str) -> dict:
    """Unauthenticated GET to NayDuck API. Raises NayduckError on failure."""
    try:
        res = requests.get(NAYDUCK_BASE_HREF + path, timeout=30)
    except requests.RequestException as e:
        raise NayduckError(f"connection error: {e}") from e
    if res.status_code != 200:
        raise NayduckError(f"API {res.status_code} for {path}: {res.text}")
    return res.json()


def _api_post(path: str, **kwargs) -> dict:
    """Authenticated POST to NayDuck API. Re-prompts on 401."""
    code_path, code = _get_auth_code()
    while True:
        try:
            res = requests.post(NAYDUCK_BASE_HREF + path,
                                cookies={"nay-code": code},
                                timeout=30,
                                **kwargs)
        except requests.RequestException as e:
            raise NayduckError(f"connection error: {e}") from e
        if res.status_code != 401:
            break
        print(f"{S['red']}Unauthorized.{S['reset']}\n")
        code = _github_auth(code_path)
    new_code = res.cookies.get("nay-code")
    if new_code:
        code_path.write_text(new_code)
    if res.status_code != 200:
        raise NayduckError(f"API {res.status_code}: {res.text}")
    return res.json()


def _fetch_log(url: str) -> str:
    """Fetch raw log content from a URL. Raises NayduckError on failure."""
    try:
        res = requests.get(url, timeout=60)
    except requests.RequestException as e:
        raise NayduckError(f"error fetching log: {e}") from e
    if res.status_code != 200:
        raise NayduckError(f"log fetch failed ({res.status_code}): {url}")
    return res.text


def _log_url(storage: str) -> str:
    """Resolve a log storage path to a full URL."""
    return storage if storage.startswith(
        "http") else NAYDUCK_BASE_HREF + storage


# ---------------------------------------------------------------------------
# Domain helpers (test files + git)
# ---------------------------------------------------------------------------


def _read_tests_from_file(path: pathlib.Path) -> typing.List[str]:
    """Read test specs from a file, handling ./path includes (max 3 levels)."""
    return list(_read_tests_impl(path.read_text().splitlines(), path.parent, 1))


def _read_tests_from_stdin() -> typing.List[str]:
    """Read test specs from stdin, resolving includes relative to cwd."""
    return list(_read_tests_impl(sys.stdin, pathlib.Path.cwd(), 1))


def _read_tests_impl(lines, dirpath, depth) -> typing.Iterable[str]:
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("./"):
            if depth >= 4:
                print(f"Warning: ignoring {line}; max include depth",
                      file=sys.stderr)
                continue
            inc = dirpath / line
            yield from _read_tests_impl(inc.read_text().splitlines(),
                                        inc.parent, depth + 1)
        else:
            yield line


def _git_sha(ref: str) -> str:
    """Resolve a git ref to a commit SHA."""
    try:
        return subprocess.check_output(["git", "rev-parse", ref],
                                       text=True).strip()
    except subprocess.CalledProcessError:
        remote = "remotes/origin/" + ref
        print(f"Local ref '{ref}' not found, trying {remote}")
        return subprocess.check_output(["git", "rev-parse", remote],
                                       text=True).strip()


def _git_branch(sha: str = "") -> str:
    """Get branch name for a SHA, or current branch if empty."""
    if sha:
        return subprocess.check_output(["git", "name-rev", sha],
                                       text=True).strip().split(" ")[-1]
    return subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"],
                                   text=True).strip()


# ---------------------------------------------------------------------------
# API layer — pure data, no printing, no sys.exit
# ---------------------------------------------------------------------------


def api_get_runs(limit=20, branch=None, pr=None) -> typing.List[dict]:
    """Fetch recent runs, optionally filtered by branch or PR number."""
    data = _api_get(f"/api/runs?limit={min(limit, 100)}")
    filtered = f"pr-{pr}" if pr else branch
    if filtered:
        data = [r for r in data if filtered in r.get("branch", "")]
    return data[:limit]


def api_get_run(run_id: int) -> dict:
    """Fetch a run with all its tests."""
    return _api_get(f"/api/run/{run_id}")


def api_get_test(test_id: int) -> dict:
    """Fetch detailed test information."""
    return _api_get(f"/api/test/{test_id}")


def api_get_test_history(test_id: int, branch=None) -> dict:
    """Fetch pass/fail history for a test."""
    path = f"/api/test/{test_id}/history"
    return _api_get(f"{path}/{branch}" if branch else path)


def api_get_nightly() -> dict:
    """Fetch the most recent nightly run."""
    return _api_get("/api/run/nightly")


def api_get_build(build_id: int) -> dict:
    """Fetch build information."""
    return _api_get(f"/api/build/{build_id}")


def api_fetch_log(test_id: int, log_type=None, fetch_all=False):
    """Fetch log content. Returns list of (type, content) tuples."""
    logs = api_get_test(test_id).get("logs", [])
    if not logs:
        return []
    if fetch_all:
        pick = logs
    elif log_type:
        pick = [l for l in logs if l.get("type") == log_type]
        if not pick:
            avail = ", ".join(l.get("type", "?") for l in logs)
            raise NayduckError(f"no '{log_type}' log. Available: {avail}")
    else:
        # Auto-pick: stack trace > stderr > first.
        trace = [l for l in logs if l.get("stack_trace")]
        stderr = [l for l in logs if l.get("type") == "stderr"]
        pick = [trace[0]] if trace else [stderr[0]] if stderr else [logs[0]]
    results = []
    for l in pick:
        storage = l.get("storage", "")
        if not storage:
            raise NayduckError(f"log '{l.get('type', '?')}' has no storage URL")
        results.append((l.get("type", "?"), _fetch_log(_log_url(storage))))
    return results


def api_schedule_run(branch, sha, tests) -> dict:
    """Submit a new test run."""
    return _api_post("/api/run/new",
                     json={
                         "branch": branch,
                         "sha": sha,
                         "tests": tests
                     })


def api_cancel_run(run_id: int) -> dict:
    """Cancel pending tests in a run."""
    return _api_post(f"/api/run/{run_id}/cancel")


def api_retry_run(run_id: int) -> dict:
    """Retry failed tests in a run."""
    return _api_post(f"/api/run/{run_id}/retry")


# ---------------------------------------------------------------------------
# Display layer — human-readable output, no API calls
# ---------------------------------------------------------------------------


def display_runs(runs):
    """Print a table of recent runs."""
    if not runs:
        print("No runs found.")
        return
    headers = ["RUN ID", "BRANCH", "SHA", "PASS", "FAIL", "OTHER", "AGE"]
    widths = [8, 45, 9, 6, 6, 6, 10]
    rows = []
    for r in runs:
        passed = failed = other = 0
        for b in r.get("builds", []):
            t = b.get("tests", {})
            passed += t.get("passed", 0)
            failed += t.get("failed", 0) + t.get("timeout", 0)
            other += sum(v for k, v in t.items()
                         if k not in ("passed", "failed", "timeout"))
        fail_str = (f"{S['red']}{failed}{S['reset']}" if failed else "0")
        rows.append([
            str(r.get("run_id", "")),
            r.get("branch", ""),
            r.get("sha", "")[:7],
            str(passed), fail_str,
            str(other),
            _time_ago(r.get("timestamp"))
        ])
    _table(headers, rows, widths)
    print(f"\n{len(rows)} run(s) shown. View at {NAYDUCK_BASE_HREF}")


def display_run(data, failed_only=False):
    """Print run metadata and test table."""
    tests = data.get("tests", [])
    counts: dict = {}
    for t in tests:
        s = t.get("status", "UNKNOWN")
        counts[s] = counts.get(s, 0) + 1

    summary = ", ".join(f"{counts[s]} {_colored(s)}" for s in [
        "PASSED", "FAILED", "TIMEOUT", "RUNNING", "PENDING", "IGNORED",
        "CANCELED", "SKIPPED"
    ] if counts.get(s))

    print(f"\n{S['bold']}Run #{data.get('run_id', '?')}{S['reset']}")
    print(f"Branch: {data.get('branch', '?')}")
    print(f"SHA:    {data.get('sha', '?')[:12]}")
    print(f"Tests:  {summary} ({len(tests)} total)")
    print(f"URL:    {NAYDUCK_BASE_HREF}/#/run/{data.get('run_id', '')}\n")

    show = tests
    if failed_only:
        show = [
            t for t in tests if t.get("status") not in ("PASSED", "IGNORED")
        ]
        if not show:
            print("All tests passed!")
            return

    show.sort(key=lambda t:
              (0 if t.get("status") in FAIL_STATUSES else 1, t.get("name", "")))
    _table(["TEST ID", "STATUS", "DURATION", "NAME"], [[
        str(t.get("test_id", "")),
        _colored(t.get("status", "?")),
        _duration(t.get("started"), t.get("finished")),
        t.get("name", "")
    ] for t in show], [9, 18, 10, 60])


def display_test(data):
    """Print detailed test information."""
    print(f"\n{S['bold']}Test #{data.get('test_id', '?')}{S['reset']}")
    print(f"Name:     {data.get('name', '?')}")
    print(f"Status:   {_colored(data.get('status', '?'))}")
    print(f"Run:      #{data.get('run_id', '?')} ({data.get('branch', '?')})")
    print(f"SHA:      {data.get('sha', '?')[:12]}")
    print(f"Duration: {_duration(data.get('started'), data.get('finished'))}")
    print(f"Tries:    {data.get('tries', 0)}")
    h = data.get("history", [])
    if h:
        print(f"History:  {h[0]} passed, {h[2]} failed, {h[1]} other")
    for label, key in [("First bad", "first_bad"), ("Last good", "last_good")]:
        val = data.get(key)
        if val:
            print(f"{label}: {val}")
    logs = data.get("logs", [])
    if logs:
        print("\nAvailable logs:")
        for l in logs:
            size_kb = l.get("size", 0) / 1024
            trace = " *" if l.get("stack_trace") else ""
            print(f"  {l.get('type', '?'):12s} {size_kb:>8.0f} KB{trace}")
    print(f"\nView logs: nayduck_v2.py logs {data.get('test_id', '?')}")


def display_logs(entries, tail=None, output=None):
    """Print or save log content."""
    for log_type, content in entries:
        if tail:
            content = "\n".join(content.splitlines()[-tail:])
        if output:
            out = pathlib.Path(output)
            if out.is_dir() or output.endswith(os.sep):
                out.mkdir(parents=True, exist_ok=True)
                out = out / log_type
            elif len(entries) > 1:
                out = out.with_name(f"{out.stem}_{log_type}{out.suffix}")
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content)
            print(f"Saved {log_type} log to {out}")
        else:
            if len(entries) > 1:
                print(f"\n{S['bold']}=== {log_type} ==={S['reset']}")
            print(content)


def display_history(data, branch=None):
    """Print test pass/fail history."""
    print(f"\n{S['bold']}History: {data.get('name', '?')}{S['reset']}")
    if branch:
        print(f"Branch: {branch}")
    h = data.get("history", [])
    if h and len(h) >= 3:
        total = sum(h)
        if total:
            print(f"Summary: {h[0]} passed, {h[2]} failed, "
                  f"{h[1]} other ({h[0]*100//total}% pass rate)")
    entries = data.get("tests", [])
    if not entries:
        print("No history available.")
        return
    _table(["DATE", "STATUS", "TRIES", "DURATION", "SHA"], [[
        time.strftime("%Y-%m-%d", time.localtime(e["started"] / 1000))
        if e.get("started") else "-",
        _colored(e.get("status", "?")),
        str(e.get("tries", "")),
        _duration(e.get("started"), e.get("finished")),
        e.get("sha", "")[:12]
    ] for e in entries], [12, 18, 6, 10, 12])


def display_build(data, show_logs=False):
    """Print build metadata."""
    print(f"\n{S['bold']}Build #{data.get('build_id', '?')}{S['reset']}")
    print(f"Status:   {_colored(data.get('status', '?'))}")
    print(f"Run:      #{data.get('run_id', '?')}")
    print(f"Branch:   {data.get('branch', '?')}")
    print(f"SHA:      {data.get('sha', '?')[:12]}")
    print(f"Features: {data.get('features', '-')}")
    print(f"Duration: {_duration(data.get('started'), data.get('finished'))}")
    if show_logs:
        for stream in ["stderr", "stdout"]:
            content = data.get(stream)
            if content:
                print(f"\n{S['bold']}=== {stream} ==={S['reset']}")
                print(content)


# ---------------------------------------------------------------------------
# CLI layer (handlers + parser + main)
# ---------------------------------------------------------------------------


def cmd_runs(args):
    runs = api_get_runs(limit=args.limit, branch=args.branch, pr=args.pr)
    if args.json:
        print(json.dumps(runs, indent=2))
    else:
        display_runs(runs)


def cmd_run(args):
    data = api_get_run(args.run_id)
    if args.json:
        print(json.dumps(data, indent=2))
    else:
        display_run(data, failed_only=args.failed)


def cmd_test(args):
    data = api_get_test(args.test_id)
    if args.json:
        print(json.dumps(data, indent=2))
    else:
        display_test(data)


def cmd_logs(args):
    entries = api_fetch_log(args.test_id,
                            log_type=args.type,
                            fetch_all=args.all)
    if not entries:
        print(f"No logs for test #{args.test_id}.")
        return
    display_logs(entries, tail=args.tail, output=args.output)


def cmd_history(args):
    data = api_get_test_history(args.test_id, branch=args.branch)
    if args.json:
        print(json.dumps(data, indent=2))
    else:
        display_history(data, branch=args.branch)


def cmd_nightly(args):
    data = api_get_nightly()
    if not data:
        print("No nightly run found.")
        return
    if args.json:
        print(json.dumps(data, indent=2))
    else:
        display_run(data, failed_only=args.failed)


def cmd_build(args):
    data = api_get_build(args.build_id)
    if args.json:
        print(json.dumps(data, indent=2))
    else:
        display_build(data, show_logs=args.logs)


def cmd_schedule(args):
    if args.stdin:
        tests = _read_tests_from_stdin()
    else:
        test_path = pathlib.Path(args.test_file)
        if not test_path.is_file():
            print(f"Test file not found: {test_path}", file=sys.stderr)
            sys.exit(1)
        tests = _read_tests_from_file(test_path)
    if not tests:
        print("No tests found.", file=sys.stderr)
        sys.exit(1)
    if args.dry_run:
        print(f"Would schedule {len(tests)} test(s):")
        for t in tests:
            print(f"  {t}")
        return
    branch = args.branch
    sha = _git_sha(branch) if branch else (args.sha or _git_sha("HEAD"))
    if not branch:
        branch = _git_branch(sha)
    print(f"Scheduling {len(tests)} test(s) on {branch} ({sha[:12]})")
    result = api_schedule_run(branch, sha, tests)
    msg = result.get("response", str(result))
    color = "green" if result.get("code") == 0 else "red"
    print(f"{S[color]}{msg}{S['reset']}")


def _run_action(run_id, action, verb):
    """Shared cancel/retry handler."""
    fn = api_cancel_run if action == "cancel" else api_retry_run
    result = fn(run_id)
    count = result if isinstance(result, int) else result.get("count", result)
    print(f"{verb} {count} test(s) in run #{run_id}.")


def cmd_cancel(args):
    _run_action(args.run_id, "cancel", "Cancelled")


def cmd_retry(args):
    _run_action(args.run_id, "retry", "Retried")


def _build_parser():
    parser = argparse.ArgumentParser(
        prog="nayduck_v2.py",
        description="NayDuck CLI — inspect runs, view logs, schedule and "
        "manage CI tests.",
        epilog=("Examples:\n"
                "  %(prog)s runs                        List recent runs\n"
                "  %(prog)s runs --pr 15238              Runs for a PR\n"
                "  %(prog)s run 4078 --failed            Show failures\n"
                "  %(prog)s test 1202517                 Test details\n"
                "  %(prog)s logs 1202517 --tail 100      Last 100 lines\n"
                "  %(prog)s history 1202517              Pass/fail history\n"
                "  %(prog)s schedule -t nightly/ci.txt   Schedule tests\n"
                "\nWeb UI: https://nayduck.nearone.org/"),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="subcommand", title="subcommands")

    p = sub.add_parser("runs", help="List recent runs.")
    p.add_argument("--limit",
                   type=int,
                   default=20,
                   help="Max runs (default 20).")
    p.add_argument("--branch", help="Filter by branch substring.")
    p.add_argument("--pr", type=int, help="Filter by PR number.")
    p.add_argument("--json", action="store_true", help="Output raw JSON.")
    p.set_defaults(func=cmd_runs)

    p = sub.add_parser("run", help="Show a specific run.")
    p.add_argument("run_id", type=int, help="Run ID.")
    p.add_argument("--failed", action="store_true", help="Only failures.")
    p.add_argument("--json", action="store_true", help="Output raw JSON.")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("test", help="Show test details.")
    p.add_argument("test_id", type=int, help="Test ID.")
    p.add_argument("--json", action="store_true", help="Output raw JSON.")
    p.set_defaults(func=cmd_test)

    p = sub.add_parser("logs", help="View or download test logs.")
    p.add_argument("test_id", type=int, help="Test ID.")
    p.add_argument("--type", dest="type", help="Log type (e.g. stderr).")
    p.add_argument("--all", action="store_true", help="All log types.")
    p.add_argument("--tail", type=int, help="Last N lines only.")
    p.add_argument("--output", "-o", help="Write to file/dir.")
    p.set_defaults(func=cmd_logs)

    p = sub.add_parser("history", help="Test pass/fail history.")
    p.add_argument("test_id", type=int, help="Test ID.")
    p.add_argument("--branch", help="Filter to branch.")
    p.add_argument("--json", action="store_true", help="Output raw JSON.")
    p.set_defaults(func=cmd_history)

    p = sub.add_parser("nightly", help="Show last nightly run.")
    p.add_argument("--failed", action="store_true", help="Only failures.")
    p.add_argument("--json", action="store_true", help="Output raw JSON.")
    p.set_defaults(func=cmd_nightly)

    p = sub.add_parser("build", help="Show build info.")
    p.add_argument("build_id", type=int, help="Build ID.")
    p.add_argument("--logs", action="store_true", help="Show build logs.")
    p.add_argument("--json", action="store_true", help="Output raw JSON.")
    p.set_defaults(func=cmd_build)

    default_test_path = REPO_DIR / DEFAULT_TEST_FILE
    p = sub.add_parser("schedule", help="Schedule a new test run.")
    p.add_argument("--branch", "-b", help="Branch (default: current).")
    p.add_argument("--sha", "-s", help="Commit SHA (default: HEAD).")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--test-file",
                   "-t",
                   default=str(default_test_path),
                   help=f"Test file (default: {DEFAULT_TEST_FILE}).")
    g.add_argument("--stdin",
                   "-i",
                   action="store_true",
                   help="Read tests from stdin.")
    p.add_argument("--dry-run",
                   "-n",
                   action="store_true",
                   help="Preview without scheduling.")
    p.set_defaults(func=cmd_schedule)

    p = sub.add_parser("cancel", help="Cancel pending tests.")
    p.add_argument("run_id", type=int, help="Run ID.")
    p.set_defaults(func=cmd_cancel)

    p = sub.add_parser("retry", help="Retry failed tests.")
    p.add_argument("run_id", type=int, help="Run ID.")
    p.set_defaults(func=cmd_retry)

    return parser


def main():
    parser = _build_parser()
    args = parser.parse_args()
    if not args.subcommand:
        parser.print_help()
        sys.exit(1)
    try:
        args.func(args)
    except NayduckError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
