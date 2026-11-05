"""Waits for other AI reviewers to finish before the NEAR AI review starts.

Greptile and the Claude review start on the same PR event as this reviewer.
The model is told not to repeat points from existing discussions, so their
comments must exist before the discussions are fetched. Each reviewer is
identified by its check run on the PR head commit.

Usage (from .github/scripts):
    python3 -m nearai_review.wait --repo near/nearcore --pr 123 \\
        --check "greptile-apps:Greptile Review" \\
        --check "github-actions:claude-review"

Waiting is best effort: the exit code is always 0.
"""

import argparse
import dataclasses
import os
import sys
import time

from . import github


@dataclasses.dataclass(frozen=True)
class Target:
    app: str
    name: str

    @classmethod
    def parse(cls, spec: str) -> "Target":
        app, sep, name = spec.partition(":")
        if not sep or not app or not name:
            raise ValueError(f"expected APP_SLUG:CHECK_NAME, got {spec!r}")
        return cls(app, name)

    def __str__(self) -> str:
        return f"{self.app}:{self.name}"


def wait_for_checks(gh: github.GitHub,
                    head_sha: str,
                    targets: list[Target],
                    max_wait: float,
                    appear_grace: float,
                    poll: float,
                    clock=time.monotonic,
                    sleep=time.sleep,
                    log=print) -> dict[str, str]:
    """Polls until every target's check run on `head_sha` has completed.

    A target whose check run does not appear within `appear_grace` seconds
    is assumed not to review this PR. Returns the final state per target:
    completed, absent or pending.
    """
    start = clock()
    states = {str(t): "pending" for t in targets}
    while True:
        elapsed = clock() - start
        for target in targets:
            if states[str(target)] != "pending":
                continue
            runs = [
                r for r in gh.check_runs(head_sha, target.name)
                if (r.get("app") or {}).get("slug") == target.app
            ]
            if runs and all(r.get("status") == "completed" for r in runs):
                states[str(target)] = "completed"
                log(f"{target} completed after {int(elapsed)} s")
            elif not runs and elapsed >= appear_grace:
                states[str(target)] = "absent"
                log(f"{target} did not start within {int(appear_grace)} s; "
                    "not waiting for it")
        if "pending" not in states.values():
            return states
        if elapsed >= max_wait:
            log(f"stopped waiting after {int(elapsed)} s: {states}")
            return states
        sleep(poll)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python3 -m nearai_review.wait")
    parser.add_argument("--repo", required=True)
    parser.add_argument("--pr", required=True, type=int)
    parser.add_argument("--check",
                        action="append",
                        required=True,
                        type=Target.parse,
                        help="APP_SLUG:CHECK_NAME, can be repeated")
    parser.add_argument("--max-wait", type=float, default=15 * 60)
    parser.add_argument("--grace", type=float, default=3 * 60)
    parser.add_argument("--poll", type=float, default=30)
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    gh = github.GitHub(os.environ.get("GH_TOKEN") or
                       os.environ.get("GITHUB_TOKEN"),
                       args.repo,
                       dry_run=True)
    try:
        head_sha = gh.get_pr(args.pr)["head"]["sha"]
        wait_for_checks(gh, head_sha, args.check, args.max_wait, args.grace,
                        args.poll)
    except (github.GitHubError, KeyError, TypeError) as e:
        print(f"::warning::could not wait for other reviewers: {e}",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
