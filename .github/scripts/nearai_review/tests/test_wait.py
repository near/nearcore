import contextlib
import io
import unittest
from unittest import mock

from nearai_review import github, wait

GREPTILE = wait.Target("greptile-apps", "Greptile Review")
CLAUDE = wait.Target("github-actions", "claude-review")


def run(app, status):
    return {"app": {"slug": app}, "status": status}


class FakeChecks:
    """Returns scripted check runs per poll round, keyed by check name."""

    def __init__(self, rounds):
        self.rounds = rounds
        self.round = 0
        self.queries = []

    def check_runs(self, sha, name):
        self.queries.append((sha, name))
        rounds = self.rounds.get(name, [[]])
        return rounds[min(self.round, len(rounds) - 1)]


class Clock:

    def __init__(self, checks):
        self.now = 0.0
        self.checks = checks

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds
        self.checks.round += 1


def run_wait(rounds, targets=(GREPTILE, CLAUDE), max_wait=900, grace=180):
    checks = FakeChecks(rounds)
    clock = Clock(checks)
    logs = []
    states = wait.wait_for_checks(checks,
                                  "a" * 40,
                                  list(targets),
                                  max_wait,
                                  grace,
                                  30,
                                  clock=clock,
                                  sleep=clock.sleep,
                                  log=logs.append)
    return states, clock.now, checks, logs


class WaitTest(unittest.TestCase):

    def test_waits_until_both_complete(self):
        states, waited, checks, _ = run_wait({
            "Greptile Review": [[run("greptile-apps", "queued")],
                                [run("greptile-apps", "in_progress")],
                                [run("greptile-apps", "completed")]],
            "claude-review": [[run("github-actions", "in_progress")]] * 4 +
                             [[run("github-actions", "completed")]],
        })
        self.assertEqual(states, {
            str(GREPTILE): "completed",
            str(CLAUDE): "completed"
        })
        self.assertEqual(waited, 120)
        self.assertTrue(all(sha == "a" * 40 for sha, _ in checks.queries))

    def test_absent_reviewer_after_grace(self):
        states, waited, _, logs = run_wait({
            "Greptile Review": [[run("greptile-apps", "completed")]],
        })
        self.assertEqual(states[str(CLAUDE)], "absent")
        self.assertEqual(waited, 180)
        self.assertTrue(any("did not start" in line for line in logs))

    def test_late_start_inside_grace(self):
        states, waited, _, _ = run_wait(
            {
                "Greptile Review": [[], [], [run("greptile-apps", "completed")]
                                   ],
            },
            targets=[GREPTILE])
        self.assertEqual(states[str(GREPTILE)], "completed")
        self.assertEqual(waited, 60)

    def test_stops_at_max_wait(self):
        states, waited, _, logs = run_wait(
            {"Greptile Review": [[run("greptile-apps", "in_progress")]]},
            targets=[GREPTILE],
            max_wait=300)
        self.assertEqual(states[str(GREPTILE)], "pending")
        self.assertEqual(waited, 300)
        self.assertTrue(any("stopped waiting" in line for line in logs))

    def test_other_apps_with_same_check_name_ignored(self):
        states, _, _, _ = run_wait(
            {"claude-review": [[run("someone-else", "completed")]]},
            targets=[CLAUDE],
            grace=60)
        self.assertEqual(states[str(CLAUDE)], "absent")

    def test_target_parse(self):
        self.assertEqual(wait.Target.parse("greptile-apps:Greptile Review"),
                         GREPTILE)
        for bad in ("no separator", ":name", "app:"):
            with self.assertRaises(ValueError):
                wait.Target.parse(bad)


class MainTest(unittest.TestCase):

    def test_api_error_is_not_fatal(self):
        stderr = io.StringIO()
        with mock.patch.object(github.GitHub,
                               "get_pr",
                               side_effect=github.GitHubError(500, "down")), \
                contextlib.redirect_stderr(stderr):
            code = wait.main(["--repo", "r/r", "--pr", "1", "--check", "a:b"])
        self.assertEqual(code, 0)
        self.assertIn("could not wait", stderr.getvalue())

    def test_main_uses_pr_head(self):
        with mock.patch.object(github.GitHub,
                               "get_pr",
                               return_value={"head": {
                                   "sha": "c" * 40
                               }}), mock.patch.object(
                                   wait, "wait_for_checks") as waiter:
            wait.main([
                "--repo", "r/r", "--pr", "1", "--check", "a:b", "--max-wait",
                "60"
            ])
        args = waiter.call_args.args
        self.assertEqual(args[1], "c" * 40)
        self.assertEqual(args[2], [wait.Target("a", "b")])
        self.assertEqual(args[3], 60)


if __name__ == "__main__":
    unittest.main()
