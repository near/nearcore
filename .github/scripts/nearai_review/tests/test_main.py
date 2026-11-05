import contextlib
import io
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock

import nearai_review.__main__ as cli
from nearai_review import agent, github, tools
from nearai_review.tests.test_agent import (GOOD_REVIEW, REPO_ROOT, FakeClient,
                                            call, reply)
from nearai_review.tests.test_diff import BASIC_PATCH

API_KEY = "sk-nearai-test-key-123"
GH_TOKEN = "ghs_test_token_456"


def pr(sha="a" * 40, state="open"):
    return {
        "number": 7,
        "state": state,
        "title": "Add y",
        "body": "",
        "user": {
            "login": "alice"
        },
        "base": {
            "ref": "master",
            "repo": {
                "full_name": "near/nearcore"
            }
        },
        "head": {
            "sha": sha
        },
    }


FILES = [{
    "filename": "src/lib.rs",
    "status": "modified",
    "patch": BASIC_PATCH,
    "additions": 3,
    "deletions": 2,
}]


class FakeGitHub:

    def __init__(self, prs, post_errors=(), pr_error=None):
        self.prs = list(prs)
        self.post_errors = list(post_errors)
        self.pr_error = pr_error
        self.posts = []

    def get_pr(self, number):
        if self.pr_error:
            raise self.pr_error
        return self.prs.pop(0) if len(self.prs) > 1 else self.prs[0]

    def list_files(self, number):
        return FILES

    def merge_base(self, ref, sha):
        return "b" * 40

    def create_review(self, number, payload):
        self.posts.append(payload)
        if self.post_errors:
            raise self.post_errors.pop(0)


def fake_fetch(git_dir, home, url, shas):
    return object()


class ReviewTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.out = pathlib.Path(self.tmp.name) / "out"
        self.summary = pathlib.Path(self.tmp.name) / "summary.md"

    def tearDown(self):
        self.tmp.cleanup()

    def config(self, **overrides):
        values = dict(
            repo="near/nearcore",
            pr=7,
            dry_run=False,
            out_dir=self.out,
            docs_root=pathlib.Path(REPO_ROOT),
            discussions_path=None,
            exclude_bot_comments=False,
            git_url="https://github.com/near/nearcore.git",
            model="deepseek-ai/DeepSeek-V4.1-Flash-1017",
            base_url="https://cloud-api.near.ai/v1",
            run_url="https://github.com/near/nearcore/actions/runs/1",
            api_key=API_KEY,
            gh_token=GH_TOKEN,
            limits=agent.Limits(),
        )
        values.update(overrides)
        return cli.Config(**values)

    def run_review(self, gh, *responses, fetch=fake_fetch, **overrides):
        self.client = FakeClient(*responses)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(
                io.StringIO()) as self.stderr:
            code = cli.review(self.config(**overrides),
                              gh,
                              self.client,
                              fetch_repo=fetch,
                              env={"GITHUB_STEP_SUMMARY": str(self.summary)})
        self.stdout = stdout.getvalue()
        return code

    def submit(self, review=GOOD_REVIEW):
        return reply(call("submit_review", review))

    def test_success_posts_one_review(self):
        gh = FakeGitHub([pr()])
        self.assertEqual(self.run_review(gh, self.submit()), 0)
        self.assertEqual(len(gh.posts), 1)
        payload = gh.posts[0]
        self.assertEqual(payload["commit_id"], "a" * 40)
        self.assertEqual(payload["event"], "COMMENT")
        self.assertEqual(len(payload["comments"]), 1)
        self.assertIn("nearai-pr-review v1", payload["body"])
        self.assertIn("1 model turns", payload["body"])
        self.assertNotIn("tokens", payload["body"])
        stats = json.loads((self.out / "stats.json").read_text())
        self.assertEqual(stats["inline_comments"], 1)
        self.assertIsNone(stats["failure"])
        self.assertTrue((self.out / "transcript.json").is_file())
        summary = self.summary.read_text()
        self.assertIn("review submitted", summary)
        self.assertIn("prompt tokens 100 (0 cached)", summary)
        self.assertEqual(self.client.calls[0]["tool_choice"], "auto")

    def test_dry_run(self):
        gh = FakeGitHub([pr()])
        self.assertEqual(self.run_review(gh, self.submit(), dry_run=True), 0)
        self.assertEqual(gh.posts, [])
        self.assertIn("## Pull request overview", self.stdout)
        self.assertTrue((self.out / "body.md").is_file())

    def test_closed_at_start(self):
        gh = FakeGitHub([pr(state="closed")])
        self.assertEqual(self.run_review(gh), 0)
        self.assertEqual(gh.posts, [])
        self.assertEqual(self.client.calls, [])

    def test_closed_before_posting(self):
        gh = FakeGitHub([pr(), pr(), pr(state="closed")])
        self.assertEqual(self.run_review(gh, self.submit()), 0)
        self.assertEqual(gh.posts, [])

    def test_head_moved_before_posting(self):
        gh = FakeGitHub([pr(), pr(), pr(sha="c" * 40)])
        self.run_review(gh, self.submit())
        self.assertEqual(gh.posts[0]["commit_id"], "a" * 40)
        self.assertIn("New commits were pushed", gh.posts[0]["body"])

    def test_head_changed_during_capture(self):
        gh = FakeGitHub([pr(), pr(sha="c" * 40)])
        self.run_review(gh, self.submit())
        self.assertEqual(gh.posts[0]["commit_id"], "c" * 40)

    def test_inline_rejected_falls_back_to_body(self):
        gh = FakeGitHub([pr()],
                        post_errors=[github.GitHubError(422, "bad line")])
        self.assertEqual(self.run_review(gh, self.submit()), 0)
        self.assertEqual(len(gh.posts), 2)
        self.assertEqual(gh.posts[1]["comments"], [])
        self.assertIn("`src/lib.rs:3`: **t** (high, correctness)\n  b",
                      gh.posts[1]["body"])

    def test_post_failure_exits_1(self):
        gh = FakeGitHub([pr()],
                        post_errors=[github.GitHubError(403, "forbidden")])
        self.assertEqual(self.run_review(gh, self.submit()), 1)

    def test_model_unavailable_posts_note(self):
        gh = FakeGitHub([pr()])
        code = self.run_review(gh, agent.ModelError("unavailable", "502"))
        self.assertEqual(code, 0)
        self.assertIn("model was unavailable", gh.posts[0]["body"])
        self.assertEqual(gh.posts[0]["comments"], [])

    def test_auth_failure_exits_1(self):
        gh = FakeGitHub([pr()])
        code = self.run_review(gh, agent.ModelError("auth", "HTTP 401"))
        self.assertEqual(code, 1)
        self.assertIn("misconfigured", gh.posts[0]["body"])

    def test_secret_in_output_is_not_posted(self):
        leaked = dict(GOOD_REVIEW, overview="key is " + API_KEY)
        gh = FakeGitHub([pr()])
        code = self.run_review(gh, self.submit(leaked))
        self.assertEqual(code, 1)
        self.assertNotIn(API_KEY, json.dumps(gh.posts))
        self.assertIn("internal error", gh.posts[0]["body"])
        self.assertNotIn(API_KEY, (self.out / "payload.json").read_text())

    def test_missing_rubric_markers(self):
        root = pathlib.Path(self.tmp.name) / "root"
        (root / ".claude/skills/pr-review").mkdir(parents=True)
        (root / cli.SKILL_PATH).write_text("no markers")
        gh = FakeGitHub([pr()])
        code = self.run_review(gh, docs_root=root)
        self.assertEqual(code, 1)
        self.assertIn("internal error", gh.posts[0]["body"])

    def test_fetch_failure(self):

        def failing_fetch(*args):
            raise tools.GitError("fetch failed")

        gh = FakeGitHub([pr()])
        self.assertEqual(self.run_review(gh, fetch=failing_fetch), 1)
        self.assertIn("internal error", gh.posts[0]["body"])

    def test_cannot_read_pr(self):
        gh = FakeGitHub([pr()], pr_error=github.GitHubError(404, "missing"))
        self.assertEqual(self.run_review(gh), 1)
        self.assertEqual(gh.posts, [])

    def test_discussions_filtered_for_comparisons(self):
        discussions = pathlib.Path(self.tmp.name) / "d.json"
        discussions.write_text(
            json.dumps({
                "comments": [{
                    "author": "github-actions",
                    "body": "CLAUDE SAID"
                }, {
                    "author": "bob",
                    "body": "HUMAN SAID"
                }]
            }))
        gh = FakeGitHub([pr()])
        self.run_review(gh,
                        self.submit(),
                        dry_run=True,
                        discussions_path=discussions,
                        exclude_bot_comments=True)
        transcript = (self.out / "transcript.json").read_text()
        self.assertIn("HUMAN SAID", transcript)
        self.assertNotIn("CLAUDE SAID", transcript)


class ConfigTest(unittest.TestCase):

    def parse(self, argv, env):
        with contextlib.redirect_stderr(io.StringIO()):
            return cli.parse_config(argv, env)

    def test_defaults(self):
        config = self.parse(
            ["--repo", "near/nearcore", "--pr", "7"], {
                "NEARAI_API_KEY": API_KEY,
                "GH_TOKEN": GH_TOKEN,
                "RUNNER_TEMP": "/tmp/rt",
                "NEARAI_REVIEW_MAX_TURNS": "9",
            })
        self.assertEqual(config.model, agent.DEFAULT_MODEL)
        self.assertEqual(config.base_url, agent.DEFAULT_BASE_URL)
        self.assertEqual(config.out_dir, pathlib.Path("/tmp/rt/nearai-review"))
        self.assertEqual(config.git_url, "https://github.com/near/nearcore.git")
        self.assertEqual(config.limits.max_turns, 9)
        self.assertTrue((config.docs_root / cli.SKILL_PATH).is_file())

    def test_required_secrets(self):
        with self.assertRaises(SystemExit):
            self.parse(["--repo", "r/r", "--pr", "1"], {"GH_TOKEN": "t"})
        with self.assertRaises(SystemExit):
            self.parse(["--repo", "r/r", "--pr", "1"], {"NEARAI_API_KEY": "k"})
        config = self.parse(["--repo", "r/r", "--pr", "1", "--dry-run"],
                            {"NEARAI_API_KEY": "k"})
        self.assertIsNone(config.gh_token)

    def test_main_removes_secrets_from_environment(self):
        env = {"NEARAI_API_KEY": API_KEY, "GH_TOKEN": GH_TOKEN}
        with mock.patch.dict(os.environ,
                             env), mock.patch.object(cli,
                                                     "review",
                                                     return_value=0) as review:
            self.assertEqual(cli.main(["--repo", "r/r", "--pr", "1"]), 0)
            self.assertNotIn("NEARAI_API_KEY", os.environ)
            self.assertNotIn("GH_TOKEN", os.environ)
        self.assertEqual(review.call_args.args[0].api_key, API_KEY)


if __name__ == "__main__":
    unittest.main()
