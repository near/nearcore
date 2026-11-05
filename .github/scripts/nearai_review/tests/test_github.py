import json
import unittest

from nearai_review import github
from nearai_review.tests.test_agent import (FakeResponse, ScriptedUrlopen,
                                            http_error)


def make(urlopen, token="ghs_token", dry_run=False):
    sleeps = []
    gh = github.GitHub(token,
                       "near/nearcore",
                       dry_run=dry_run,
                       urlopen=urlopen,
                       sleep=sleeps.append)
    return gh, sleeps


def json_response(value):
    return FakeResponse(json.dumps(value).encode())


class GitHubTest(unittest.TestCase):

    def test_get_pr_request(self):
        urlopen = ScriptedUrlopen(json_response({"number": 7}))
        gh, _ = make(urlopen)
        self.assertEqual(gh.get_pr(7), {"number": 7})
        request = urlopen.requests[0]
        self.assertEqual(request.full_url,
                         "https://api.github.com/repos/near/nearcore/pulls/7")
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(request.get_header("Authorization"),
                         "Bearer ghs_token")
        self.assertEqual(request.get_header("X-github-api-version"),
                         "2022-11-28")

    def test_no_token(self):
        urlopen = ScriptedUrlopen(json_response({}))
        gh, _ = make(urlopen, token=None)
        gh.get_pr(1)
        self.assertIsNone(urlopen.requests[0].get_header("Authorization"))

    def test_list_files_paginates(self):
        urlopen = ScriptedUrlopen(json_response([{
            "filename": "a"
        }] * 100), json_response([{
            "filename": "b"
        }] * 5))
        gh, _ = make(urlopen)
        self.assertEqual(len(gh.list_files(7)), 105)
        self.assertIn("page=2", urlopen.requests[1].full_url)

    def test_merge_base(self):
        urlopen = ScriptedUrlopen(
            json_response({"merge_base_commit": {
                "sha": "b" * 40
            }}))
        gh, _ = make(urlopen)
        self.assertEqual(gh.merge_base("release/2.1", "a" * 40), "b" * 40)
        self.assertTrue(
            urlopen.requests[0].full_url.endswith("/compare/release/2.1..." +
                                                  "a" * 40))

    def test_check_runs(self):
        urlopen = ScriptedUrlopen(
            json_response({"check_runs": [{
                "name": "Greptile Review"
            }]}), json_response({}))
        gh, _ = make(urlopen)
        self.assertEqual(gh.check_runs("a" * 40, "Greptile Review"), [{
            "name": "Greptile Review"
        }])
        self.assertIn(
            "/commits/" + "a" * 40 + "/check-runs?check_name=Greptile%20Review",
            urlopen.requests[0].full_url)
        self.assertEqual(gh.check_runs("a" * 40, "x"), [])

    def test_create_review(self):
        urlopen = ScriptedUrlopen(json_response({"id": 1}))
        gh, _ = make(urlopen)
        gh.create_review(7, {"event": "COMMENT"})
        request = urlopen.requests[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertTrue(request.full_url.endswith("/pulls/7/reviews"))
        self.assertEqual(json.loads(request.data), {"event": "COMMENT"})

    def test_dry_run_never_posts(self):
        urlopen = ScriptedUrlopen()
        gh, _ = make(urlopen, dry_run=True)
        with self.assertRaises(RuntimeError):
            gh.create_review(7, {})
        self.assertEqual(urlopen.requests, [])

    def test_retries(self):
        urlopen = ScriptedUrlopen(http_error(502),
                                  http_error(403, {"Retry-After": "3"}),
                                  json_response({"ok": True}))
        gh, sleeps = make(urlopen)
        self.assertEqual(gh.get_pr(1), {"ok": True})
        self.assertEqual(sleeps, [2.0, 3.0])

    def test_client_errors_not_retried(self):
        for code in (404, 422, 403):
            urlopen = ScriptedUrlopen(http_error(code))
            gh, sleeps = make(urlopen)
            with self.assertRaises(github.GitHubError) as e:
                gh.get_pr(1)
            self.assertEqual(e.exception.status, code)
            self.assertEqual(sleeps, [])

    def test_gives_up(self):
        urlopen = ScriptedUrlopen(*[http_error(500) for _ in range(3)])
        gh, sleeps = make(urlopen)
        with self.assertRaises(github.GitHubError) as e:
            gh.get_pr(1)
        self.assertEqual(e.exception.status, 500)
        self.assertEqual(len(sleeps), 2)


if __name__ == "__main__":
    unittest.main()
