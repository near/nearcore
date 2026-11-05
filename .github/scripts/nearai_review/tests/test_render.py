import unittest
from unittest import mock

from nearai_review import diff, render
from nearai_review.tests.test_diff import BASIC_PATCH, files_for

INFO = render.RunInfo(model="deepseek-ai/DeepSeek-V4.1-Flash-1017",
                      head_sha="a" * 40,
                      run_url="https://github.com/near/nearcore/actions/runs/1",
                      stats="3 turns")


def finding(**overrides):
    item = {
        "path": "src/lib.rs",
        "side": "RIGHT",
        "line": 3,
        "severity": "high",
        "category": "correctness",
        "title": "Overflow on large input",
        "body": "Use checked_add.",
    }
    item.update(overrides)
    return item


def submission(findings=(), **overrides):
    raw = {
        "overview": "Adds y.",
        "changes": ["Add y"],
        "files": [{
            "path": "src/lib.rs",
            "description": "adds y"
        }],
        "findings": list(findings),
    }
    raw.update(overrides)
    parsed, problems = render.parse_submission(raw)
    return parsed, problems


class ParseSubmissionTest(unittest.TestCase):

    def test_valid(self):
        parsed, problems = submission([finding()])
        self.assertEqual(problems, [])
        self.assertEqual(parsed.overview, "Adds y.")
        self.assertEqual(parsed.changes, ["Add y"])
        self.assertEqual(parsed.files, [("src/lib.rs", "adds y")])
        self.assertEqual(parsed.findings[0].severity, "high")
        self.assertEqual(parsed.findings[0].side, "RIGHT")

    def test_not_an_object(self):
        with self.assertRaises(render.SubmissionError):
            render.parse_submission(["x"])

    def test_defaults_and_problems(self):
        parsed, problems = render.parse_submission({"findings": "nope"})
        self.assertIn("overview is missing", problems)
        self.assertIn("findings must be a list", problems)
        self.assertEqual(parsed.findings, [])
        self.assertEqual(parsed.changes, [])

    def test_finding_normalization(self):
        parsed, problems = submission([
            finding(severity="BLOCKER",
                    category="style",
                    side="right",
                    path="./src/lib.rs",
                    title=None,
                    body="First line\nmore"),
            "not an object",
            finding(title="", body=""),
        ])
        f = parsed.findings[0]
        self.assertEqual(f.severity, "medium")
        self.assertEqual(f.category, "code_quality")
        self.assertEqual(f.side, "RIGHT")
        self.assertEqual(f.path, "src/lib.rs")
        self.assertEqual(f.title, "First line")
        self.assertEqual(len(parsed.findings), 1)
        self.assertTrue(any("invalid severity" in p for p in problems))
        self.assertTrue(any("#1 is not an object" in p for p in problems))
        self.assertTrue(any("#2 has no title or body" in p for p in problems))

    def test_limits(self):
        parsed, problems = submission(
            [finding() for _ in range(render.MAX_FINDINGS + 3)],
            overview="o" * 5000,
            changes=["c"] * 30 + [5])
        self.assertEqual(len(parsed.findings), render.MAX_FINDINGS)
        self.assertTrue(any("first 40 findings" in p for p in problems))
        self.assertTrue(parsed.overview.endswith("[clipped]"))
        self.assertEqual(len(parsed.changes), render.MAX_CHANGES)


class LocateTest(unittest.TestCase):

    def setUp(self):
        self.files = files_for(BASIC_PATCH)

    def test_locations(self):
        parsed, _ = submission([
            finding(),
            finding(line=10),
            finding(line=21, start_line=5),
            finding(path=None, line=None),
            finding(path="other.rs"),
        ])
        errors = render.locate_findings(parsed, self.files)
        f = parsed.findings
        self.assertEqual(f[0].location, diff.Location("src/lib.rs", "RIGHT", 3))
        self.assertIsNone(f[1].location)
        # A bad start_line keeps a one-line location.
        self.assertEqual(f[2].location, diff.Location("src/lib.rs", "RIGHT",
                                                      21))
        self.assertIsNone(f[3].location)
        self.assertIsNone(f[4].location)
        self.assertEqual(len(errors), 3)
        self.assertIn("finding #1", errors[0])
        self.assertIn("valid RIGHT lines: 1-6, 21-22", errors[0])
        self.assertIn("more than one diff hunk", errors[1])
        self.assertIn("not a file changed", errors[2])

    def test_choose_inline_keeps_most_severe(self):
        raw = [finding(severity="low") for _ in range(25)]
        raw += [finding(severity="critical") for _ in range(5)]
        parsed, _ = submission(raw)
        render.locate_findings(parsed, self.files)
        render.choose_inline(parsed)
        inline = [f for f in parsed.findings if f.inline]
        self.assertEqual(len(inline), render.MAX_INLINE_COMMENTS)
        self.assertTrue(all(f.inline for f in parsed.findings[25:]))
        self.assertEqual(sum(1 for f in parsed.findings[:25] if f.inline), 20)


class SanitizeTest(unittest.TestCase):

    def test_mentions(self):
        self.assertEqual(render.sanitize("ping @alice and @near/core-team"),
                         "ping `@alice` and `@near/core-team`")
        self.assertEqual(render.sanitize("mail a@b.com"), "mail a@b.com")
        self.assertEqual(render.sanitize("code `@x` stays"), "code `@x` stays")

    def test_dashes_and_comments(self):
        out = render.sanitize("a\u2014b \u2013 c\u2026 <!-- x -->")
        self.assertEqual(out, "a - b - c... &lt;!-- x -->")
        fenced = "```\nlet s = \"\u2014 @bob\";\n```"
        self.assertEqual(render.sanitize(fenced), fenced)


class RenderBodyTest(unittest.TestCase):

    def render(self, raw_findings, info=INFO):
        parsed, _ = submission(raw_findings)
        render.locate_findings(parsed, files_for(BASIC_PATCH))
        return render.build_review(parsed, info)

    def test_layout_with_findings(self):
        review = self.render([
            finding(),
            finding(severity="low", line=99, title="Nit \u2014 rename"),
            finding(path=None, line=None, severity="medium", title="General"),
        ])
        body = review["body"]
        lines = body.split("\n")
        self.assertTrue(lines[0].startswith("<!-- nearai-pr-review v1 "))
        self.assertIn("head=" + "a" * 40, lines[0])
        self.assertIn("`aaaaaaaaaa`", lines[1])
        self.assertIn("## Pull request overview", body)
        self.assertIn("**Changes:**\n- Add y", body)
        self.assertIn("| `src/lib.rs` | adds y |", body)
        self.assertIn(
            "**Blocking** (must fix before merge):\n- "
            "`src/lib.rs:3`: **Overflow on large input** (high, "
            "correctness)", body)
        self.assertIn(
            "`src/lib.rs:99` (not on a changed line): **Nit - "
            "rename**", body)
        self.assertIn("- `general`: **General** (medium, correctness)", body)
        self.assertIn("⚠️ Issues found", body)
        self.assertNotIn("\u2014", body)
        self.assertIn("[Run](https://github.com/near/nearcore/actions/runs/1)",
                      body)
        self.assertIn("3 turns", body)
        self.assertEqual(review["event"], "COMMENT")
        self.assertEqual(review["commit_id"], "a" * 40)
        self.assertEqual(len(review["comments"]), 1)

    def test_approved_when_only_non_blocking(self):
        body = self.render([finding(severity="medium")])["body"]
        self.assertIn("✅ Approved", body)
        self.assertNotIn("**Blocking**", body)
        body = self.render([])["body"]
        self.assertIn("✅ Approved", body)
        self.assertNotIn("### Findings", body)

    def test_table_cells_escaped(self):
        parsed, _ = submission(files=[{
            "path": "a|b.rs",
            "description": "x | y\nz"
        }])
        body = render.render_body(parsed, INFO)
        self.assertIn("| `a\\|b.rs` | x \\| y z |", body)

    def test_head_moved_note(self):
        info = render.RunInfo(model="m", head_sha="b" * 40, head_moved=True)
        body = self.render([], info)["body"]
        self.assertIn("New commits were pushed", body)

    def test_inline_comment_shape(self):
        review = self.render([finding(line=4, start_line=2)])
        comment = review["comments"][0]
        self.assertEqual(comment["path"], "src/lib.rs")
        self.assertEqual(comment["line"], 4)
        self.assertEqual(comment["start_line"], 2)
        self.assertEqual(comment["start_side"], "RIGHT")
        self.assertNotIn("position", comment)
        self.assertIn("**Blocking** (high, correctness)", comment["body"])
        single = self.render([finding()])["comments"][0]
        self.assertNotIn("start_line", single)

    def test_body_only_fallback(self):
        parsed, _ = submission([finding()])
        render.locate_findings(parsed, files_for(BASIC_PATCH))
        review = render.build_review(parsed, INFO, inline=False)
        self.assertEqual(review["comments"], [])
        self.assertIn("Use checked_add.", review["body"])

    def test_size_cap(self):
        parsed, _ = submission(files=[{
            "path": f"f{i}.rs",
            "description": "d" * 150
        } for i in range(150)])
        with mock.patch.object(render, "MAX_BODY_CHARS", 3000):
            body = render.render_body(parsed, INFO)
        self.assertNotIn("Per-file summary", body)
        parsed.overview = "o" * 5000
        with mock.patch.object(render, "MAX_BODY_CHARS", 3000):
            body = render.render_body(parsed, INFO)
        self.assertTrue(body.endswith("[review body clipped]"))


class FailureAndSecretTest(unittest.TestCase):

    def test_failure_note(self):
        review = render.render_failure("budget", INFO)
        self.assertIn("ran out of its time or token budget", review["body"])
        self.assertIn("[run](", review["body"])
        self.assertEqual(review["comments"], [])
        self.assertIn("an internal error",
                      render.render_failure("weird", INFO)["body"])

    def test_contains_secret(self):
        payload = {"body": "x sk-12345678abc y", "comments": []}
        self.assertTrue(render.contains_secret(payload, ["sk-12345678abc"]))
        self.assertFalse(render.contains_secret(payload, ["other-secret"]))
        self.assertFalse(render.contains_secret(payload, ["x", ""]))


if __name__ == "__main__":
    unittest.main()
