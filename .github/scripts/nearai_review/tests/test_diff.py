import re
import unittest

from nearai_review import diff
from nearai_review.diff import LEFT, RIGHT

# Two hunks. Expected numbering:
#   hunk 0: LEFT 1-5, RIGHT 1-6 (L2 removed, R2-R3 added)
#   hunk 1: LEFT 20-22, RIGHT 21-22 (L21 removed)
BASIC_PATCH = "\n".join([
    "@@ -1,5 +1,6 @@",
    " fn a() {",
    "-    let x = 1;",
    "+    let x = 2;",
    "+    let y = 3;",
    " }",
    " ",
    " fn b() {}",
    "@@ -20,3 +21,2 @@ fn c() {",
    " line20",
    "-line21",
    " line22",
])


def files_for(patch, status="modified", path="src/lib.rs"):
    return {
        path:
            diff.FileDiff.from_api({
                "filename": path,
                "status": status,
                "patch": patch,
            })
    }


class ParsePatchTest(unittest.TestCase):

    def test_basic_two_hunks(self):
        lines = diff.parse_patch(BASIC_PATCH)
        self.assertEqual(lines[LEFT], {
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
            20: 1,
            21: 1,
            22: 1
        })
        self.assertEqual(lines[RIGHT], {
            1: 0,
            2: 0,
            3: 0,
            4: 0,
            5: 0,
            6: 0,
            21: 1,
            22: 1
        })

    def test_header_without_counts(self):
        lines = diff.parse_patch("@@ -3 +3 @@\n-a\n+b")
        self.assertEqual(lines[LEFT], {3: 0})
        self.assertEqual(lines[RIGHT], {3: 0})

    def test_new_file(self):
        lines = diff.parse_patch("@@ -0,0 +1,3 @@\n+a\n+b\n+c")
        self.assertEqual(lines[LEFT], {})
        self.assertEqual(sorted(lines[RIGHT]), [1, 2, 3])

    def test_deleted_file(self):
        lines = diff.parse_patch("@@ -1,2 +0,0 @@\n-a\n-b")
        self.assertEqual(sorted(lines[LEFT]), [1, 2])
        self.assertEqual(lines[RIGHT], {})

    def test_no_newline_marker_does_not_advance(self):
        patch = "\n".join([
            "@@ -1,2 +1,2 @@",
            " a",
            "-b",
            "\\ No newline at end of file",
            "+b",
        ])
        lines = diff.parse_patch(patch)
        self.assertEqual(sorted(lines[LEFT]), [1, 2])
        self.assertEqual(sorted(lines[RIGHT]), [1, 2])

    def test_hunk_header_text_inside_content(self):
        patch = "\n".join([
            "@@ -1,2 +1,2 @@",
            "-@@ -9 +9 @@ old",
            "+@@ -9 +9 @@ new",
            " @@ -5 +5 @@ context",
        ])
        lines = diff.parse_patch(patch)
        self.assertEqual(lines[LEFT], {1: 0, 2: 0})
        self.assertEqual(lines[RIGHT], {1: 0, 2: 0})

    def test_crlf_content(self):
        lines = diff.parse_patch("@@ -1,2 +1,2 @@\n a\r\n-b\r\n+c\r")
        self.assertEqual(sorted(lines[LEFT]), [1, 2])
        self.assertEqual(sorted(lines[RIGHT]), [1, 2])

    def test_trailing_newline_is_not_a_line(self):
        lines = diff.parse_patch("@@ -1 +1 @@\n-a\n+b\n")
        self.assertEqual(lines[LEFT], {1: 0})
        self.assertEqual(lines[RIGHT], {1: 0})

    def test_lines_before_first_header_ignored(self):
        lines = diff.parse_patch("garbage\n+x\n@@ -4 +4 @@\n-a\n+b")
        self.assertEqual(lines[LEFT], {4: 0})
        self.assertEqual(lines[RIGHT], {4: 0})


class AnnotatePatchTest(unittest.TestCase):

    def test_line_numbers_next_to_lines(self):
        annotated = diff.annotate_patch(BASIC_PATCH).split("\n")
        self.assertEqual(annotated[0], "@@ -1,5 +1,6 @@")
        self.assertIn("L1", annotated[1])
        self.assertIn("R1", annotated[1])
        self.assertTrue(annotated[2].startswith("-"))
        self.assertIn("L2", annotated[2])
        self.assertNotIn("R", annotated[2].split("|")[0])
        self.assertTrue(annotated[4].startswith("+"))
        self.assertIn("R3", annotated[4])
        self.assertNotIn("L", annotated[4].split("|")[0])
        self.assertTrue(annotated[4].endswith("|     let y = 3;"))
        self.assertIn("L3", annotated[5])
        self.assertIn("R4", annotated[5])

    def test_annotation_agrees_with_parser(self):
        for patch in (BASIC_PATCH, "@@ -0,0 +1,2 @@\n+a\n+b",
                      "@@ -7,2 +7,0 @@\n-a\n-b"):
            parsed = diff.parse_patch(patch)
            seen = {LEFT: set(), RIGHT: set()}
            for line in diff.annotate_patch(patch).split("\n"):
                prefix = line.split("|")[0]
                for n in re.findall(r"L(\d+)", prefix):
                    seen[LEFT].add(int(n))
                for n in re.findall(r"R(\d+)", prefix):
                    seen[RIGHT].add(int(n))
            self.assertEqual(seen[LEFT], set(parsed[LEFT]))
            self.assertEqual(seen[RIGHT], set(parsed[RIGHT]))

    def test_no_newline_marker_kept_verbatim(self):
        annotated = diff.annotate_patch(
            "@@ -1 +1 @@\n-a\n\\ No newline at end of file\n+b")
        self.assertIn("\\ No newline at end of file", annotated.split("\n"))


class DescribeRangesTest(unittest.TestCase):

    def test_ranges(self):
        self.assertEqual(diff.describe_ranges({}), "none")
        self.assertEqual(
            diff.describe_ranges(dict.fromkeys([8, 1, 2, 3, 5, 7], 0)),
            "1-3, 5, 7-8")


class FileDiffTest(unittest.TestCase):

    def test_missing_patch(self):
        file = diff.FileDiff.from_api({"filename": "a.bin", "status": "added"})
        self.assertFalse(file.has_patch())
        self.assertIsNone(file.patch)

    def test_rename_fields(self):
        file = diff.FileDiff.from_api({
            "filename": "new.rs",
            "previous_filename": "old.rs",
            "status": "renamed",
            "additions": 1,
            "deletions": 0,
            "patch": "@@ -1 +1,2 @@\n a\n+b",
        })
        self.assertEqual(file.previous_path, "old.rs")
        self.assertEqual(file.status, "renamed")
        self.assertTrue(file.has_patch())


class ValidateLocationTest(unittest.TestCase):

    def setUp(self):
        self.files = files_for(BASIC_PATCH)

    def check_ok(self, *args, expected):
        location, error = diff.validate_location(self.files, *args)
        self.assertIsNone(error)
        self.assertEqual(location, expected)

    def check_error(self, *args, contains):
        location, error = diff.validate_location(self.files, *args)
        self.assertIsNone(location)
        self.assertIn(contains, error)

    def test_valid_single_lines(self):
        self.check_ok("src/lib.rs",
                      RIGHT,
                      3,
                      expected=diff.Location("src/lib.rs", RIGHT, 3))
        self.check_ok("src/lib.rs",
                      LEFT,
                      21,
                      expected=diff.Location("src/lib.rs", LEFT, 21))

    def test_valid_range_inside_hunk(self):
        self.check_ok("src/lib.rs",
                      RIGHT,
                      4,
                      2,
                      expected=diff.Location("src/lib.rs", RIGHT, 4, 2))

    def test_equal_start_line_collapses(self):
        self.check_ok("src/lib.rs",
                      RIGHT,
                      4,
                      4,
                      expected=diff.Location("src/lib.rs", RIGHT, 4))

    def test_unknown_path(self):
        self.check_error("other.rs", RIGHT, 1, contains="not a file changed")
        self.check_error(None, RIGHT, 1, contains="not a file changed")

    def test_file_without_patch(self):
        self.files["a.bin"] = diff.FileDiff.from_api({
            "filename": "a.bin",
            "status": "added"
        })
        self.check_error("a.bin", RIGHT, 1, contains="no diff from GitHub")

    def test_bad_side(self):
        self.check_error("src/lib.rs", "right", 1, contains="LEFT or RIGHT")
        self.check_error("src/lib.rs", None, 1, contains="LEFT or RIGHT")

    def test_line_outside_diff_lists_ranges(self):
        self.check_error("src/lib.rs",
                         RIGHT,
                         10,
                         contains="valid RIGHT lines: 1-6, 21-22")
        self.check_error("src/lib.rs",
                         LEFT,
                         6,
                         contains="valid LEFT lines: 1-5, 20-22")

    def test_line_must_be_positive_int(self):
        for bad in (True, 0, -1, "3", 3.0, None):
            self.check_error("src/lib.rs",
                             RIGHT,
                             bad,
                             contains="is not a RIGHT line")

    def test_start_line_after_line(self):
        self.check_error("src/lib.rs", RIGHT, 2, 5, contains="after line")

    def test_start_line_in_other_hunk(self):
        self.check_error("src/lib.rs",
                         RIGHT,
                         21,
                         5,
                         contains="more than one diff hunk")

    def test_start_line_outside_diff(self):
        self.check_error("src/lib.rs", RIGHT, 21, 10, contains="start_line 10")
        self.check_error("src/lib.rs",
                         RIGHT,
                         3,
                         False,
                         contains="start_line False")

    def test_side_must_match_file_status(self):
        self.files = files_for("@@ -0,0 +1,2 @@\n+a\n+b", status="added")
        self.check_error("src/lib.rs",
                         LEFT,
                         1,
                         contains="valid LEFT lines: none")
        self.files = files_for("@@ -1,2 +0,0 @@\n-a\n-b", status="removed")
        self.check_error("src/lib.rs",
                         RIGHT,
                         1,
                         contains="valid RIGHT lines: none")


if __name__ == "__main__":
    unittest.main()
