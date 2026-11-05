"""Validates the model's review submission and renders the GitHub review.

The model returns structured data through the `submit_review` tool. This
module checks it, decides which findings can be inline comments, and builds
the review body in the same layout as the Claude review bot
(`.claude/skills/pr-review/SKILL.md`), so the two can be compared.
"""

import dataclasses
import re

from . import diff

MARKER = "nearai-pr-review"
SEVERITIES = ("critical", "high", "medium", "low")
BLOCKING_SEVERITIES = ("critical", "high")
CATEGORIES = (
    "correctness",
    "engineering_standards",
    "production_safety",
    "performance",
    "rust",
    "security",
    "code_quality",
    "comment_quality",
)

MAX_OVERVIEW_CHARS = 1500
MAX_CHANGES = 15
MAX_CHANGE_CHARS = 500
MAX_FILES = 150
MAX_FILE_DESCRIPTION_CHARS = 200
MAX_FINDINGS = 40
MAX_TITLE_CHARS = 150
MAX_FINDING_BODY_CHARS = 3500
MAX_INLINE_COMMENTS = 25
MAX_BODY_CHARS = 60_000

SUBMIT_REVIEW_SCHEMA = {
    "type": "function",
    "function": {
        "name": "submit_review",
        "description":
            "Submit the final review. Call it exactly once, after you have "
            "inspected every changed area.",
        "parameters": {
            "type": "object",
            "properties": {
                "overview": {
                    "type": "string",
                    "description": "2 to 4 sentences: what the PR does and why."
                },
                "changes": {
                    "type": "array",
                    "items": {
                        "type": "string"
                    },
                    "description":
                        "Substantive changes, one per item, related edits "
                        f"grouped. At most {MAX_CHANGES}."
                },
                "files": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string"
                            },
                            "description": {
                                "type": "string"
                            },
                        },
                        "required": ["path", "description"],
                    },
                    "description": "One short description per changed file."
                },
                "findings": {
                    "type":
                        "array",
                    "description":
                        "Actionable problems only. Empty if there are none.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": {
                                "type": "string"
                            },
                            "side": {
                                "type": "string",
                                "enum": ["RIGHT", "LEFT"],
                                "description":
                                    "RIGHT for new code (R numbers), LEFT "
                                    "for removed code (L numbers)."
                            },
                            "line": {
                                "type": "integer",
                                "description":
                                    "Diff line the finding is about. Omit "
                                    "for a finding not tied to one line."
                            },
                            "start_line": {
                                "type": "integer",
                                "description":
                                    "First line of a multi-line range in "
                                    "the same hunk. Omit for one line."
                            },
                            "severity": {
                                "type": "string",
                                "enum": list(SEVERITIES)
                            },
                            "category": {
                                "type": "string",
                                "enum": list(CATEGORIES)
                            },
                            "title": {
                                "type": "string"
                            },
                            "body": {
                                "type": "string",
                                "description":
                                    "Impact, the condition that triggers it, "
                                    "and a concrete fix."
                            },
                        },
                        "required": ["severity", "category", "title", "body"],
                    },
                },
            },
            "required": ["overview", "changes", "files", "findings"],
        },
    },
}


class SubmissionError(Exception):
    """The submission cannot be used at all."""


@dataclasses.dataclass
class Finding:
    path: str | None
    side: str
    line: int | None
    start_line: int | None
    severity: str
    category: str
    title: str
    body: str
    location: diff.Location | None = None
    inline: bool = False

    def is_blocking(self) -> bool:
        return self.severity in BLOCKING_SEVERITIES

    def anchor(self) -> str:
        if self.location:
            loc = self.location
            if loc.start_line:
                return f"{loc.path}:{loc.start_line}-{loc.line}"
            return f"{loc.path}:{loc.line}"
        if self.path and isinstance(self.line, int):
            return f"{self.path}:{self.line}"
        return self.path or "general"


@dataclasses.dataclass
class Submission:
    overview: str
    changes: list[str]
    files: list[tuple[str, str]]
    findings: list[Finding]


def _text(value, limit: int) -> str:
    if not isinstance(value, str):
        return ""
    value = value.strip()
    if len(value) > limit:
        value = value[:limit].rstrip() + " [clipped]"
    return value


def parse_submission(raw) -> tuple[Submission, list[str]]:
    """Normalizes raw `submit_review` arguments.

    Returns the submission and a list of problems to send back to the model.
    Problems never block posting: missing values get defaults.
    """
    if not isinstance(raw, dict):
        raise SubmissionError("submit_review arguments must be an object")
    problems = []
    overview = _text(raw.get("overview"), MAX_OVERVIEW_CHARS)
    if not overview:
        problems.append("overview is missing")
    changes = []
    if isinstance(raw.get("changes"), list):
        for item in raw["changes"][:MAX_CHANGES]:
            text = _text(item, MAX_CHANGE_CHARS)
            if text:
                changes.append(text)
    files = []
    if isinstance(raw.get("files"), list):
        for item in raw["files"][:MAX_FILES]:
            if isinstance(item, dict) and _text(item.get("path"), 300):
                files.append((_text(item.get("path"), 300),
                              _text(item.get("description"),
                                    MAX_FILE_DESCRIPTION_CHARS)))
    findings = []
    raw_findings = raw.get("findings")
    if raw_findings is None:
        raw_findings = []
    if not isinstance(raw_findings, list):
        problems.append("findings must be a list")
        raw_findings = []
    if len(raw_findings) > MAX_FINDINGS:
        problems.append(f"only the first {MAX_FINDINGS} findings are kept")
    for index, item in enumerate(raw_findings[:MAX_FINDINGS]):
        if not isinstance(item, dict):
            problems.append(f"finding #{index} is not an object")
            continue
        title = _text(item.get("title"), MAX_TITLE_CHARS)
        body = _text(item.get("body"), MAX_FINDING_BODY_CHARS)
        if not title and not body:
            problems.append(f"finding #{index} has no title or body")
            continue
        severity = str(item.get("severity", "")).lower()
        if severity not in SEVERITIES:
            problems.append(f"finding #{index} has invalid severity "
                            f"{item.get('severity')!r}; using medium")
            severity = "medium"
        category = str(item.get("category", "")).lower()
        if category not in CATEGORIES:
            category = "code_quality"
        side = item.get("side", diff.RIGHT)
        if isinstance(side, str):
            side = side.upper()
        path = item.get("path")
        findings.append(
            Finding(
                path=path.removeprefix("./")
                if isinstance(path, str) and path else None,
                side=side,
                line=item.get("line"),
                start_line=item.get("start_line"),
                severity=severity,
                category=category,
                title=title or body.split("\n", 1)[0][:MAX_TITLE_CHARS],
                body=body,
            ))
    return Submission(overview, changes, files, findings), problems


def locate_findings(submission: Submission,
                    files: dict[str, diff.FileDiff]) -> list[str]:
    """Sets `location` on findings that can be inline comments.

    Returns one message per finding whose location is not valid, so the
    model can repair it. A finding with a valid `line` but a bad
    `start_line` keeps a one-line location.
    """
    errors = []
    for index, finding in enumerate(submission.findings):
        finding.location = None
        if finding.path is None or finding.line is None:
            continue
        location, error = diff.validate_location(files, finding.path,
                                                 finding.side, finding.line,
                                                 finding.start_line)
        if error and finding.start_line is not None:
            location, _ = diff.validate_location(files, finding.path,
                                                 finding.side, finding.line)
        finding.location = location
        if error:
            errors.append(f"finding #{index} ({finding.title}): {error}")
    return errors


def choose_inline(submission: Submission) -> None:
    """Marks up to MAX_INLINE_COMMENTS located findings as inline comments.

    The most severe findings are kept inline. The rest are shown in the body.
    """
    located = [f for f in submission.findings if f.location]
    located.sort(key=lambda f: SEVERITIES.index(f.severity))
    for rank, finding in enumerate(located):
        finding.inline = rank < MAX_INLINE_COMMENTS


_FENCE_RE = re.compile(r"(```.*?```|`[^`\n]*`)", re.DOTALL)
_MENTION_RE = re.compile(
    r"(?<![\w`/.-])@([A-Za-z0-9][A-Za-z0-9-]*(?:/[A-Za-z0-9._-]+)?)")


def sanitize(text: str) -> str:
    """Makes model text safe to post.

    Outside code: @mentions are wrapped in backticks so nobody is notified,
    dashes and ellipsis characters are replaced with ASCII, and HTML comment
    openers are escaped so the text cannot imitate our hidden marker.
    """
    parts = _FENCE_RE.split(text)
    for i in range(0, len(parts), 2):
        part = parts[i]
        part = _MENTION_RE.sub(r"`@\1`", part)
        part = re.sub(r"[ \t]*\u2014[ \t]*", " - ", part)
        part = part.replace("\u2013", "-")
        part = part.replace("\u2026", "...")
        part = part.replace("<!--", "&lt;!--")
        parts[i] = part
    return "".join(parts)


def _cell(text: str) -> str:
    return sanitize(text).replace("|", "\\|").replace("\n", " ")


@dataclasses.dataclass
class RunInfo:
    model: str
    head_sha: str
    run_url: str | None = None
    head_moved: bool = False
    stats: str = ""


def _header(info: RunInfo) -> list[str]:
    run = f" run={info.run_url}" if info.run_url else ""
    lines = [
        f"<!-- {MARKER} v1 model={info.model} head={info.head_sha}{run} -->",
        f"> **NEAR AI review** by `{info.model}` (served by NEAR AI) of "
        f"commit `{info.head_sha[:10]}`.",
    ]
    if info.head_moved:
        lines.append("> New commits were pushed while this review ran. It "
                     "covers the commit above only.")
    return lines + [""]


def _finding_line(finding: Finding) -> str:
    where = f"`{finding.anchor()}`"
    note = ""
    if finding.location is None and finding.line is not None:
        note = " (not on a changed line)"
    line = (f"- {where}{note}: **{sanitize(finding.title)}** "
            f"({finding.severity}, {finding.category})")
    if not finding.inline and finding.body:
        body = sanitize(finding.body).replace("\n", "\n  ")
        line += f"\n  {body}"
    return line


def render_body(submission: Submission,
                info: RunInfo,
                with_file_table: bool = True) -> str:
    lines = _header(info)
    lines += ["## Pull request overview", ""]
    lines += [sanitize(submission.overview) or "(no overview provided)", ""]
    if submission.changes:
        lines += ["**Changes:**"]
        lines += [f"- {sanitize(c)}" for c in submission.changes]
        lines += [""]
    if with_file_table and submission.files:
        lines += [
            "### Reviewed changes",
            "",
            "<details>",
            "<summary>Per-file summary</summary>",
            "",
            "| File | Description |",
            "| ---- | ----------- |",
        ]
        lines += [f"| `{_cell(p)}` | {_cell(d)} |" for p, d in submission.files]
        lines += ["", "</details>", ""]
    blocking = [f for f in submission.findings if f.is_blocking()]
    other = [f for f in submission.findings if not f.is_blocking()]
    if blocking or other:
        lines += ["### Findings", ""]
    if blocking:
        lines += ["**Blocking** (must fix before merge):"]
        lines += [_finding_line(f) for f in blocking]
        lines += [""]
    if other:
        lines += ["**Non-blocking** (nits, follow-ups, suggestions):"]
        lines += [_finding_line(f) for f in other]
        lines += [""]
    lines += ["⚠️ Issues found" if blocking else "✅ Approved", ""]
    footer = "<sub>Automated review by a NEAR AI hosted model."
    if info.stats:
        footer += f" {info.stats}."
    if info.run_url:
        footer += f" [Run]({info.run_url})."
    footer += " Comment `/nearai-review` to run it again.</sub>"
    lines.append(footer)
    body = "\n".join(lines)
    if len(body) > MAX_BODY_CHARS and with_file_table:
        return render_body(submission, info, with_file_table=False)
    if len(body) > MAX_BODY_CHARS:
        body = body[:MAX_BODY_CHARS] + "\n\n[review body clipped]"
    return body


def render_inline_comment(finding: Finding) -> dict:
    kind = "Blocking" if finding.is_blocking() else "Non-blocking"
    text = (f"**{kind}** ({finding.severity}, {finding.category}): "
            f"**{sanitize(finding.title)}**\n\n{sanitize(finding.body)}\n\n"
            "<sub>NEAR AI review</sub>")
    loc = finding.location
    comment = {
        "path": loc.path,
        "side": loc.side,
        "line": loc.line,
        "body": text
    }
    if loc.start_line is not None:
        comment["start_line"] = loc.start_line
        comment["start_side"] = loc.side
    return comment


def build_review(submission: Submission,
                 info: RunInfo,
                 inline: bool = True) -> dict:
    """Builds the `POST /pulls/{n}/reviews` payload.

    With `inline=False` every finding goes in the body. That is the fallback
    when GitHub rejects the inline comments.
    """
    if inline:
        choose_inline(submission)
    else:
        for finding in submission.findings:
            finding.inline = False
    comments = [
        render_inline_comment(f) for f in submission.findings if f.inline
    ]
    return {
        "commit_id": info.head_sha,
        "event": "COMMENT",
        "body": render_body(submission, info),
        "comments": comments,
    }


FAILURE_REASONS = {
    "model_unavailable": "the NEAR AI model was unavailable",
    "budget": "the review ran out of its time or token budget",
    "bad_output": "the model did not return a usable review",
    "config": "the workflow is misconfigured (check the API key and "
              "permissions)",
    "internal": "an internal error occurred",
}


def render_failure(reason: str, info: RunInfo) -> dict:
    """A short body-only review saying the review could not complete."""
    text = FAILURE_REASONS.get(reason, FAILURE_REASONS["internal"])
    lines = _header(info)
    lines.append(f"The NEAR AI review could not complete: {text}.")
    if info.run_url:
        lines.append(f"See the [run]({info.run_url}) for details.")
    lines.append("Comment `/nearai-review` to try again.")
    return {
        "commit_id": info.head_sha,
        "event": "COMMENT",
        "body": "\n".join(lines),
        "comments": [],
    }


def contains_secret(payload: dict, secrets: list[str]) -> bool:
    """True if any secret value appears anywhere in the payload."""
    text = repr(payload)
    return any(s and len(s) >= 8 and s in text for s in secrets)
