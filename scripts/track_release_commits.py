#!/usr/bin/env python3
# cspell:words checkmarks ownerchecked backticked
"""Track release-branch commits that are not yet on master.

Invoked from the release workflow (`release: published`). For the current
release tag (``GITHUB_REF_NAME``):

1. Compute ``BASE = git merge-base origin/master $TAG``. All patch releases
   cut from the same master base share a tracker issue.
2. Run ``git cherry -v origin/master $TAG`` to list release-only commits.
   ``+`` means no patch-equivalent on master (owner must review);
   ``-`` means an equivalent patch already exists on master (auto-checked).
3. Find the existing tracker issue by base (label ``release-tracker`` + a
   hidden ``<!-- tracker-base: $BASE -->`` marker). If the issue was
   closed, reopen it. Preserve owner-set checkmarks; append any new
   commits; rename the title to the new tag; post a comment logging that
   this release reuses the same base.
4. If no tracker matches the base, create a new issue.

Dependencies: ``gh`` CLI authenticated via ``GH_TOKEN``.

Local testing
-------------

In CI the master ref is ``origin/master``. On a developer machine the
canonical ``near/nearcore`` remote is typically ``upstream``, so the
examples below pass ``--master-ref upstream/master``.

1. Create path — what the workflow would do for a brand-new tracker.
   Pipe the generated body (stripping the "[dry-run] would create"
   preamble) into a file for the next step.

   .. code-block:: shell

      python3 scripts/track_release_commits.py \\
        --tag 2.11.0-rc.1 \\
        --master-ref upstream/master \\
        --dry-run \\
        | sed -n '/^<!--/,$p' > /tmp/tracker_prev.md

2. Update path — feed the previous body back in as a new release
   (``2.11.0``) cut from the same master base. Verify the existing
   commits are preserved in order and the new ones are appended, with
   ``-`` entries auto-checked and ``+`` entries left unchecked.

   .. code-block:: shell

      python3 scripts/track_release_commits.py \\
        --tag 2.11.0 \\
        --master-ref upstream/master \\
        --dry-run-issue /tmp/tracker_prev.md

3. Reopen path — same as above but simulate the previous issue being
   closed. Confirm the output includes ``would reopen issue #0`` and
   the comment text mentions the reopen.

   .. code-block:: shell

      python3 scripts/track_release_commits.py \\
        --tag 2.11.0 \\
        --master-ref upstream/master \\
        --dry-run-issue /tmp/tracker_prev.md \\
        --dry-run-issue-state closed

4. Owner-check preservation — flip one ``- [ ]`` entry to ``- [x]`` in
   the simulated body and confirm the owner's check survives even
   though ``git cherry`` still reports ``+`` for that commit.

   .. code-block:: shell

      sed 's|^- \\[ \\] \\[`21e335f`|- [x] [`21e335f`|' \\
        /tmp/tracker_prev.md > /tmp/tracker_prev_ownerchecked.md

      python3 scripts/track_release_commits.py \\
        --tag 2.11.0 \\
        --master-ref upstream/master \\
        --dry-run-issue /tmp/tracker_prev_ownerchecked.md \\
        | grep '21e335f'
"""

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
from collections import OrderedDict

LABEL = "release-tracker"
BASE_MARKER_RE = re.compile(r"<!--\s*tracker-base:\s*([0-9a-f]+)\s*-->")
# Accept either a plain backticked SHA or a markdown-linked backticked SHA
# so older bodies keep parsing cleanly after the format change.
ITEM_RE = re.compile(r"^- \[(?P<box>[ xX])\] "
                     r"(?:\[)?`(?P<sha>[0-9a-f]+)`(?:\]\([^)]+\))?"
                     r"(?P<rest>.*?)"
                     r"(?:\s*<!--\s*cherry:\s*(?P<cherry>[+-])\s*-->)?\s*$")
PR_SUFFIX_RE = re.compile(r"\(#(\d+)\)\s*$")
COMMITS_HEADER = "### Commits"
RELEASES_HEADER = "### Releases"


def run(cmd, check=True, capture=True):
    """Run a subprocess command, returning stdout (stripped) on success."""
    result = subprocess.run(
        cmd,
        check=check,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    return (result.stdout or "").strip()


def git_merge_base(master_ref, tag):
    return run(["git", "merge-base", master_ref, tag])


def short_sha(sha):
    return run(["git", "rev-parse", "--short", sha])


def cherry_commits(master_ref, tag):
    """Return list of (marker, sha, subject) for master_ref vs tag."""
    raw = run(["git", "cherry", "-v", master_ref, tag])
    commits = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        # "+ <sha> <subject>" or "- <sha> <subject>"
        parts = line.split(" ", 2)
        if len(parts) < 3:
            continue
        marker, sha, subject = parts[0], parts[1], parts[2]
        if marker not in ("+", "-"):
            continue
        commits.append((marker, sha, subject))
    return commits


def commit_url(sha):
    """Return the GitHub URL for ``sha`` in the current repository.

    Uses ``GITHUB_SERVER_URL`` and ``GITHUB_REPOSITORY`` when set (Actions
    sets both). Falls back to ``https://github.com/near/nearcore`` for
    local invocations.
    """
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com")
    repo = os.environ.get("GITHUB_REPOSITORY", "near/nearcore")
    return f"{server}/{repo}/commit/{sha}"


def format_item(marker, sha, subject):
    """Render one checklist line for a commit.

    ``-`` (patch already on master) pre-checks the box and records the auto
    marker; ``+`` leaves it unchecked. The short SHA is a markdown link to
    the commit on GitHub.
    """
    short = sha[:7]
    pr_match = PR_SUFFIX_RE.search(subject)
    pr_suffix = f" (#{pr_match.group(1)})" if pr_match else ""
    clean_subject = PR_SUFFIX_RE.sub("", subject).strip()
    box = "x" if marker == "-" else " "
    link = f"[`{short}`]({commit_url(sha)})"
    line = f"- [{box}] {link} {clean_subject}{pr_suffix}"
    if marker == "-":
        line += "  <!-- cherry: - -->"
    else:
        line += "  <!-- cherry: + -->"
    return line


def build_new_body(base_full, base_short, tag, items):
    today = datetime.date.today().isoformat()
    lines = [
        f"<!-- tracker-base: {base_full} -->",
        "",
        f"Tracks commits that exist on release tags derived from master "
        f"`{base_short}` but not yet reflected in master. Items pre-checked "
        f"by `git cherry` (patch-id already on master) start as `[x]`; the "
        f"remaining `[ ]` items need owner review — either merge into master "
        f"or mark as \"not needed\".",
        "",
        RELEASES_HEADER,
        f"- {tag} (added {today})",
        "",
        COMMITS_HEADER,
    ]
    lines.extend(items)
    lines.append("")
    return "\n".join(lines)


RELEASE_LINE_RE = re.compile(
    r"^-\s+(?P<tag>\S+)(?:\s+\(added\s+(?P<date>\d{4}-\d{2}-\d{2})\))?\s*$")


def parse_existing(body):
    """Parse an existing tracker issue body.

    Returns ``(releases, items_by_sha)`` where ``releases`` is a list of
    ``(tag, added_date)`` tuples (``added_date`` is ``None`` for entries
    written before dates were stored) and ``items_by_sha`` is an
    ``OrderedDict`` mapping a 7-char short SHA to its raw line and parsed
    ``(box, cherry_marker)`` metadata. Insertion order in the
    ``OrderedDict`` mirrors the order commits appear in ``body``.
    """
    releases = []
    items = OrderedDict()
    section = None
    for line in body.splitlines():
        if line.startswith(RELEASES_HEADER):
            section = "releases"
            continue
        if line.startswith(COMMITS_HEADER):
            section = "commits"
            continue
        if line.startswith("### "):
            section = None
            continue
        if section == "releases":
            m = RELEASE_LINE_RE.match(line)
            if m:
                releases.append((m.group("tag"), m.group("date")))
        elif section == "commits":
            m = ITEM_RE.match(line)
            if not m:
                continue
            sha = m.group("sha")[:7]
            box = m.group("box").lower()
            cherry = m.group("cherry")
            items[sha] = {
                "line": line,
                "checked": box == "x",
                "cherry": cherry,  # '+', '-', or None
            }
    return releases, items


def merge_items(existing_items, cherry_list):
    """Produce the merged checklist preserving owner-set marks.

    Rule per SHA: an owner-set ``[x]`` (previous box was checked and the
    previous marker was ``+`` or missing — i.e. not an auto-check) is
    preserved. Otherwise the box follows the current ``git cherry`` marker.
    """
    merged_lines = []
    seen_shorts = set()
    new_shorts = []

    # First pass: process commits in the order git cherry returned them.
    current_by_short = {}
    for marker, sha, subject in cherry_list:
        short = sha[:7]
        current_by_short[short] = (marker, sha, subject)

    # Preserve existing order: iterate over existing items first, keeping
    # any still reported by cherry; drop any no longer reported (they have
    # since landed on master with the same SHA).
    for short, meta in existing_items.items():
        if short not in current_by_short:
            continue
        marker, sha, subject = current_by_short[short]
        owner_checked = meta["checked"] and meta["cherry"] != "-"
        effective_marker = marker
        if owner_checked:
            # Keep the owner's [x]; still record the current cherry marker
            # so the context (auto-eligible or not) stays accurate.
            line = format_item(marker, sha, subject)
            # Force box to [x] regardless of current marker.
            line = re.sub(r"- \[ \]", "- [x]", line, count=1)
        else:
            line = format_item(effective_marker, sha, subject)
        merged_lines.append(line)
        seen_shorts.add(short)

    # Append commits that weren't in the existing body.
    for marker, sha, subject in cherry_list:
        short = sha[:7]
        if short in seen_shorts:
            continue
        merged_lines.append(format_item(marker, sha, subject))
        new_shorts.append(short)

    return merged_lines, new_shorts


def build_updated_body(base_full, base_short, existing_releases, merged_items,
                       tag):
    today = datetime.date.today().isoformat()
    existing_tags = {t for t, _ in existing_releases}
    releases = list(existing_releases)
    if tag not in existing_tags:
        releases.append((tag, today))

    descriptive = (
        f"Tracks commits that exist on release tags derived from master "
        f"`{base_short}` but not yet reflected in master. Items pre-checked "
        f"by `git cherry` (patch-id already on master) start as `[x]`; the "
        f"remaining `[ ]` items need owner review — either merge into master "
        f"or mark as \"not needed\".")

    release_lines = []
    for t, d in releases:
        date = d or today
        release_lines.append(f"- {t} (added {date})")

    lines = [
        f"<!-- tracker-base: {base_full} -->",
        "",
        descriptive,
        "",
        RELEASES_HEADER,
    ]
    lines.extend(release_lines)
    lines.append("")
    lines.append(COMMITS_HEADER)
    lines.extend(merged_items)
    lines.append("")
    return "\n".join(lines)


def find_tracker(base_full, dry_run, simulated=None):
    """Return (number, state, title, body) for a matching tracker, or None.

    A marker matches when its stored SHA is a prefix of ``base_full``
    (which in practice means equal for new trackers written as 40-char
    hex, or a valid prefix for older trackers that stored an abbreviated
    SHA). This keeps lookups working across git's auto-growing short SHA
    length.

    ``simulated`` is a ``(number, state, title, body)`` tuple injected by
    the ``--dry-run-issue`` flag to exercise the update path without
    hitting ``gh``. If provided, its base marker must be a prefix of
    ``base_full``.
    """
    if simulated is not None:
        _, _, _, body = simulated
        m = BASE_MARKER_RE.search(body)
        if not m:
            sys.exit(
                "--dry-run-issue body has no <!-- tracker-base: … --> marker; "
                "cannot simulate update path")
        if not base_full.startswith(m.group(1)):
            sys.exit(
                f"--dry-run-issue base marker ({m.group(1)}) does not match "
                f"the computed base for this tag ({base_full}); the real "
                "workflow would create a new issue instead of updating")
        return simulated
    if dry_run:
        return None
    raw = run([
        "gh",
        "issue",
        "list",
        "--label",
        LABEL,
        "--state",
        "all",
        "--json",
        "number,state,body,title",
        "--limit",
        "200",
    ])
    if not raw:
        return None
    issues = json.loads(raw)
    for issue in issues:
        body = issue.get("body") or ""
        m = BASE_MARKER_RE.search(body)
        if not m:
            continue
        if base_full.startswith(m.group(1)):
            return (
                issue["number"],
                issue["state"],
                issue["title"],
                body,
            )
    return None


def gh_create_issue(title, body, dry_run):
    if dry_run:
        print(f"[dry-run] would create issue: {title}\n{body}")
        return
    # Use stdin to avoid fighting with argv length limits.
    subprocess.run(
        [
            "gh", "issue", "create", "--label", LABEL, "--title", title,
            "--body-file", "-"
        ],
        input=body,
        text=True,
        check=True,
    )


def gh_update_issue(number, title, body, dry_run):
    if dry_run:
        print(f"[dry-run] would edit issue #{number} title={title!r}\n{body}")
        return
    subprocess.run(
        [
            "gh",
            "issue",
            "edit",
            str(number),
            "--title",
            title,
            "--body-file",
            "-",
        ],
        input=body,
        text=True,
        check=True,
    )


def gh_reopen_issue(number, dry_run):
    if dry_run:
        print(f"[dry-run] would reopen issue #{number}")
        return
    run(["gh", "issue", "reopen", str(number)])


def gh_comment(number, body, dry_run):
    if dry_run:
        print(f"[dry-run] would comment on #{number}:\n{body}")
        return
    subprocess.run(
        ["gh", "issue", "comment",
         str(number), "--body-file", "-"],
        input=body,
        text=True,
        check=True,
    )


def format_comment(tag, base_short, new_shorts, reopened):
    parts = [
        f"Release `{tag}` uses the same master base (`{base_short}`) as the "
        f"previous release tracked in this issue."
    ]
    if reopened:
        parts.append(
            "The issue was reopened automatically for the new release.")
    if new_shorts:
        listing = "\n".join(f"- `{s}`" for s in new_shorts)
        parts.append(f"Newly added commits:\n{listing}")
    else:
        parts.append("No new commits since the previous release.")
    return "\n\n".join(parts)


def ensure_label(dry_run):
    """Ensure the ``release-tracker`` label exists in the current repo.

    Uses ``gh label create --force`` so an already-present label is left
    unchanged. Skipped under ``--dry-run`` to keep offline exercises
    from reaching out to GitHub.
    """
    if dry_run:
        return
    subprocess.run(
        [
            "gh", "label", "create", LABEL, "--color", "0E8A16",
            "--description",
            "Tracks release commits not yet merged into master", "--force"
        ],
        check=True,
    )


def ensure_master_ref(master_ref):
    """Make sure ``master_ref`` is available locally.

    The release workflow checks out with ``fetch-depth: 0`` which brings
    the full history, so ``origin/master`` is normally present. If it's
    not (local invocation against a shallow clone), fetch it from the
    matching remote. For non-default refs (e.g. ``upstream/master``) the
    caller is expected to have the remote configured already.
    """
    try:
        run(["git", "rev-parse", "--verify", master_ref])
    except subprocess.CalledProcessError:
        if "/" not in master_ref:
            raise
        remote, branch = master_ref.split("/", 1)
        run([
            "git", "fetch", "--quiet", remote,
            f"{branch}:refs/remotes/{master_ref}"
        ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tag",
        default=os.environ.get("GITHUB_REF_NAME"),
        help="Release tag (default: $GITHUB_REF_NAME)",
    )
    parser.add_argument(
        "--master-ref",
        default="origin/master",
        help="Remote-qualified master ref to compare against "
        "(default: origin/master; use upstream/master when running "
        "against a fork where upstream is the canonical remote).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resulting body/comment instead of calling gh.",
    )
    parser.add_argument(
        "--dry-run-issue",
        metavar="FILE",
        help="Path to a file containing a simulated existing tracker issue "
        "body. Implies --dry-run. Use this to test the update path locally "
        "without calling gh: the file is treated as the current body of a "
        "matching tracker, and the script prints the would-be edited body "
        "and comment. The body must contain a <!-- tracker-base: SHA --> "
        "marker matching the base computed for the tag.",
    )
    parser.add_argument(
        "--dry-run-issue-state",
        choices=("open", "closed"),
        default="open",
        help="State of the simulated tracker issue. Use `closed` to "
        "exercise the reopen path (default: open).",
    )
    args = parser.parse_args()

    if args.dry_run_issue:
        args.dry_run = True

    if not args.tag:
        sys.exit("tag not provided (set --tag or $GITHUB_REF_NAME)")

    ensure_label(args.dry_run)
    ensure_master_ref(args.master_ref)
    base_full = git_merge_base(args.master_ref, args.tag)
    base_short = short_sha(base_full)

    commits = cherry_commits(args.master_ref, args.tag)
    if not commits:
        print(
            f"no release-only commits: {args.master_ref}..{args.tag} is "
            "empty — nothing to track",
            file=sys.stderr,
        )
        return

    simulated = None
    if args.dry_run_issue:
        with open(args.dry_run_issue) as f:
            simulated_body = f.read()
        simulated = (
            0,
            args.dry_run_issue_state.upper(),
            "Release tracker: <simulated>",
            simulated_body,
        )
    existing = find_tracker(base_full, args.dry_run, simulated)
    title = f"Release tracker: {args.tag}"

    if existing is None:
        items = [format_item(m, s, subj) for m, s, subj in commits]
        body = build_new_body(base_full, base_short, args.tag, items)
        gh_create_issue(title, body, args.dry_run)
        return

    number, state, _existing_title, existing_body = existing
    reopened = state.upper() == "CLOSED"
    if reopened:
        gh_reopen_issue(number, args.dry_run)

    existing_releases, existing_items = parse_existing(existing_body)
    merged_items, new_shorts = merge_items(existing_items, commits)
    new_body = build_updated_body(base_full, base_short, existing_releases,
                                  merged_items, args.tag)
    gh_update_issue(number, title, new_body, args.dry_run)
    gh_comment(number, format_comment(args.tag, base_short, new_shorts,
                                      reopened), args.dry_run)


if __name__ == "__main__":
    main()
