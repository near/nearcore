"""Reviews a PR with a NEAR AI hosted model and posts one GitHub review.

Usage (from .github/scripts):
    python3 -m nearai_review --repo near/nearcore --pr 123 [--dry-run]

Environment:
    NEARAI_API_KEY      NEAR AI Cloud API key (required)
    GH_TOKEN            GitHub token; needs pull-requests: write to post
    NEARAI_MODEL        model id (default: agent.DEFAULT_MODEL)
    NEARAI_BASE_URL     API base URL (default: agent.DEFAULT_BASE_URL)
    RUN_URL             link to the CI run, shown in the review
    NEARAI_REVIEW_*     limit overrides, see agent.Limits
"""

import argparse
import dataclasses
import json
import os
import pathlib
import shutil
import sys
import tempfile
import time

from . import agent, diff, github, render, tools

SKILL_PATH = ".claude/skills/pr-review/SKILL.md"
DOCUMENTS = ("AGENTS.md", "docs/practices/style.md", "CONTRIBUTING.md")
DEFAULT_ROOT = pathlib.Path(__file__).resolve().parents[3]

# Bot logins as the GraphQL API in fetch-pr-comments.sh reports them.
BOT_AUTHORS = {
    "github-actions",
    "greptile-apps",
    "copilot-pull-request-reviewer",
    "Copilot",
    "codecov",
}

# Failures caused by the model or its API. They are reported on the PR but
# do not fail the job, because the reviewer is advisory.
SOFT_FAILURES = ("model_unavailable", "budget", "bad_output")


class Failure(Exception):

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


@dataclasses.dataclass
class Config:
    repo: str
    pr: int
    dry_run: bool
    out_dir: pathlib.Path
    docs_root: pathlib.Path
    discussions_path: pathlib.Path | None
    exclude_bot_comments: bool
    git_url: str
    model: str
    base_url: str
    run_url: str | None
    api_key: str
    gh_token: str | None
    limits: agent.Limits


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def parse_config(argv: list[str], env: dict[str, str]) -> Config:
    parser = argparse.ArgumentParser(prog="python3 -m nearai_review")
    parser.add_argument("--repo", required=True, help="owner/name")
    parser.add_argument("--pr", required=True, type=int)
    parser.add_argument("--dry-run",
                        action="store_true",
                        help="write the review to --out instead of posting")
    parser.add_argument("--out", type=pathlib.Path, help="artifact directory")
    parser.add_argument("--docs-root",
                        type=pathlib.Path,
                        default=DEFAULT_ROOT,
                        help="trusted checkout holding SKILL.md and docs")
    parser.add_argument("--discussions",
                        type=pathlib.Path,
                        help="output file of fetch-pr-comments.sh")
    parser.add_argument("--exclude-bot-comments",
                        action="store_true",
                        help="hide bot reviews from the model (comparisons)")
    args = parser.parse_args(argv)
    if not env.get("NEARAI_API_KEY"):
        parser.error("NEARAI_API_KEY is not set")
    token = env.get("GH_TOKEN") or env.get("GITHUB_TOKEN")
    if not token and not args.dry_run:
        parser.error("GH_TOKEN is required unless --dry-run is given")
    out = args.out or pathlib.Path(env.get("RUNNER_TEMP") or
                                   ".") / "nearai-review"
    return Config(
        repo=args.repo,
        pr=args.pr,
        dry_run=args.dry_run,
        out_dir=out,
        docs_root=args.docs_root,
        discussions_path=args.discussions,
        exclude_bot_comments=args.exclude_bot_comments,
        git_url=f"https://github.com/{args.repo}.git",
        model=env.get("NEARAI_MODEL") or agent.DEFAULT_MODEL,
        base_url=env.get("NEARAI_BASE_URL") or agent.DEFAULT_BASE_URL,
        run_url=env.get("RUN_URL") or None,
        api_key=env["NEARAI_API_KEY"],
        gh_token=token,
        limits=agent.Limits.from_env(env),
    )


def capture(gh: github.GitHub, number: int) -> tuple[dict, list[dict]]:
    """Reads the PR and its files for one consistent head commit."""
    pr = gh.get_pr(number)
    for _ in range(3):
        files = gh.list_files(number)
        again = gh.get_pr(number)
        if again["head"]["sha"] == pr["head"]["sha"]:
            return again, files
        pr = again
    raise Failure("internal", "the PR head kept changing while it was read")


def _read(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def format_stats(stats: dict, seconds: float) -> str:
    # Token counts are left out on purpose: most prompt tokens are cache hits,
    # so a raw total would overstate the cost to PR readers. They are in the
    # job summary and stats.json.
    return (f"{stats.get('turns', 0)} model turns, "
            f"{stats.get('tool_calls', 0)} tool calls, {int(seconds)} s")


def format_tokens(stats: dict) -> str:
    prompt = stats.get("prompt_tokens", 0)
    cached = stats.get("cached_prompt_tokens", 0)
    return (f"prompt tokens {prompt} ({cached} cached), completion tokens "
            f"{stats.get('completion_tokens', 0)} (reasoning "
            f"{stats.get('reasoning_tokens', 0)})")


def write_artifacts(out_dir: pathlib.Path, artifacts: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, value in artifacts.items():
        text = value if isinstance(value, str) else json.dumps(value, indent=1)
        (out_dir / name).write_text(text, encoding="utf-8")


def write_job_summary(env: dict[str, str], lines: list[str]) -> None:
    path = env.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")


def post_review(gh: github.GitHub, number: int, payload: dict,
                submission: render.Submission | None,
                info: render.RunInfo) -> None:
    try:
        gh.create_review(number, payload)
        return
    except github.GitHubError as e:
        if e.status != 422 or not payload["comments"] or submission is None:
            raise
        log("::warning::GitHub rejected the inline comments; posting all "
            "findings in the review body")
    gh.create_review(number, render.build_review(submission, info,
                                                 inline=False))


def run_model(config: Config, gh: github.GitHub, client, fetch_repo, pr: dict,
              raw_files: list[dict], work: str) -> agent.Result:
    head_sha = pr["head"]["sha"]
    files = {f["filename"]: diff.FileDiff.from_api(f) for f in raw_files}
    base_sha = gh.merge_base(pr["base"]["ref"], head_sha)
    rubric = agent.extract_rubric(_read(config.docs_root / SKILL_PATH))
    documents = {
        name: _read(config.docs_root / name)
        for name in DOCUMENTS
        if (config.docs_root / name).is_file()
    }
    discussions = ""
    if config.discussions_path and config.discussions_path.is_file():
        discussions = _read(config.discussions_path)
    if config.exclude_bot_comments:
        discussions = agent.filter_discussions(discussions, BOT_AUTHORS)
    home = os.path.join(work, "home")
    os.mkdir(home)
    repo = fetch_repo(os.path.join(work, "pr.git"), home, config.git_url,
                      [head_sha, base_sha])
    repo_tools = tools.RepoTools(repo, head_sha, base_sha, files)
    system_prompt = agent.build_system_prompt(rubric, documents)
    user_prompt = agent.build_user_prompt(pr, files,
                                          repo_tools.list_changed_files(),
                                          discussions, config.limits,
                                          agent.Wrapper())
    reviewer = agent.Reviewer(client, repo_tools, files, config.limits, log=log)
    return reviewer.run(system_prompt, user_prompt)


def review(config: Config,
           gh: github.GitHub,
           client,
           fetch_repo=tools.Repo.fetch,
           env: dict[str, str] | None = None,
           clock=time.monotonic) -> int:
    """Runs one review. Returns the process exit code."""
    env = env if env is not None else {}
    start = clock()
    info = render.RunInfo(model=config.model,
                          head_sha="",
                          run_url=config.run_url)
    result = None
    failure = None
    work = tempfile.mkdtemp(prefix="nearai-review-")
    try:
        try:
            pr, raw_files = capture(gh, config.pr)
        except (github.GitHubError, KeyError, TypeError, Failure) as e:
            log(f"::error::could not read the PR: {e}")
            return 1
        info.head_sha = pr["head"]["sha"]
        if pr.get("state") != "open":
            log(f"PR #{config.pr} is {pr.get('state')}; nothing to review")
            return 0
        try:
            result = run_model(config, gh, client, fetch_repo, pr, raw_files,
                               work)
            failure = result.failure
        except Failure as e:
            log(f"::error::{e}")
            failure = e.reason
        except github.GitHubError as e:
            log(f"::error::GitHub request failed: {e}")
            failure = "config" if e.status in (401, 403) else "internal"
        except (tools.GitError, OSError, ValueError, KeyError) as e:
            log(f"::error::review setup failed: {type(e).__name__}: {e}")
            failure = "internal"
    finally:
        shutil.rmtree(work, ignore_errors=True)

    submission = result.submission if result else None
    stats = dict(result.stats) if result else {}
    stats["seconds"] = round(clock() - start, 1)
    stats["model_retries"] = getattr(client, "retries", 0)
    info.stats = format_stats(stats, stats["seconds"])

    if not config.dry_run:
        try:
            current = gh.get_pr(config.pr)
            if current.get("state") != "open":
                log(f"PR #{config.pr} was closed during the review; not "
                    "posting")
                return 0
            info.head_moved = current["head"]["sha"] != info.head_sha
        except (github.GitHubError, KeyError, TypeError) as e:
            log(f"::warning::could not re-read the PR before posting: {e}")

    if submission is not None:
        payload = render.build_review(submission, info)
    else:
        failure = failure or "internal"
        payload = render.render_failure(failure, info)
    if render.contains_secret(payload, [config.api_key, config.gh_token]):
        log("::error::the review contained a secret value; posting a "
            "failure note instead")
        submission = None
        failure = "internal"
        payload = render.render_failure(failure, info)

    stats["failure"] = failure if submission is None else None
    stats["inline_comments"] = len(payload["comments"])
    if submission is not None:
        stats["findings"] = len(submission.findings)
        stats["findings_off_diff"] = sum(
            1 for f in submission.findings
            if f.location is None and f.line is not None)
    write_artifacts(
        config.out_dir, {
            "transcript.json": result.messages if result else [],
            "payload.json": payload,
            "body.md": payload["body"],
            "stats.json": stats,
        })
    write_job_summary(env, [
        f"### NEAR AI review of #{config.pr}",
        "",
        f"- Model: `{config.model}`",
        f"- Result: {failure or 'review submitted'}",
        f"- {info.stats}",
        f"- {format_tokens(stats)}",
        f"- Inline comments: {stats['inline_comments']}",
    ])

    if config.dry_run:
        print(payload["body"])
        log(f"dry run: review written to {config.out_dir}")
    else:
        try:
            post_review(gh, config.pr, payload, submission, info)
        except github.GitHubError as e:
            log(f"::error::posting the review failed: {e}")
            return 1
    if submission is not None:
        return 0
    if failure in SOFT_FAILURES:
        log(f"::warning::NEAR AI review could not complete: {failure}")
        return 0
    return 1


def main(argv: list[str] | None = None) -> int:
    env = dict(os.environ)
    config = parse_config(sys.argv[1:] if argv is None else argv, env)
    # Secrets live only in the config from here on. Git subprocesses get a
    # fresh environment anyway (tools.git_env).
    for name in ("NEARAI_API_KEY", "GH_TOKEN", "GITHUB_TOKEN"):
        os.environ.pop(name, None)
    gh = github.GitHub(config.gh_token, config.repo, dry_run=config.dry_run)
    client = agent.Client(config.api_key,
                          base_url=config.base_url,
                          model=config.model)
    return review(config, gh, client, env=env)


if __name__ == "__main__":
    sys.exit(main())
