#!/usr/bin/env python3
"""Merge queue health monitor for nearcore.

Analyzes GitHub Actions workflow runs triggered by the merge queue and
Nayduck CI test results to surface flaky tests, failure patterns, and
overall merge queue reliability.

Dependencies: gh CLI (authenticated), requests (pip install requests).
"""

import argparse
import json
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

try:
    import requests
except ImportError:
    sys.exit("requests is required: pip install requests")

REPO = "near/nearcore"
NAYDUCK_API = "https://nayduck.nearone.org/api"

WORKFLOWS = {
    "CI": "ci.yml",
    "CI Nayduck tests": "nayduck_ci.yml",
    "Benchmarks": "benchmarks.yml",
}

# Job name substrings used to classify CI failures.
CARGO_AUDIT_JOB = "Cargo Audit"
COVERAGE_JOB = "Generate Coverage Artifact"
NEXTEST_JOB = "Cargo Nextest"
CLIPPY_JOB = "Clippy"

# ---------------------------------------------------------------------------
# Progress helpers
# ---------------------------------------------------------------------------


def progress(message, end="\n"):
    """Print a progress message to stderr."""
    print(message, file=sys.stderr, end=end, flush=True)


def progress_item(index, total, label):
    """Print a progress counter like [3/10] label..."""
    print(f"\r  [{index}/{total}] {label}...",
          file=sys.stderr,
          end="",
          flush=True)
    if index == total:
        print(file=sys.stderr)


# ---------------------------------------------------------------------------
# GitHub helpers
# ---------------------------------------------------------------------------


def gh_api(endpoint, paginate=False):
    """Call `gh api` and return parsed JSON."""
    cmd = ["gh", "api", "-H", "Accept: application/vnd.github+json"]
    if paginate:
        cmd.append("--paginate")
    cmd.append(endpoint)
    try:
        result = subprocess.run(cmd,
                                capture_output=True,
                                text=True,
                                timeout=120)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"gh api timed out for: {endpoint}")
    if result.returncode != 0:
        raise RuntimeError(
            f"gh api failed for {endpoint}: {result.stderr.strip()}")
    text = result.stdout.strip()
    if not text:
        return []
    # --paginate concatenates JSON arrays; merge them.
    if paginate:
        arrays = []
        decoder = json.JSONDecoder()
        pos = 0
        while pos < len(text):
            while pos < len(text) and text[pos] in " \t\n\r":
                pos += 1
            if pos >= len(text):
                break
            obj, next_pos = decoder.raw_decode(text, pos)
            if isinstance(obj, list):
                arrays.extend(obj)
            else:
                arrays.append(obj)
            pos = next_pos
        return arrays
    return json.loads(text)


def fetch_workflow_runs(workflow_file, days, per_page=100):
    """Fetch all merge_group-triggered runs for a workflow within the time window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    runs = []
    page = 1
    while True:
        progress(f"  page {page}...", end="")
        endpoint = (f"/repos/{REPO}/actions/workflows/{workflow_file}/runs"
                    f"?event=merge_group&per_page={per_page}&page={page}")
        data = gh_api(endpoint)
        batch = data.get("workflow_runs", [])
        if not batch:
            progress(f" {len(runs)} runs total")
            break
        for run in batch:
            created = datetime.fromisoformat(run["created_at"].replace(
                "Z", "+00:00"))
            if created < cutoff:
                progress(f" {len(runs)} runs total")
                return runs
            runs.append(run)
        if len(batch) < per_page:
            progress(f" {len(runs)} runs total")
            break
        page += 1
        time.sleep(0.25)
    return runs


def fetch_jobs(run_id):
    """Return all jobs for a workflow run."""
    endpoint = f"/repos/{REPO}/actions/runs/{run_id}/jobs?per_page=100"
    data = gh_api(endpoint)
    return data.get("jobs", [])


def extract_pr_number(run):
    """Extract PR number from the merge queue branch name."""
    branch = run.get("head_branch", "")
    match = re.search(r"/pr-(\d+)-", branch)
    return int(match.group(1)) if match else None


# ---------------------------------------------------------------------------
# Nayduck helpers
# ---------------------------------------------------------------------------

_nayduck_sha_to_id = {}
_nayduck_min_id = None
_nayduck_index_initialized = False


def _init_nayduck_index():
    """Bulk-fetch the Nayduck runs listing (max 100) to seed the SHA index."""
    global _nayduck_min_id, _nayduck_index_initialized
    if _nayduck_index_initialized:
        return
    _nayduck_index_initialized = True

    progress("Fetching Nayduck runs index...")
    try:
        resp = requests.get(f"{NAYDUCK_API}/runs?limit=100", timeout=30)
        if resp.status_code != 200:
            progress(f"  warning: Nayduck API returned {resp.status_code}")
            return
        runs = resp.json()
    except Exception as exception:
        progress(f"  warning: Nayduck API error: {exception}")
        return

    for run in runs:
        sha = run.get("sha", "")
        if sha:
            _nayduck_sha_to_id.setdefault(sha, run["run_id"])
    if runs:
        _nayduck_min_id = min(run["run_id"] for run in runs)
    progress(
        f"  indexed {len(_nayduck_sha_to_id)} runs (oldest id={_nayduck_min_id})"
    )


def _extend_nayduck_index(target_sha):
    """Extend the index backwards by fetching older Nayduck runs by ID.

    The /api/runs listing is capped at 100. For older runs we fetch
    /api/run/{id} individually, walking backwards from the oldest known
    ID. Each response includes the sha so we can match. Progress is
    tracked so repeated calls don't re-scan already-visited IDs.
    """
    global _nayduck_min_id
    if _nayduck_min_id is None:
        return None

    start_id = _nayduck_min_id
    run_id = _nayduck_min_id - 1
    scanned = 0
    # Walk backwards until we find the target or exhaust 200 new IDs.
    while scanned < 200 and run_id > 0:
        if target_sha in _nayduck_sha_to_id:
            break
        if scanned % 20 == 0:
            progress(f"\r  extending Nayduck index: id={run_id}...", end="")
        try:
            resp = requests.get(f"{NAYDUCK_API}/run/{run_id}", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                sha = data.get("sha", "")
                if sha:
                    _nayduck_sha_to_id.setdefault(sha, run_id)
        except requests.ConnectionError:
            progress(
                f"\n  warning: Nayduck API connection error, stopping extension"
            )
            break
        except Exception:
            pass
        run_id -= 1
        scanned += 1
    _nayduck_min_id = run_id + 1
    progress(
        f"\r  extended Nayduck index: {start_id} -> {_nayduck_min_id} ({scanned} fetched, {len(_nayduck_sha_to_id)} total)"
    )
    return _nayduck_sha_to_id.get(target_sha)


def resolve_nayduck_run_id(gh_run):
    """Resolve a GitHub Actions run to a Nayduck run ID via SHA matching.

    First checks the bulk index (from /api/runs listing). On cache miss,
    extends the index backwards by fetching older Nayduck runs by ID.
    """
    _init_nayduck_index()
    sha = gh_run.get("head_sha", "")
    if not sha:
        return None
    nayduck_id = _nayduck_sha_to_id.get(sha)
    if nayduck_id:
        return nayduck_id
    return _extend_nayduck_index(sha)


def fetch_nayduck_results(nayduck_run_id):
    """Fetch test results from Nayduck API. Returns the full run JSON or None."""
    url = f"{NAYDUCK_API}/run/{nayduck_run_id}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        progress(
            f"  warning: Nayduck API returned {resp.status_code} for run {nayduck_run_id}"
        )
    except Exception as exception:
        progress(
            f"  warning: Nayduck API error for run {nayduck_run_id}: {exception}"
        )
    return None


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def classify_ci_failure(jobs):
    """Classify a CI workflow failure. Returns a category string."""
    failed_jobs = [job for job in jobs if job.get("conclusion") == "failure"]
    if not failed_jobs:
        return "unknown"
    names = [job["name"] for job in failed_jobs]
    # Cargo audit is not a required check but still shows as failure.
    if all(CARGO_AUDIT_JOB in name for name in names):
        return "cargo_audit"
    if all(COVERAGE_JOB in name for name in names):
        return "coverage_cascade"
    if len(failed_jobs) >= 4:
        # When nearly all jobs fail simultaneously it's likely infra.
        return "infra"
    for name in names:
        if NEXTEST_JOB in name:
            return "nextest"
        if CLIPPY_JOB in name:
            return "clippy"
    if any(COVERAGE_JOB in name for name in names):
        return "coverage_cascade"
    return "other"


def classify_nayduck_failure(jobs):
    """Classify a CI Nayduck tests workflow failure."""
    failed_jobs = [job for job in jobs if job.get("conclusion") == "failure"]
    names = [job["name"] for job in failed_jobs]
    categories = []
    for name in names:
        name_lower = name.lower()
        if "nayduck" in name_lower:
            categories.append("nayduck_flaky")
        elif "pytest" in name_lower:
            categories.append("large_pytest_flaky")
        else:
            categories.append("other")
    return categories or ["unknown"]


# ---------------------------------------------------------------------------
# Report building
# ---------------------------------------------------------------------------


def build_report(days, verbose=False):
    """Gather all data and build the report dictionary."""
    report = {
        "window_days": days,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "workflows": {},
        "pr_attempts": {},
        "job_failures": defaultdict(lambda: defaultdict(int)),
        "ci_failure_categories": defaultdict(int),
        "nayduck_failed_tests": defaultdict(int),
        "nayduck_retry_tests": defaultdict(int),
        "large_pytest_failures": defaultdict(int),
        "details": [],
    }

    for workflow_name, workflow_file in WORKFLOWS.items():
        progress(f"Fetching {workflow_name} runs ({workflow_file})...")
        runs = fetch_workflow_runs(workflow_file, days)
        counts = defaultdict(int)
        for run in runs:
            counts[run["conclusion"] or "in_progress"] += 1

        report["workflows"][workflow_name] = {
            "total": len(runs),
            "conclusions": dict(counts),
            "runs": runs,
        }

        # Track PR merge-queue attempts.
        for run in runs:
            pr_number = extract_pr_number(run)
            if pr_number is None:
                continue
            branch = run.get("head_branch", "")
            report["pr_attempts"].setdefault(pr_number, set())
            report["pr_attempts"][pr_number].add(branch)

    # --- Fetch jobs for all failed runs across all workflows ---
    for workflow_name in WORKFLOWS:
        workflow_data = report["workflows"].get(workflow_name, {})
        workflow_runs = workflow_data.get("runs", [])
        workflow_failures = [
            run for run in workflow_runs if run.get("conclusion") == "failure"
        ]
        if not workflow_failures:
            continue
        progress(
            f"Fetching jobs for {len(workflow_failures)} failed {workflow_name} runs..."
        )
        for index, run in enumerate(workflow_failures, 1):
            pr_number = extract_pr_number(run)
            progress_item(index, len(workflow_failures),
                          f"PR #{pr_number or '?'}")
            jobs = fetch_jobs(run["id"])

            # Per-job failure tracking.
            failed_names = []
            for job in jobs:
                if job.get("conclusion") == "failure":
                    report["job_failures"][workflow_name][job["name"]] += 1
                    failed_names.append(job["name"])

            # CI-specific classification.
            if workflow_name == "CI":
                category = classify_ci_failure(jobs)
                report["ci_failure_categories"][category] += 1
                if verbose:
                    report["details"].append({
                        "workflow": "CI",
                        "run_id": run["id"],
                        "pr": pr_number,
                        "conclusion": run["conclusion"],
                        "category": category,
                        "failed_jobs": failed_names,
                        "created_at": run["created_at"],
                    })

            # Nayduck-specific: get individual test failures.
            elif workflow_name == "CI Nayduck tests":
                categories = classify_nayduck_failure(jobs)
                for category in categories:
                    report["ci_failure_categories"][category] += 1
                for job in jobs:
                    name_lower = job["name"].lower()
                    if job.get("conclusion"
                              ) == "failure" and "nayduck" in name_lower:
                        nayduck_run_id = resolve_nayduck_run_id(run)
                        if nayduck_run_id:
                            nayduck_data = fetch_nayduck_results(nayduck_run_id)
                            if nayduck_data:
                                for test in nayduck_data.get("tests", []):
                                    if test.get("status") == "FAILED":
                                        report["nayduck_failed_tests"][
                                            test["name"]] += 1
                    elif job.get("conclusion"
                                ) == "failure" and "pytest" in name_lower:
                        for step in job.get("steps", []):
                            if step.get("conclusion") == "failure":
                                report["large_pytest_failures"][
                                    step["name"]] += 1
                if verbose:
                    report["details"].append({
                        "workflow": "CI Nayduck tests",
                        "run_id": run["id"],
                        "pr": pr_number,
                        "conclusion": run["conclusion"],
                        "categories": categories,
                        "failed_jobs": failed_names,
                        "created_at": run["created_at"],
                    })

            # Benchmarks or other workflows.
            elif verbose:
                report["details"].append({
                    "workflow": workflow_name,
                    "run_id": run["id"],
                    "pr": pr_number,
                    "conclusion": run["conclusion"],
                    "failed_jobs": failed_names,
                    "created_at": run["created_at"],
                })
            time.sleep(0.15)

    # --- Nayduck hidden flakiness (retry data from successful + failed runs) ---
    nayduck_runs = report["workflows"].get("CI Nayduck tests",
                                           {}).get("runs", [])
    completed_nayduck = [
        run for run in nayduck_runs
        if run.get("conclusion") in ("success", "failure")
    ]
    progress(
        f"Fetching retry data for {len(completed_nayduck)} completed Nayduck runs..."
    )
    nayduck_run_count = 0
    for index, run in enumerate(completed_nayduck, 1):
        pr_number = extract_pr_number(run)
        nayduck_run_id = resolve_nayduck_run_id(run)
        if not nayduck_run_id:
            progress_item(index, len(completed_nayduck),
                          f"PR #{pr_number or '?'} (no nayduck match)")
            continue
        progress_item(index, len(completed_nayduck),
                      f"PR #{pr_number or '?'} (nayduck #{nayduck_run_id})")
        nayduck_data = fetch_nayduck_results(nayduck_run_id)
        if not nayduck_data:
            continue
        nayduck_run_count += 1
        for test in nayduck_data.get("tests", []):
            tries = test.get("tries", 1)
            if tries > 1:
                report["nayduck_retry_tests"][test["name"]] += 1
        time.sleep(0.1)
    progress(
        f"  successfully fetched {nayduck_run_count}/{len(completed_nayduck)} Nayduck runs"
    )
    report["nayduck_sampled_runs"] = nayduck_run_count

    progress("Building report...")

    # Convert sets/defaultdict for JSON serialization.
    report["pr_attempts"] = {
        str(pr_number): len(branches)
        for pr_number, branches in report["pr_attempts"].items()
    }
    report["job_failures"] = {
        workflow: dict(jobs)
        for workflow, jobs in report["job_failures"].items()
    }
    report["ci_failure_categories"] = dict(report["ci_failure_categories"])
    report["nayduck_failed_tests"] = dict(report["nayduck_failed_tests"])
    report["nayduck_retry_tests"] = dict(report["nayduck_retry_tests"])
    report["large_pytest_failures"] = dict(report["large_pytest_failures"])

    # Remove raw run data before returning.
    for workflow in report["workflows"].values():
        del workflow["runs"]

    return report


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------


def print_text_report(report):
    days = report["window_days"]
    print(f"\n{'='*60}")
    print(f"  Merge Queue Health Report  ({days}-day window)")
    print(f"  Generated: {report['generated_at']}")
    print(f"{'='*60}\n")

    # Workflow summary
    print("## Workflow Runs\n")
    for name, data in report["workflows"].items():
        total = data["total"]
        conclusions = data["conclusions"]
        success = conclusions.get("success", 0)
        failure = conclusions.get("failure", 0)
        cancelled = conclusions.get("cancelled", 0)
        rate = f"{success/total*100:.0f}%" if total else "N/A"
        print(f"  {name}: {total} runs  ({rate} success)")
        print(
            f"    success={success}  failure={failure}  cancelled={cancelled}")
    print()

    # Per-job failure rates
    job_failures = report.get("job_failures", {})
    if job_failures:
        print("## Job Failure Rates\n")
        for workflow_name, jobs in job_failures.items():
            workflow_data = report["workflows"].get(workflow_name, {})
            conclusions = workflow_data["conclusions"]
            completed = conclusions.get("success", 0) + conclusions.get(
                "failure", 0)
            print(f"  {workflow_name} ({completed} completed runs):")
            for job, count in sorted(jobs.items(), key=lambda x: -x[1]):
                percent = f"{count/completed*100:.0f}%" if completed else "?"
                print(f"    {count:3d} ({percent:>4s})  {job}")
            print()

    # PR attempt stats
    attempts = report["pr_attempts"]
    if attempts:
        total_prs = len(attempts)
        first_try = sum(1 for value in attempts.values() if value == 1)
        requeued = sum(1 for value in attempts.values() if value > 1)
        average = sum(attempts.values()) / total_prs
        print("## Merge Queue Reliability\n")
        print(f"  PRs seen:                {total_prs}")
        print(
            f"  First-attempt merges:    {first_try} ({first_try/total_prs*100:.0f}%)"
        )
        print(
            f"  Re-queued PRs:           {requeued} ({requeued/total_prs*100:.0f}%)"
        )
        print(f"  Avg attempts per PR:     {average:.2f}")
        if requeued:
            worst = sorted(attempts.items(), key=lambda x: -x[1])[:5]
            print(
                f"  Worst PRs:               {', '.join(f'#{pr_number}({count}x)' for pr_number, count in worst)}"
            )
        print()

    # CI failure categories
    failure_categories = report["ci_failure_categories"]
    if failure_categories:
        print("## Failure Categories\n")
        for category, count in sorted(failure_categories.items(),
                                      key=lambda x: -x[1]):
            print(f"  {category:25s} {count}")
        print()

    # Nayduck failed tests (caused ejections)
    failed = report["nayduck_failed_tests"]
    if failed:
        print("## Nayduck Tests Causing Ejections\n")
        for name, count in sorted(failed.items(), key=lambda x: -x[1]):
            print(f"  {count}x  {name}")
        print()

    # Large pytest failures
    large_pytest_failures = report["large_pytest_failures"]
    if large_pytest_failures:
        print("## Large pytest Failures\n")
        for name, count in sorted(large_pytest_failures.items(),
                                  key=lambda x: -x[1]):
            print(f"  {count}x  {name}")
        print()

    # Nayduck hidden flakiness (retries)
    retries = report["nayduck_retry_tests"]
    sampled = report.get("nayduck_sampled_runs", 0)
    if retries:
        print(
            f"## Nayduck Hidden Flakiness (retries, sampled {sampled} runs)\n")
        for name, count in sorted(retries.items(), key=lambda x: -x[1])[:20]:
            rate = f"{count}/{sampled}" if sampled else "?"
            print(f"  {rate:8s}  {name}")
        print()

    # Verbose details
    details = report.get("details", [])
    if details:
        print("## Detailed Failures\n")
        for detail in details:
            pr_number = detail.get("pr", "?")
            print(
                f"  [{detail['created_at'][:10]}] PR #{pr_number}  run={detail['run_id']}"
            )
            print(
                f"    workflow={detail['workflow']}  category={detail.get('category') or detail.get('categories')}"
            )
            print(f"    failed_jobs={detail.get('failed_jobs', [])}")
        print()


def print_json_report(report):
    print(json.dumps(report, indent=2, default=str))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Analyze nearcore merge queue health and flaky tests.")
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Time window in days (default: 14)",
    )
    parser.add_argument(
        "--output",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show per-failure details",
    )
    args = parser.parse_args()

    report = build_report(args.days, verbose=args.verbose)

    if args.output == "json":
        print_json_report(report)
    else:
        print_text_report(report)


if __name__ == "__main__":
    main()
