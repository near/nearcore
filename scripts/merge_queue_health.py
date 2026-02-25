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

def progress(msg, end="\n"):
    """Print a progress message to stderr."""
    print(msg, file=sys.stderr, end=end, flush=True)


def progress_item(i, total, label):
    """Print a progress counter like [3/10] label..."""
    print(f"\r  [{i}/{total}] {label}...", file=sys.stderr, end="", flush=True)
    if i == total:
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
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"gh api failed: {result.stderr.strip()}")
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
            obj, end = decoder.raw_decode(text, pos)
            if isinstance(obj, list):
                arrays.extend(obj)
            else:
                arrays.append(obj)
            pos = end
        return arrays
    return json.loads(text)


def fetch_workflow_runs(workflow_file, days, per_page=100):
    """Fetch all merge_group-triggered runs for a workflow within the time window."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    runs = []
    page = 1
    while True:
        progress(f"  page {page}...", end="")
        endpoint = (
            f"/repos/{REPO}/actions/workflows/{workflow_file}/runs"
            f"?event=merge_group&per_page={per_page}&page={page}"
        )
        data = gh_api(endpoint)
        batch = data.get("workflow_runs", [])
        if not batch:
            progress(f" {len(runs)} runs total")
            break
        for r in batch:
            created = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
            if created < cutoff:
                progress(f" {len(runs)} runs total")
                return runs
            runs.append(r)
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


def fetch_failed_step(run_id, job_name):
    """Return the name of the first failed step in a job, or None."""
    jobs = fetch_jobs(run_id)
    for job in jobs:
        if job["name"] == job_name:
            for step in job.get("steps", []):
                if step.get("conclusion") == "failure":
                    return step["name"]
    return None


def extract_pr_number(run):
    """Extract PR number from the merge queue branch name."""
    branch = run.get("head_branch", "")
    m = re.search(r"/pr-(\d+)-", branch)
    return int(m.group(1)) if m else None


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
    except Exception as e:
        progress(f"  warning: Nayduck API error: {e}")
        return

    for r in runs:
        sha = r.get("sha", "")
        if sha:
            _nayduck_sha_to_id.setdefault(sha, r["run_id"])
    if runs:
        _nayduck_min_id = min(r["run_id"] for r in runs)
    progress(f"  indexed {len(_nayduck_sha_to_id)} runs (oldest id={_nayduck_min_id})")


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
        except Exception:
            pass
        run_id -= 1
        scanned += 1
    _nayduck_min_id = run_id + 1
    progress(f"\r  extended Nayduck index: {start_id} -> {_nayduck_min_id} ({scanned} fetched, {len(_nayduck_sha_to_id)} total)")
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
    nay_id = _nayduck_sha_to_id.get(sha)
    if nay_id:
        return nay_id
    return _extend_nayduck_index(sha)


def fetch_nayduck_results(nayduck_run_id):
    """Fetch test results from Nayduck API. Returns the full run JSON or None."""
    url = f"{NAYDUCK_API}/run/{nayduck_run_id}"
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_ci_failure(jobs):
    """Classify a CI workflow failure. Returns a category string."""
    failed_jobs = [j for j in jobs if j.get("conclusion") == "failure"]
    if not failed_jobs:
        return "unknown"
    names = [j["name"] for j in failed_jobs]
    # Cargo audit is not a required check but still shows as failure.
    if all(CARGO_AUDIT_JOB in n for n in names):
        return "cargo_audit"
    if all(COVERAGE_JOB in n for n in names):
        return "coverage_cascade"
    if len(failed_jobs) >= 4:
        # When nearly all jobs fail simultaneously it's likely infra.
        return "infra"
    for n in names:
        if NEXTEST_JOB in n:
            return "nextest"
        if CLIPPY_JOB in n:
            return "clippy"
    if any(COVERAGE_JOB in n for n in names):
        return "coverage_cascade"
    return "other"


def classify_nayduck_failure(jobs):
    """Classify a CI Nayduck tests workflow failure."""
    failed_jobs = [j for j in jobs if j.get("conclusion") == "failure"]
    names = [j["name"] for j in failed_jobs]
    categories = []
    for n in names:
        if "Nayduck" in n or "nayduck" in n:
            categories.append("nayduck_flaky")
        elif "Large pytest" in n or "pytest" in n.lower():
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

    for wf_name, wf_file in WORKFLOWS.items():
        progress(f"Fetching {wf_name} runs ({wf_file})...")
        runs = fetch_workflow_runs(wf_file, days)
        counts = defaultdict(int)
        for r in runs:
            counts[r["conclusion"] or "in_progress"] += 1

        report["workflows"][wf_name] = {
            "total": len(runs),
            "conclusions": dict(counts),
            "runs": runs,
        }

        # Track PR merge-queue attempts.
        for r in runs:
            pr = extract_pr_number(r)
            if pr is None:
                continue
            branch = r.get("head_branch", "")
            report["pr_attempts"].setdefault(pr, set())
            report["pr_attempts"][pr].add(branch)

    # --- Fetch jobs for all failed runs across all workflows ---
    for wf_name in WORKFLOWS:
        wf_data = report["workflows"].get(wf_name, {})
        wf_runs = wf_data.get("runs", [])
        wf_failures = [r for r in wf_runs if r.get("conclusion") == "failure"]
        if not wf_failures:
            continue
        progress(f"Fetching jobs for {len(wf_failures)} failed {wf_name} runs...")
        for i, r in enumerate(wf_failures, 1):
            pr = extract_pr_number(r)
            progress_item(i, len(wf_failures), f"PR #{pr or '?'}")
            jobs = fetch_jobs(r["id"])

            # Per-job failure tracking.
            failed_names = []
            for j in jobs:
                if j.get("conclusion") == "failure":
                    report["job_failures"][wf_name][j["name"]] += 1
                    failed_names.append(j["name"])

            # CI-specific classification.
            if wf_name == "CI":
                cat = classify_ci_failure(jobs)
                report["ci_failure_categories"][cat] += 1
                if verbose:
                    report["details"].append({
                        "workflow": "CI",
                        "run_id": r["id"],
                        "pr": pr,
                        "conclusion": r["conclusion"],
                        "category": cat,
                        "failed_jobs": failed_names,
                        "created_at": r["created_at"],
                    })

            # Nayduck-specific: get individual test failures.
            elif wf_name == "CI Nayduck tests":
                cats = classify_nayduck_failure(jobs)
                for cat in cats:
                    report["ci_failure_categories"][cat] += 1
                for j in jobs:
                    if j.get("conclusion") == "failure" and "Nayduck" in j["name"]:
                        nay_id = resolve_nayduck_run_id(r)
                        if nay_id:
                            nay_data = fetch_nayduck_results(nay_id)
                            if nay_data:
                                for t in nay_data.get("tests", []):
                                    if t.get("status") == "FAILED":
                                        report["nayduck_failed_tests"][t["name"]] += 1
                    elif j.get("conclusion") == "failure" and "pytest" in j["name"].lower():
                        for step in j.get("steps", []):
                            if step.get("conclusion") == "failure":
                                report["large_pytest_failures"][step["name"]] += 1
                if verbose:
                    report["details"].append({
                        "workflow": "CI Nayduck tests",
                        "run_id": r["id"],
                        "pr": pr,
                        "conclusion": r["conclusion"],
                        "categories": cats,
                        "failed_jobs": failed_names,
                        "created_at": r["created_at"],
                    })

            # Benchmarks or other workflows.
            elif verbose:
                report["details"].append({
                    "workflow": wf_name,
                    "run_id": r["id"],
                    "pr": pr,
                    "conclusion": r["conclusion"],
                    "failed_jobs": failed_names,
                    "created_at": r["created_at"],
                })
            time.sleep(0.15)

    # --- Nayduck hidden flakiness (retry data from successful + failed runs) ---
    nay_runs = report["workflows"].get("CI Nayduck tests", {}).get("runs", [])
    completed_nay = [r for r in nay_runs if r.get("conclusion") in ("success", "failure")]
    sample = completed_nay
    progress(f"Fetching retry data for {len(sample)} completed Nayduck runs...")
    nayduck_run_count = 0
    for i, r in enumerate(sample, 1):
        pr = extract_pr_number(r)
        nay_id = resolve_nayduck_run_id(r)
        if not nay_id:
            progress_item(i, len(sample), f"PR #{pr or '?'} (no nayduck match)")
            continue
        progress_item(i, len(sample), f"PR #{pr or '?'} (nayduck #{nay_id})")
        nay_data = fetch_nayduck_results(nay_id)
        if not nay_data:
            continue
        nayduck_run_count += 1
        for t in nay_data.get("tests", []):
            tries = t.get("tries", 1)
            if tries > 1:
                report["nayduck_retry_tests"][t["name"]] += 1
        time.sleep(0.1)
    progress(f"  successfully fetched {nayduck_run_count}/{len(sample)} Nayduck runs")
    report["nayduck_sampled_runs"] = nayduck_run_count

    progress("Building report...")

    # Convert sets/defaultdicts for JSON serialization.
    report["pr_attempts"] = {
        str(pr): len(branches) for pr, branches in report["pr_attempts"].items()
    }
    report["job_failures"] = {
        wf: dict(jobs) for wf, jobs in report["job_failures"].items()
    }
    report["ci_failure_categories"] = dict(report["ci_failure_categories"])
    report["nayduck_failed_tests"] = dict(report["nayduck_failed_tests"])
    report["nayduck_retry_tests"] = dict(report["nayduck_retry_tests"])
    report["large_pytest_failures"] = dict(report["large_pytest_failures"])

    # Remove raw run data before returning.
    for wf in report["workflows"].values():
        del wf["runs"]

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
        c = data["conclusions"]
        success = c.get("success", 0)
        failure = c.get("failure", 0)
        cancelled = c.get("cancelled", 0)
        rate = f"{success/total*100:.0f}%" if total else "N/A"
        print(f"  {name}: {total} runs  ({rate} success)")
        print(f"    success={success}  failure={failure}  cancelled={cancelled}")
    print()

    # Per-job failure rates
    job_failures = report.get("job_failures", {})
    if job_failures:
        print("## Job Failure Rates\n")
        for wf_name, jobs in job_failures.items():
            wf_data = report["workflows"].get(wf_name, {})
            completed = wf_data["total"] - wf_data["conclusions"].get("cancelled", 0)
            print(f"  {wf_name} ({completed} completed runs):")
            for job, count in sorted(jobs.items(), key=lambda x: -x[1]):
                pct = f"{count/completed*100:.0f}%" if completed else "?"
                print(f"    {count:3d} ({pct:>4s})  {job}")
            print()

    # PR attempt stats
    attempts = report["pr_attempts"]
    if attempts:
        total_prs = len(attempts)
        first_try = sum(1 for v in attempts.values() if v == 1)
        multi = sum(1 for v in attempts.values() if v > 1)
        avg = sum(attempts.values()) / total_prs
        print("## Merge Queue Reliability\n")
        print(f"  PRs seen:                {total_prs}")
        print(f"  First-attempt merges:    {first_try} ({first_try/total_prs*100:.0f}%)")
        print(f"  Re-queued PRs:           {multi} ({multi/total_prs*100:.0f}%)")
        print(f"  Avg attempts per PR:     {avg:.2f}")
        if multi:
            worst = sorted(attempts.items(), key=lambda x: -x[1])[:5]
            print(f"  Worst PRs:               {', '.join(f'#{pr}({n}x)' for pr, n in worst)}")
        print()

    # CI failure categories
    cats = report["ci_failure_categories"]
    if cats:
        print("## Failure Categories\n")
        for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
            print(f"  {cat:25s} {count}")
        print()

    # Nayduck failed tests (caused ejections)
    failed = report["nayduck_failed_tests"]
    if failed:
        print("## Nayduck Tests Causing Ejections\n")
        for name, count in sorted(failed.items(), key=lambda x: -x[1]):
            print(f"  {count}x  {name}")
        print()

    # Large pytest failures
    lp = report["large_pytest_failures"]
    if lp:
        print("## Large pytest Failures\n")
        for name, count in sorted(lp.items(), key=lambda x: -x[1]):
            print(f"  {count}x  {name}")
        print()

    # Nayduck hidden flakiness (retries)
    retries = report["nayduck_retry_tests"]
    sampled = report.get("nayduck_sampled_runs", 0)
    if retries:
        print(f"## Nayduck Hidden Flakiness (retries, sampled {sampled} runs)\n")
        for name, count in sorted(retries.items(), key=lambda x: -x[1])[:20]:
            rate = f"{count}/{sampled}" if sampled else "?"
            print(f"  {rate:8s}  {name}")
        print()

    # Verbose details
    details = report.get("details", [])
    if details:
        print("## Detailed Failures\n")
        for d in details:
            pr = d.get("pr", "?")
            print(f"  [{d['created_at'][:10]}] PR #{pr}  run={d['run_id']}")
            print(f"    workflow={d['workflow']}  category={d.get('category') or d.get('categories')}")
            print(f"    failed_jobs={d.get('failed_jobs', [])}")
        print()


def print_json_report(report):
    print(json.dumps(report, indent=2, default=str))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Analyze nearcore merge queue health and flaky tests."
    )
    parser.add_argument(
        "--days", type=int, default=14,
        help="Time window in days (default: 14)",
    )
    parser.add_argument(
        "--output", choices=["text", "json"], default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
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
