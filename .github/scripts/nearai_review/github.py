"""Minimal GitHub REST client for the reviewer, using only the stdlib."""

import json
import time
import urllib.error
import urllib.parse
import urllib.request

API_URL = "https://api.github.com"
MAX_FILES_PAGES = 30  # The files API returns at most 3000 files.


class GitHubError(Exception):

    def __init__(self, status: int | None, message: str):
        super().__init__(message)
        self.status = status


class GitHub:

    def __init__(self,
                 token: str | None,
                 repo: str,
                 dry_run: bool = False,
                 api_url: str = API_URL,
                 max_attempts: int = 3,
                 urlopen=urllib.request.urlopen,
                 sleep=time.sleep):
        self._token = token
        self.repo = repo
        self.dry_run = dry_run
        self.api_url = api_url.rstrip("/")
        self.max_attempts = max_attempts
        self.urlopen = urlopen
        self.sleep = sleep

    def _request(self, method: str, path: str, body=None):
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "nearai-pr-review",
        }
        if self._token:
            headers["Authorization"] = "Bearer " + self._token
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        error = None
        for attempt in range(1, self.max_attempts + 1):
            request = urllib.request.Request(self.api_url + path,
                                             data=data,
                                             headers=headers,
                                             method=method)
            delay = 2.0**attempt
            try:
                with self.urlopen(request, timeout=30) as response:
                    raw = response.read()
                return json.loads(raw) if raw else None
            except urllib.error.HTTPError as e:
                detail = e.read()[:500].decode(errors="replace")
                error = GitHubError(e.code,
                                    f"{method} {path}: HTTP {e.code} {detail}")
                retry_after = e.headers.get("Retry-After")
                rate_limited = e.code in (403, 429) and (
                    retry_after is not None or
                    e.headers.get("X-RateLimit-Remaining") == "0")
                if e.code < 500 and not rate_limited:
                    raise error from None
                if retry_after is not None:
                    try:
                        delay = min(float(retry_after), 60.0)
                    except ValueError:
                        pass
            except (urllib.error.URLError, OSError, ValueError) as e:
                error = GitHubError(None,
                                    f"{method} {path}: {type(e).__name__}")
            if attempt < self.max_attempts:
                self.sleep(delay)
        raise error

    def _repo_path(self, suffix: str) -> str:
        return f"/repos/{self.repo}{suffix}"

    def get_pr(self, number: int) -> dict:
        return self._request("GET", self._repo_path(f"/pulls/{number}"))

    def list_files(self, number: int) -> list[dict]:
        files = []
        for page in range(1, MAX_FILES_PAGES + 1):
            batch = self._request(
                "GET",
                self._repo_path(
                    f"/pulls/{number}/files?per_page=100&page={page}"))
            files.extend(batch or [])
            if not batch or len(batch) < 100:
                break
        return files

    def merge_base(self, base_ref: str, head_sha: str) -> str:
        ref = urllib.parse.quote(base_ref, safe="/")
        compare = self._request("GET",
                                self._repo_path(f"/compare/{ref}...{head_sha}"))
        return compare["merge_base_commit"]["sha"]

    def check_runs(self, sha: str, check_name: str) -> list[dict]:
        """Check runs with the given name on a commit."""
        name = urllib.parse.quote(check_name, safe="")
        result = self._request(
            "GET",
            self._repo_path(f"/commits/{sha}/check-runs?check_name={name}"
                            "&filter=latest&per_page=100"))
        return result.get("check_runs", []) if result else []

    def create_review(self, number: int, payload: dict) -> dict:
        if self.dry_run:
            raise RuntimeError("dry run must not post a review")
        return self._request("POST",
                             self._repo_path(f"/pulls/{number}/reviews"),
                             payload)
